from sqlalchemy import select, func, cast
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import session
from sqlalchemy.orm.loading import instances

from datetime import datetime, timedelta
import logging

from database import sync_engine, sync_session_fabric
from model import *

# Настройка логгера
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    filename = 'app.log',  # Имя файла для хранения логов
                    filemode = 'a'  # Режим записи: 'a' для добавления, 'w' для перезаписи
)
logger = logging.getLogger(__name__)


def user_registration(user_id: str, user_name: str) -> str:
    '''
    Добавляет нового пользователя в базу.
    Если пользователь с таким user_id уже существует, выбрасывает исключение.

    :param user_id: Уникальный идентификатор пользователя (он должен быть уникальным в таблице)

    :return: Строка с подтверждением успешной регистрации пользователя.
    :raises ValueError: Если user_id или user_name пустые строки,
                        или если пользователь с таким user_id уже существует.
    '''

    if not user_id or not isinstance(user_id, str) or not user_name or not isinstance(user_name, str):
        logger.error('ERROR: User user_id and user_name can\'t be empty')
        raise ValueError('user_id и user_name не должны быть пустыми строками')

    user = UserORM(user_id=user_id, username=user_name)
    with sync_session_fabric() as session:
        session.add(user)
        try:
            session.commit()
            logger.info(f'User {user_id} - {user_name} successfully registered')
            return 'Пользователь успешно зарегистрирован'
        except IntegrityError:
            session.rollback()
            logger.error(f'ERROR: User {user_id} - {user_name} already exists')
            raise ValueError(f'Пользователь с ID {user_id} уже существует')


def get_user_mood_liist() -> list:
    '''
    Получаем список всех настроений пользователя и возвращаем его запросу
    :return: list of moods from MoodsEnum
    '''
    return [mood.value for mood in MoodsEnum]


def get_user_personal_moods(user_id: str) -> dict:
    '''
    Получаем все персональные настроения пользователя
    :param user_id:
    :return: dict of moods from MoodsEnum as a {'personal_mood': mood_weight}
    '''
    personal_moods = (
        sync_session_fabric.query(PersonalMoodORM.user_mood, PersonalMoodORM.mood_weight)
        .filter(PersonalMoodORM.user_id == user_id)
        .all
    )
    mood_dict = {user_mood: mood_weight for user_mood, mood_weight in personal_moods}
    return mood_dict


def insert_user_mood(user_id: str, mood: str, why: Optional[str] = None) -> str:
    '''
    Устанавливает настроение юзера на текущий момент
    :param user_id: id пользователя (не уникальный, но связан с таблицей user_id)
    :param mood: параметр настроения, должен быть взят из class MoodsEnum(Enum)
    :param why: параметр содержит пояснение к настроению, может быть пустым
    :return: None
    '''

    if not user_id or not isinstance(user_id, str) \
            or not mood or not isinstance(mood, str):
        logger.error('ERROR: User user_id and mood can\'t be empty')
        raise ValueError('user_id и mood не должны быть пустыми строками')

    with sync_session_fabric() as session:
        # Получаем weight из PersonalMoodORM (если есть)
        personal_mood_weight = (
            session.query(PersonalMoodORM)
            .filter(PersonalMoodORM.user_id == user_id, PersonalMoodORM.user_mood == mood)
            .first()
        )

        if personal_mood_weight:
            weight = personal_mood_weight.mood_weight
            logger.info(f'Using existing personal mood weight for user {user_id}: {weight}')
        else:
            # Получаем вес из WeightEnun
            try:
                weight = WeightEnun[mood].value
                logger.info(f'Using WeightEnun for mood {mood}: {weight}')
            except KeyError:
                logger.error(f'ERROR: incorrect mood value: {mood}')
                raise ValueError(f'Недопустимое значение настроения: {mood}')

        user_id_mood = MoodORM(
            user_id=user_id,
            mood=mood,
            why=why,
            weight=weight,
        )

        session.add(user_id_mood)
        try:
            session.commit()
            logger.info(f'User mood inserted successfully: user_id={user_id}, mood={mood}, weight={weight}')
        except IntegrityError as e:
            session.rollback()
            logger.error(f'ERROR: commit error {e}')
            raise Exception(f'Ошибка при записи в базу данных: {e}')  # Четкое сообщение об ошибке


def avg_user_mood_set_by_sheduler() -> str:
    '''
    1. Запускать функцию каждый день после полуночи
    2. Получить все записи weight у всех user_id за прошедший день
    3. Для каждого user_id посчитать среднеарифметический weight за прошедший день, в случае отсутствия записей вернуть None
    4. Делает запись в AverageMoodORM для каждого user_id с усреднённым за день weight и указанием даты прошедшего дня формата гггг.мм.дд
    5. По мере записи в AverageMoodORM выводить прогресс в консоль в виде соотношения: всего нужно сделать N записей / N записано к текущему моменту (можно оформить как доп. функцию)
    :return: f'User id {user_id} : start {entry_time} : end {end_time} : total time {end_time - entry_time} : average_mood_weight {average_weight}'
             f'total time for commit : {total_time_end - total_time_start} : added {total_records_counter} records'
             and error string, all in list
    '''

    total_time_start = datetime.now()

    # Определяем вчерашнюю дату
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    date_str = yesterday.date()

    with sync_session_fabric() as session:
        try:
            # Получаем список уникальных пользователей
            user_id_list = session.query(UserORM.user_id).filter(
                MoodORM.date >= yesterday.replace(hour=0, minute=0, second=0, microsecond=0),
                MoodORM.date < datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
            ).distinct().all()

            # Лог
            total_users = len(user_id_list)
            total_records_counter = 0

            logger.info(f'Found {total_users} unique users for mood processing.')

            for (user_id,) in user_id_list:
                try:
                    entry_time = datetime.now()
                    average_weight = session.query(func.avg(cast(MoodORM.weight, Integer))).filter(
                        MoodORM.user_id == user_id,
                        MoodORM.date >= yesterday.replace(hour=0, minute=0, second=0, microsecond=0),
                        MoodORM.date < datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
                    ).scalar()

                    # Создаём запись для добавления
                    avg_mood_entry = AverageMoodORM(
                        user_id=user_id,
                        avg_mood_weight=average_weight,
                        date=date_str,
                    )

                    session.add(avg_mood_entry)

                    # Лог
                    total_records_counter += 1
                    end_time = datetime.now()
                    logger.info(
                        f'User id {user_id} : start {entry_time} : end {end_time} : '
                        f'total time {end_time - entry_time} : average_mood_weight {average_weight}'
                    )

                except Exception as e:
                    logger.error(f'ERROR for user_id {user_id} : {str(e)}')
                    return (f'ERROR during the batch operation : {str(e)}')

            session.commit()

        except Exception as e:
            logger.error(f'ERROR during the batch operation : {str(e)}')
            return (f'ERROR during the batch operation : {str(e)}')

        finally:
            total_time_end = datetime.now()
            logger.info(
                f'Total time for commit : {total_time_end - total_time_start} : added {total_records_counter} records'
            )

    return "Success"  # Возвращаем подтверждение


def avg_user_mood_set_worker() -> str:
    '''
    1. Запустить функцию вручную
    2. Получить все записи weight у всех user_id за все периоды существования данного user_id, с разбиением по дням
    3. Для каждого user_id посчитать среднеарифметический weight за каждый прошедший день отдельно, в случае отсутствия записей вернуть None
    4. Делает запись в AverageMoodORM для каждого user_id отдельнор для каждого прошедшего дня с усреднённым за этот день weight и указанием даты дня формата гггг.мм.дд
    5. По мере записи в AverageMoodORM выводить прогресс в консоль в виде соотношения: всего нужно сделать N записей / N записано к текущему моменту (можно оформить как доп. функцию)
    :return: либо: log_string(f'user_id : start_time : end_time : end_time-start_time : количество внесённых записей : количество существующих записей до внесения : обновлённое количество записей') -
             такие строки должны быть для всех пользователей. В самом конце должно быть (f'total_time_start : total_time_end : total_time_end-total_time_start : всего внесено N пользователей')
             лабо: ошибку с указанием, где произошло (log_string для ошибку), использовать существующий логгер
                    # Настройка логгера
                        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        filename = 'app.log',  # Имя файла для хранения логов
                        filemode = 'a'  # Режим записи: 'a' для добавления, 'w' для перезаписи
                    )
                    logger = logging.getLogger(__name__)
    '''

    total_time_start = datetime.now()
    total_records_inserted = 0

    with sync_session_fabric() as session:
        try:
            # Получаем список всех уникальных user_id
            user_ids = session.query(UserORM.user_id).all()
            total_users = len(user_ids)
            logger.info(f'Found {total_users} unique users for mood processing.')

            if total_users == 0:
                logger.info('No users found for mood processing.')
                return "No users found."

            for (user_id,) in user_ids:
                try:
                    # Получаем все уникальные даты, на которые есть записи настроения данного пользователя
                    distinct_dates = session.query(func.date(MoodORM.date)).filter(MoodORM.user_id == user_id).distinct().all()

                    for (mood_date,) in distinct_dates:
                        # Рассчитываем среднее значение weight для каждого дня
                        average_weight = session.query(func.avg(cast(MoodORM.weight, Integer))).filter(
                            MoodORM.user_id == user_id,
                            func.date(MoodORM.date) == mood_date,
                        ).scalar()

                        avg_mood_entry = AverageMoodORM(
                            user_id=user_id,
                            avg_mood_weight=average_weight,  # может быть None, если не найдено
                            date=mood_date,
                        )

                        # Добавляем запись в очередь
                        session.add(avg_mood_entry)
                        total_records_inserted += 1

                        # Логи
                        logger.info(f"Inserted average mood for user_id {user_id} on date {mood_date}: {average_weight}")

                except Exception as e:
                    logger.error(f"Error processing mood data for user_id {user_id} on date {mood_date}: {str(e)}")
                    continue  # продолжить выполнение для следующего пользователя

        except Exception as e:
            logger.error(f"Error processing user records: {str(e)}")
            return f'ERROR during the batch operation: {str(e)}'

        finally:
            session.commit()
            total_time_end = datetime.now()
            logger.info(f'Total records inserted: {total_records_inserted}.')
            logger.info(f'Total time taken: {total_time_end - total_time_start} seconds.')

    return "Success"


def get_statistic_user_mood(user_id: str, period=None) -> dict:
    '''
    1. Получить все записи на пользователя user_id из AverageMoodORM за указанный период.
       ВАЖНО: период может быть одним конкретным днём, конкретным месяцем, конкретным годом, всем периодом существования пользователя в случае None
    2. ЕСЛИ период ограничен конкретным днём, то берём все данные weight из на user_id из MoodORM
    3. ИНОЕ — берём все данные weight из на user_id из AverageMoodORM за указанный период
    4. Возвращаем dict по принципу:
       ЕСЛИ период ограничем конкретным днём, то {'hh:mm': weight}
       ЕСЛИ период ограничен месяцем, то {'day_number_in_the_month': weight}
       ЕСЛИ период ограничен годом, то {'month': average_weight_for_this_month}
    :return: dict or None
    '''
    pass

def get_detail_day_statistic_user_mood(user_id: str, target_date: date = None) -> dict | None:
    '''
    1. Получить все записи на пользователя user_id из AverageMoodORM за указанный день, формат гггг.мм.дд. Если None - данные за вчера.
    2. Берём все данные weight, mood и why на user_id из MoodORM
    :return: dict {mood: (time hh:mm, weight, why)} или None
    '''

    # Если дата не указана, получаем данные за вчера
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    start_of_day = datetime.combine(target_date, datetime.min.time())  # Начало дня
    end_of_day = datetime.combine(target_date, datetime.max.time())  # Конец дня

    with sync_session_fabric() as session:
        # Запрашиваем все записи для пользователя за указанный день
        query = session.query(MoodORM).filter(
            MoodORM.user_id == user_id,
            MoodORM.date >= start_of_day,
            MoodORM.date <= end_of_day,
        ).all()

    # Если не найдено записей, возвращаем None
    if not query:
        logging.info(f'WARNING: No records for user {user_id} on date {target_date}.')
        return None

    # Создаем словарь для статистики
    mood_statistics = {}
    num_of_records = len(query)

    for mood_record in query:
        # Форматирование времени и заполнение словаря
        formatted_time = mood_record.date.strftime('%H:%M')
        mood_statistics[mood_record.mood] = (formatted_time, mood_record.weight, mood_record.why)
        logging.info(f'For {user_id=}: {formatted_time}, Mood: {mood_record.mood}, Weight: {mood_record.weight}, Why: {mood_record.why}')

    logging.info(f'For {user_id=} extracted {num_of_records} records on date {target_date}.')

    return mood_statistics


