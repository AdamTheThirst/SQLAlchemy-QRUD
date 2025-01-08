from datetime import datetime, timezone, date
from typing import Optional, Annotated
from enum import Enum

from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, func, text, CheckConstraint, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from  database import Base, str_200

intpk = Annotated[int, mapped_column(primary_key=True)]

class UserORM(Base):
    '''
    Таблица с записями о всех юзерах
    '''
    __tablename__ = 'users_orm'

    id: Mapped[intpk]
    user_id: Mapped[str_200] = mapped_column(index=True, unique=True)
    username: Mapped[str_200]
    date_of_registration: Mapped[datetime] = mapped_column(server_default=text("TIMEZONE('utc', now())"))

    moods = relationship("MoodORM", back_populates="user", cascade="all, delete-orphan")
    average_moods = relationship("AverageMoodORM", back_populates="user", cascade="all, delete-orphan")

class PersonalMoodORM(Base):
    '''
    Таблица с записями персональных настроений
    user_id: к каому пользователю привязано настроение
    user_mood: название настроения пользователя
    mood_weight: какое качество этого настроения - -1 (плохое), 0 (нейтральное), 1 (хорошее)
    '''

    __tablename__ = 'personal_moods_orm'

    id: Mapped[intpk]
    user_id: Mapped[str_200] = mapped_column(foreign_key='users_orm.user_id', index=True, ondelete='CASCADE')
    user_mood: Mapped[str_200]
    mood_weight: Mapped[int] = mapped_column(check=CheckConstraint("mood_weight IN (-1, 0, 1)"))

class MoodsEnum(Enum):
    happy = 'Радостное'
    perfect = 'Отличное'
    good = 'Хорошее'
    normal = 'Обычное'
    sad = 'Грустное'
    bad = 'Плохое'
    fighty = 'Боевое'
    down = 'Упадническое'
    self_issure = 'Уверенное'
    not_self_issure = 'Не уверенное'

class WeightEnun(Enum):
    happy = 2
    perfect = 2
    good = 1
    normal = 0
    sad = -1
    bad = -2
    fighty = 2
    down = -2
    self_issure = 1
    not_self_issure = -1

class MoodORM(Base):
    '''
    Таблица с записями, какое настроение у пользователя в моменте. На один день может быть много записей
    user_id: привязка к юзеру
    mood: настроение
    weight: вес настроения: очень плохое (-2), плохое (-1), нейтральное (0), позитивное (1), очень позитивное (2)
    why: причины настроения (можно оставлять пустым)
    '''

    __tablename__ = 'moods_orm'

    id: Mapped[intpk]
    user_id: Mapped[str_200] = mapped_column(foreign_key='users_orm.user_id', index=True)
    user: Mapped[UserORM] = relationship("UserORM", back_populates="moods")
    mood: Mapped[MoodsEnum]
    weight: Mapped[WeightEnun]
    why: Mapped[str] = mapped_column(Text, nullable=True)
    date: Mapped[datetime] = mapped_column(server_default=text("TIMEZONE('utc', now())"))

class AverageMoodORM(Base):
    '''
    Табица с записями среднего настроения пользователя по дням
    '''

    __tablename__ = 'average_moods_orm'

    id: Mapped[intpk]
    user_id: Mapped[str_200] = mapped_column(foreign_key='users_orm.user_id', index=True)
    user: Mapped[UserORM] = relationship("UserORM", back_populates="average_moods")
    avg_mood_weight: Mapped[Optional[Integer]] = mapped_column(
        Integer,
        nullable=True,  # Делаем поле необязательным
        check=CheckConstraint("avg_mood_weight IN (-2, -1, 0, 1, 2) OR avg_mood_weight IS NULL")  # Учитываем значение NULL
    )
    date: Mapped[date]