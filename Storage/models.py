from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped
from sqlalchemy import Integer, String, Float, func, DateTime, VARCHAR

class Base(DeclarativeBase):
    pass

class PlayerSnapshot(Base):
    __tablename__ = "player_snapshot"
    id = mapped_column(Integer, primary_key=True)
    trace_id = mapped_column(VARCHAR(64), nullable=False)
    match_id = mapped_column(String(64), nullable=False)
    player_puuid = mapped_column(String(64), nullable=False)
    game_time_seconds = mapped_column(Integer, nullable=False)
    kills = mapped_column(Integer, nullable=False)
    deaths = mapped_column(Integer, nullable=False)
    assists = mapped_column(Integer, nullable=False)
    cs = mapped_column(Integer, nullable=False)
    gold = mapped_column(Integer, nullable=False)
    lane = mapped_column(String(7), nullable=False)
    date_created = mapped_column(DateTime, nullable=False, default=func.now())

class MatchEvent(Base):
    __tablename__ = "match_event"
    id = mapped_column(Integer, primary_key=True)
    trace_id = mapped_column(VARCHAR(64), nullable=False)
    match_id = mapped_column(String(64), nullable=False)
    event_type = mapped_column(String(16), nullable=False)
    game_time_seconds = mapped_column(Integer, nullable=False)
    team_id = mapped_column(Integer, nullable=False)
    x = mapped_column(Float, nullable=False)
    y = mapped_column(Float, nullable=False)
    killer_puuid = mapped_column(String(64), nullable=True)
    date_created = mapped_column(DateTime, nullable=False, default=func.now())