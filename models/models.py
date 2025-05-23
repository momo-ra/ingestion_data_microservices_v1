import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, UniqueConstraint, PrimaryKeyConstraint, Index, create_engine, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.sql import func
from core.config import settings
from datetime import datetime

Base = declarative_base()

class Tag(Base):
    __tablename__ = "tag"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(String, nullable=True)
    unit_of_measure = Column(String, nullable=True)
    subscriptions = relationship("subscription_tasks", back_populates="tag")

class TimeSeries(Base):
    __tablename__ = "time_series"
    # شيلنا id column لأننا هنستخدم composite primary key
    tag_id = Column(Integer, ForeignKey("tag.id"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    value = Column(String, nullable=False)
    frequency = Column(String, nullable=False)

    # عملنا primary key مركب من tag_id و timestamp
    __table_args__ = (
        PrimaryKeyConstraint('tag_id', 'timestamp'),
        Index('idx_time_series_tag_time', 'tag_id', 'timestamp', unique=True),
        Index('idx_time_series_frequency', 'frequency', 'timestamp'),
    )

class Alerts(Base):
    __tablename__ = "alerts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    tag_id = Column(Integer, ForeignKey("tag.id"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    message = Column(String, nullable=False)

class polling_tasks(Base):
    __tablename__ = "polling_tasks"
    id = Column(Integer, primary_key=True, autoincrement=True)
    tag_id = Column(Integer, ForeignKey("tag.id"), nullable=False)
    time_interval = Column(Integer, nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    last_polled = Column(DateTime, nullable=True)
    next_polled = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    __table_args__ = (
        UniqueConstraint('tag_id', 'time_interval'),
    )

class subscription_tasks(Base):
    """Model for storing OPC UA subscription tasks"""
    __tablename__ = "subscription_tasks"
    
    id = Column(Integer, primary_key=True, index=True)
    tag_id = Column(Integer, ForeignKey("tag.id"))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)
    last_updated = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # Relationship
    tag = relationship("Tag", back_populates="subscriptions")

engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)