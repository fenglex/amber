"""
数据库模型定义
"""

from datetime import datetime
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Boolean,
    Enum,
    ForeignKey,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, scoped_session
import enum
import os

Base = declarative_base()


class RunStatus(enum.Enum):
    """任务运行状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class TriggerType(enum.Enum):
    """触发器类型枚举"""
    CRONTAB = "crontab"
    INTERVAL = "interval"


class Task(Base):
    """任务模型"""
    __tablename__ = "tasks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(100), unique=True, nullable=False, index=True)
    description = Column(String(500))
    func_name = Column(String(200), nullable=False)
    module_name = Column(String(500), nullable=False)
    trigger_type = Column(String(20), nullable=False)
    cron_expression = Column(String(100))
    interval_seconds = Column(Integer)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    runs = relationship("TaskRun", back_populates="task", cascade="all, delete-orphan")

    def to_dict(self):
        """转换为字典"""
        return {
            "id": self.id,
            "task_id": self.task_id,
            "description": self.description,
            "func_name": self.func_name,
            "module_name": self.module_name,
            "trigger_type": self.trigger_type.value if isinstance(self.trigger_type, TriggerType) else self.trigger_type,
            "cron_expression": self.cron_expression,
            "interval_seconds": self.interval_seconds,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class TaskRun(Base):
    """任务运行记录模型"""
    __tablename__ = "task_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(50), unique=True, nullable=False, index=True)
    task_id = Column(Integer, ForeignKey("tasks.id"), nullable=False)
    status = Column(String(20), nullable=False, default=RunStatus.PENDING.value)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    error_message = Column(Text)
    created_at = Column(DateTime, default=datetime.now)

    task = relationship("Task", back_populates="runs")
    logs = relationship("TaskLog", back_populates="run", cascade="all, delete-orphan")

    def to_dict(self):
        """转换为字典"""
        return {
            "id": self.id,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "status": self.status.value if isinstance(self.status, RunStatus) else self.status,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class TaskLog(Base):
    """任务日志模型"""
    __tablename__ = "task_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey("task_runs.id"), nullable=False)
    sequence = Column(Integer, nullable=False, default=0)  # 日志序号，保证顺序
    log_level = Column(String(10), nullable=False)
    log_message = Column(Text, nullable=False)
    log_time = Column(DateTime, default=datetime.now)
    created_at = Column(DateTime, default=datetime.now)

    run = relationship("TaskRun", back_populates="logs")

    def to_dict(self):
        """转换为字典"""
        return {
            "id": self.id,
            "run_id": self.run_id,
            "sequence": self.sequence,
            "log_level": self.log_level,
            "log_message": self.log_message,
            "log_time": self.log_time.isoformat() if self.log_time else None,
        }


# 数据库配置
DATABASE_URL = "sqlite:///task_schedule.db"
engine = None
session_factory = None


def init_db(db_path=None):
    """初始化数据库"""
    global engine, session_factory

    if db_path:
        DATABASE_URL = f"sqlite:///{db_path}"
    else:
        DATABASE_URL = "sqlite:///task_schedule.db"

    engine = create_engine(
        DATABASE_URL,
        echo=False,
        connect_args={"check_same_thread": False},
    )
    session_factory = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    Base.metadata.create_all(bind=engine)
    return session_factory


def get_session():
    """获取数据库会话"""
    global session_factory
    if session_factory is None:
        init_db()
    return session_factory()


def get_engine():
    """获取数据库引擎"""
    global engine
    if engine is None:
        init_db()
    return engine
