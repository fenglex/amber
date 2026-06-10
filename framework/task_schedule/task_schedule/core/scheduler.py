"""
任务调度器核心实现
"""

import threading
import time
import traceback
import asyncio
from datetime import datetime
from typing import Callable, Dict, Optional, Any
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from ..core.database import (
    Task,
    TaskRun,
    TaskLog,
    RunStatus,
    TriggerType,
    get_session,
    init_db,
)
from ..utils.helper import generate_run_id


class LogManager:
    """日志管理器，用于实时日志推送"""

    def __init__(self):
        self._subscribers: Dict[str, set] = {}
        self._lock = threading.Lock()

    def subscribe(self, run_id: str):
        """订阅日志"""
        with self._lock:
            if run_id not in self._subscribers:
                self._subscribers[run_id] = set()
            self._subscribers[run_id].add(asyncio.Queue())
            return self._subscribers[run_id][-1]

    def unsubscribe(self, run_id: str, queue: asyncio.Queue):
        """取消订阅"""
        with self._lock:
            if run_id in self._subscribers:
                self._subscribers[run_id].discard(queue)

    async def emit(self, run_id: str, log_level: str, message: str):
        """发送日志到所有订阅者"""
        timestamp = datetime.now().isoformat()
        log_data = {
            "run_id": run_id,
            "log_level": log_level,
            "message": message,
            "timestamp": timestamp,
        }

        with self._lock:
            queues = list(self._subscribers.get(run_id, set()))

        for queue in queues:
            try:
                await queue.put(log_data)
            except Exception:
                pass

    def emit_sync(self, run_id: str, log_level: str, message: str):
        """同步发送日志（用于非异步上下文）"""
        try:
            # 创建一个新的事件循环来运行异步函数
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self.emit(run_id, log_level, message))
            finally:
                loop.close()
        except Exception:
            pass


log_manager = LogManager()


class TaskScheduler:
    """任务调度器类"""

    def __init__(self, db_path=None):
        self.scheduler = BackgroundScheduler(
            executors={"default": {"type": "threadpool", "max_workers": 10}},
            job_defaults={"max_instances": 3},
        )
        self.running_tasks: Dict[str, threading.Thread] = {}
        self.running_locks: Dict[str, threading.Lock] = {}
        self.db_path = db_path
        self._initialized = False
        self._app = None

    def init_database(self, db_path: str = None):
        """初始化数据库"""
        if db_path:
            self.db_path = db_path
        if not self._initialized:
            init_db(self.db_path)
            self._initialized = True

    def set_app(self, app):
        """设置FastAPI应用实例"""
        self._app = app

    def emit_log(
        self, run_id: str, log_level: str, message: str, save_to_db: bool = True
    ):
        """发送日志到Web界面并可选保存到数据库"""
        # 实时推送日志到Web界面
        if self._app:
            try:
                log_manager.emit_sync(run_id, log_level, message)
            except Exception:
                pass

        # 保存到数据库
        if not save_to_db:
            return

        session = get_session()
        try:
            run = session.query(TaskRun).filter_by(run_id=run_id).first()
            if run:
                # 获取当前日志数量作为序列号
                log_count = session.query(TaskLog).filter_by(run_id=run.id).count()
                log_entry = TaskLog(
                    run_id=run.id,
                    sequence=log_count,
                    log_level=log_level,
                    log_message=message,
                    log_time=datetime.now(),
                )
                session.add(log_entry)
                session.commit()
        except Exception as e:
            session.rollback()
        finally:
            session.close()

    def register_task(
        self,
        task_id: str,
        func: Callable,
        description: str = "",
        trigger: str = "interval",
        interval: int = None,
        cron: str = None,
    ):
        """注册任务到调度器"""
        self.init_database()
        session = get_session()

        try:
            existing_task = session.query(Task).filter_by(task_id=task_id).first()

            if existing_task:
                existing_task.description = description
                existing_task.func_name = func.__name__
                existing_task.module_name = func.__module__
                existing_task.trigger_type = trigger
                existing_task.cron_expression = cron
                existing_task.interval_seconds = interval
                existing_task.is_active = True
            else:
                new_task = Task(
                    task_id=task_id,
                    description=description,
                    func_name=func.__name__,
                    module_name=func.__module__,
                    trigger_type=trigger,
                    cron_expression=cron,
                    interval_seconds=interval,
                    is_active=True,
                )
                session.add(new_task)

            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"注册任务失败: {e}")
        finally:
            session.close()

        trigger_obj = None
        if trigger == "interval" and interval:
            trigger_obj = IntervalTrigger(seconds=interval)
        elif trigger == "crontab" and cron:
            trigger_obj = CronTrigger.from_crontab(cron)

        if trigger_obj:
            try:
                self.scheduler.remove_job(task_id)
            except Exception:
                pass
            self.scheduler.add_job(
                func=func,  # 直接使用包装后的函数，任务装饰器会处理日志捕获
                trigger=trigger_obj,
                id=task_id,
                name=task_id,
            )

        if task_id not in self.running_locks:
            self.running_locks[task_id] = threading.Lock()

    def start(self):
        """启动调度器"""
        self.init_database()
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("任务调度器已启动")

    def stop(self):
        """停止调度器"""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logger.info("任务调度器已停止")

    def pause_task(self, task_id: str):
        """暂停任务"""
        self.scheduler.pause_job(task_id)
        session = get_session()
        try:
            task = session.query(Task).filter_by(task_id=task_id).first()
            if task:
                task.is_active = False
                session.commit()
        finally:
            session.close()

    def resume_task(self, task_id: str):
        """恢复任务"""
        self.scheduler.resume_job(task_id)
        session = get_session()
        try:
            task = session.query(Task).filter_by(task_id=task_id).first()
            if task:
                task.is_active = True
                session.commit()
        finally:
            session.close()

    def run_task_now(self, task_id: str):
        """立即运行任务"""
        import functools
        from ..core.task import task as task_decorator
        wrapped_func = task_decorator.get_wrapped_function(task_id)
        if wrapped_func:
            self.scheduler.add_job(
                func=functools.partial(wrapped_func, force_run=True),
                trigger="date",
                id=f"manual_{task_id}_{int(time.time())}",
            )

    def get_task_function(self, task_id: str):
        """获取任务函数"""
        from ..core.task import task as task_decorator
        return task_decorator.get_raw_function(task_id)


scheduler = TaskScheduler()
