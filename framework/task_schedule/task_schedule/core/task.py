"""
任务装饰器定义

自动包装任务函数，提供日志捕获、AOP 风格的任务前后处理
"""

import functools
import threading
from datetime import datetime
from typing import Callable, Optional, Union
import sys
import traceback

from ..core.database import Task, TriggerType, get_session
from ..core.scheduler import scheduler
from ..utils.helper import generate_run_id


class TaskDecorator:
    """任务装饰器类 - 支持 AOP 风格的自动日志捕获"""

    def __init__(self):
        self._task_configs = {}
        self._task_functions = {}
        self._raw_functions = {}  # 保存原始函数

    def __call__(
        self,
        task_id: str = None,
        description: str = "",
        trigger: str = "interval",
        interval: int = None,
        cron: str = None,
    ):
        """
        任务装饰器

        Args:
            task_id: 任务唯一标识
            description: 任务描述
            trigger: 触发类型 ('interval' 或 'crontab')
            interval: 间隔时间（秒），trigger为'interval'时必填
            cron: crontab表达式，trigger为'crontab'时必填
        """
        def decorator(func: Callable):
            actual_task_id = task_id or func.__name__

            @functools.wraps(func)
            def wrapper(*args, force_run=False, **kwargs):
                return self._execute_task(actual_task_id, func, force_run=force_run, *args, **kwargs)

            task_config = {
                "task_id": actual_task_id,
                "description": description,
                "trigger": trigger,
                "interval": interval,
                "cron": cron,
                "func": func,
                "func_name": func.__name__,
                "module_name": func.__module__,
            }

            self._task_configs[actual_task_id] = task_config
            self._task_functions[actual_task_id] = wrapper
            self._raw_functions[actual_task_id] = func

            return wrapper
        return decorator

    def _execute_task(self, task_id: str, func: Callable, force_run: bool = False, *args, **kwargs):
        """
        执行任务的核心方法
        负责创建运行记录、捕获日志、处理异常等
        """
        from ..utils.log_capture import _get_thread_data
        from ..core.database import RunStatus, TaskRun, get_session
        from ..utils.log_capture import capture_task_logs
        from loguru import logger

        session = get_session()
        run_id = generate_run_id(task_id)

        try:
            task_info = session.query(Task).filter_by(task_id=task_id).first()
            if not task_info:
                return None
            
            if not task_info.is_active and not force_run:
                return None

            # 检查是否有任务正在运行
            active_run = (
                session.query(TaskRun)
                .filter(
                    TaskRun.task_id == task_info.id,
                    TaskRun.status == RunStatus.RUNNING.value,
                )
                .first()
            )

            if active_run:
                # 任务正在运行，跳过本次执行
                self._create_run_record(
                    session, task_info.id, run_id, RunStatus.SKIPPED.value,
                    "任务正在运行中，跳过本次执行"
                )
                logger.warning(f"任务 {task_id} 正在运行中，跳过本次执行")
                return None

            # 创建运行记录
            self._create_run_record(
                session, task_info.id, run_id, RunStatus.RUNNING.value
            )

            def emit_log(run_id: str, level: str, message: str):
                """日志发射回调 - 接受 run_id, level, message"""
                scheduler.emit_log(run_id, level, message, save_to_db=True)

            # 开始执行任务
            start_time = datetime.now()


            # 将 run_id 保存到线程本地存储，供 run_task 使用（必须在 capture_task_logs 之前）
            _get_thread_data()['run_id'] = run_id

            # 执行包装后的函数（自动捕获日志）
            with capture_task_logs(run_id, emit_log):
                result = func(*args, **kwargs)

            # 在 capture_task_logs 结束后，仍然可以使用 run_id
            _get_thread_data()['run_id'] = run_id

            end_time = datetime.now()

            # 更新运行状态为成功
            self._update_run_status(
                session, run_id, RunStatus.SUCCESS.value,
                end_time=end_time,
            )
            
            # 发送任务完成日志
            emit_log(run_id, "INFO", f"任务执行成功，耗时: {(end_time - start_time).total_seconds():.2f}秒")
            return result

        except Exception as e:
            error_msg = traceback.format_exc()
            self._update_run_status(
                session, run_id, RunStatus.FAILED.value,
                error_message=str(e),
            )
            scheduler.emit_log(run_id, "ERROR", f"任务执行失败: {str(e)}\n{error_msg}")
            logger.error(f"任务 {task_id} 执行失败: {e}")
            raise

        finally:
            session.close()

    def _create_run_record(self, session, task_id: int, run_id: str, status: str, error_message: str = None):
        """创建运行记录"""
        from ..core.database import TaskRun

        run = TaskRun(
            run_id=run_id,
            task_id=task_id,
            status=status,
            start_time=datetime.now(),
            error_message=error_message,
        )
        session.add(run)
        session.commit()
        
        self._cleanup_old_records(session, task_id)

    def _cleanup_old_records(self, session, task_id: int, keep_count: int = 100):
        """清理每个任务的旧运行记录"""
        from sqlalchemy import delete
        from ..core.database import TaskRun, TaskLog

        subquery = (
            session.query(TaskRun.id)
            .filter_by(task_id=task_id)
            .order_by(TaskRun.created_at.desc())
            .offset(keep_count)
            .subquery()
        )

        delete_logs = delete(TaskLog).where(TaskLog.run_id.in_(subquery))
        session.execute(delete_logs)

        delete_runs = delete(TaskRun).where(
            TaskRun.task_id == task_id,
            ~TaskRun.id.in_(
                session.query(TaskRun.id)
                .filter_by(task_id=task_id)
                .order_by(TaskRun.created_at.desc())
                .limit(keep_count)
            ),
        )
        session.execute(delete_runs)
        session.commit()

    def _update_run_status(self, session, run_id: str, status: str, end_time: datetime = None, error_message: str = None):
        """更新运行状态"""
        from ..core.database import TaskRun

        run = session.query(TaskRun).filter_by(run_id=run_id).first()
        if run:
            run.status = status
            run.end_time = end_time or datetime.now()
            if error_message:
                run.error_message = error_message
            session.commit()

    def register_all(self):
        """注册所有任务到调度器"""
        for task_id, config in self._task_configs.items():
            # 使用包装后的函数
            wrapper = self._task_functions.get(task_id) or config["func"]
            scheduler.register_task(
                task_id=task_id,
                func=wrapper,
                description=config["description"],
                trigger=config["trigger"],
                interval=config["interval"],
                cron=config["cron"],
            )

    def get_raw_function(self, task_id: str) -> Optional[Callable]:
        """获取任务的原始函数"""
        return self._raw_functions.get(task_id)
    
    def get_wrapped_function(self, task_id: str) -> Optional[Callable]:
        """获取任务的包装函数"""
        return self._task_functions.get(task_id)
    
    def run_task(self, parent_run_id: str, task_id: str, *args, **kwargs):
        """
        在父任务中调用子任务，子任务的日志会自动归属到父任务
        
        使用示例:
            @task(task_id="parent_task")
            def parent_task():
                self.run_task(run_id, "child_task")  # child_task 的日志会显示在 parent_task 的日志历史中
        
        Args:
            parent_run_id: 父任务的 run_id
            task_id: 要调用的子任务 ID
            *args: 传递给子任务的参数
            **kwargs: 传递给子任务的关键字参数
        """
        from ..utils.log_capture import set_parent_run_id, clear_parent_run_id
        
        raw_func = self.get_raw_function(task_id)
        if not raw_func:
            raise ValueError(f"任务 {task_id} 未找到")
        
        # 设置 parent_run_id
        set_parent_run_id(parent_run_id)
        try:
            # 调用原始函数（不经过包装器，避免创建新的运行记录）
            result = raw_func(*args, **kwargs)
            return result
        finally:
            # 清除 parent_run_id
            clear_parent_run_id()


task = TaskDecorator()
