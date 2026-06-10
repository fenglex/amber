"""
重构后的日志捕获系统

使用线程本地存储，确保每个任务都能独立捕获日志，互不干扰。
自动捕获 print 和 loguru 的日志输出，并按产生顺序输出。
"""

import sys
import threading
from datetime import datetime
from typing import Callable, Optional
from contextlib import contextmanager
from loguru import logger


# 线程本地存储 - 用于隔离不同任务的数据
_local = threading.local()
# 全局 sink ID 列表，用于跟踪所有添加的 sink
_global_sink_ids = []


def _get_thread_data():
    """获取当前线程的本地数据"""
    if not hasattr(_local, 'data'):
        _local.data = {
            'run_id': None,
            'parent_run_id': None,  # 父任务的 run_id，用于嵌套任务日志传递
            'original_stdout': None,
            'original_stderr': None,
            'log_buffer': None,  # 当前任务的日志缓冲区
            'emit_callback': None,
            'sink_id': None,  # 当前任务的 loguru sink ID
        }
    return _local.data


def set_parent_run_id(parent_run_id: str):
    """设置父任务的 run_id，用于嵌套任务日志传递"""
    data = _get_thread_data()
    data['parent_run_id'] = parent_run_id


def clear_parent_run_id():
    """清除父任务的 run_id"""
    data = _get_thread_data()
    data['parent_run_id'] = None


def get_effective_run_id() -> str:
    """获取有效的 run_id，如果有父任务 run_id 则使用父任务的"""
    data = _get_thread_data()
    return data.get('parent_run_id') or data.get('run_id')


class LogCapture:
    """日志捕获器 - 使用线程本地存储"""
    
    def __init__(self):
        self._lock = threading.Lock()
    
    def start(self, run_id: str, emit_callback: Callable):
        """开始捕获当前任务的日志"""
        data = _get_thread_data()
        
        # 停止之前的捕获（如果有）
        if data.get('log_buffer') is not None:
            self.stop()
        
        data['run_id'] = run_id
        data['emit_callback'] = emit_callback
        
        # 创建新的日志缓冲区
        data['log_buffer'] = []
        
        # 保存原始 stdout/stderr
        data['original_stdout'] = sys.stdout
        data['original_stderr'] = sys.stderr
        
        # 创建自定义 stdout 用于捕获 print
        class CaptureStdout:
            def __init__(self, original, buffer, run_id):
                self.original = original
                self.buffer = buffer
                self.run_id = run_id
                self.closed = False
            
            def write(self, text):
                if self.closed:
                    return 0
                if text.strip():
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    self.buffer.append({
                        'timestamp': timestamp,
                        'level': 'INFO',
                        'message': f"{timestamp} | INFO     | <stdin>:1 - {text.strip()}",
                        'run_id': self.run_id
                    })
                self.original.write(text)
                self.original.flush()
                return len(text)
            
            def flush(self):
                self.original.flush()
            
            def isatty(self):
                return False
            
            def readable(self):
                return True
            
            def writable(self):
                return True
            
            def seekable(self):
                return False
        
        # 重定向 stdout/stderr
        sys.stdout = CaptureStdout(data['original_stdout'], data['log_buffer'], run_id)
        sys.stderr = CaptureStdout(data['original_stderr'], data['log_buffer'], run_id)
        
        # 添加 loguru sink
        current_run_id = run_id
        current_buffer = data['log_buffer']
        
        def loguru_sink(message):
            """Loguru sink 回调函数 - 只捕获当前任务的日志"""
            try:
                current_data = _get_thread_data()
                
                # 验证当前任务的 run_id 是否匹配
                if current_data.get('run_id') != current_run_id:
                    return
                
                # 验证日志缓冲区是否是同一个
                if current_data.get('log_buffer') is not current_buffer:
                    return
                
                text = str(message).strip()
                if text:
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    current_buffer.append({
                        'timestamp': timestamp,
                        'level': 'INFO',
                        'message': text,
                        'run_id': current_run_id
                    })
            except Exception:
                pass
        
        # 添加 sink 并保存 ID
        sink_id = logger.add(loguru_sink)
        data['sink_id'] = sink_id
        _global_sink_ids.append(sink_id)
    
    def stop(self):
        """停止捕获日志并恢复"""
        data = _get_thread_data()
        
        if data.get('log_buffer') is None:
            return
        
        effective_run_id = get_effective_run_id()
        run_id = data.get('run_id')
        emit_callback = data.get('emit_callback')
        log_buffer = data.get('log_buffer')
        sink_id = data.get('sink_id')
        
        # 移除 loguru sink
        if sink_id is not None:
            try:
                logger.remove(sink_id)
                if sink_id in _global_sink_ids:
                    _global_sink_ids.remove(sink_id)
            except Exception:
                pass
        
        # 恢复 stdout/stderr
        if data.get('original_stdout'):
            sys.stdout = data['original_stdout']
        if data.get('original_stderr'):
            sys.stderr = data['original_stderr']
        
        # 发送所有日志
        if log_buffer and emit_callback:
            for log_entry in log_buffer:
                if log_entry.get('run_id') == effective_run_id:
                    emit_callback(effective_run_id, log_entry['level'], log_entry['message'])
        
        # 清理数据
        data['run_id'] = None
        data['parent_run_id'] = None
        data['emit_callback'] = None
        data['log_buffer'] = None
        data['original_stdout'] = None
        data['original_stderr'] = None
        data['sink_id'] = None
    
    def flush(self):
        """刷新日志"""
        data = _get_thread_data()
        
        effective_run_id = get_effective_run_id()
        run_id = data.get('run_id')
        emit_callback = data.get('emit_callback')
        log_buffer = data.get('log_buffer')
        
        if not log_buffer or not emit_callback:
            return
        
        # 发送新增的日志
        for log_entry in log_buffer:
            if log_entry.get('run_id') == effective_run_id:
                emit_callback(effective_run_id, log_entry['level'], log_entry['message'])
        
        # 清空已发送的日志
        data['log_buffer'] = []
    
    def is_active(self) -> bool:
        """检查当前线程的日志捕获是否激活"""
        data = _get_thread_data()
        return data.get('log_buffer') is not None


# 全局日志捕获器实例
task_log_capture = LogCapture()


@contextmanager
def capture_task_logs(run_id: str, emit_callback: Callable):
    """
    上下文管理器，用于捕获任务执行期间的日志
    
    使用示例:
        with capture_task_logs(run_id, emit_callback):
            print("这是一条 print 日志")
            logger.info("这是一条 loguru 日志")
    
    Args:
        run_id: 运行ID
        emit_callback: 日志回调函数，接收 (run_id, level, message) 参数
    """
    task_log_capture.start(run_id, emit_callback)
    try:
        yield
    finally:
        task_log_capture.stop()


def get_task_logger(name: str = None):
    """
    获取任务专用的 logger

    Returns:
        loguru logger 实例
    """
    return logger


# 向后兼容：旧版 TaskLogger 类的别名
class LegacyTaskLogger:
    """旧版任务日志类（已废弃，使用 get_task_logger 替代）"""

    def __init__(self, run_id: str):
        self.run_id = run_id

    def info(self, message: str):
        logger.info(message)

    def error(self, message: str):
        logger.error(message)

    def warning(self, message: str):
        logger.warning(message)

    def debug(self, message: str):
        logger.debug(message)


# 向后兼容：旧版 LogCapture 类
class LegacyLogCapture:
    """旧版日志捕获类（已废弃，使用 LogCapture 替代）"""

    def __init__(self, run_id: str):
        self.run_id = run_id
        self.original_stderr = sys.stderr
        self.original_stdout = sys.stdout

    def write(self, message: str):
        if message.strip():
            self.original_stderr.write(message)
            self.original_stderr.flush()
        return len(message)

    def flush(self):
        self.original_stderr.flush()
        self.original_stdout.flush()


def setup_logger(run_id: str):
    """
    设置任务的日志输出（已废弃，保留向后兼容）

    Args:
        run_id: 运行实例ID
    """
    pass


def restore_logger():
    """恢复默认日志设置（已废弃，保留向后兼容）"""
    pass


def get_task_logger_legacy(run_id: str):
    """
    获取任务日志实例（已废弃，使用 get_task_logger 替代）

    Args:
        run_id: 运行实例ID

    Returns:
        LegacyTaskLogger实例
    """
    return LegacyTaskLogger(run_id)
