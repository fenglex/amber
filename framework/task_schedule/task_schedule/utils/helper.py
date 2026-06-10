"""
工具函数
"""

from datetime import datetime


def generate_run_id(task_id: str) -> str:
    """
    生成运行实例ID
    格式: 202601120910100000123456_taskid

    Args:
        task_id: 任务ID

    Returns:
        运行实例ID
    """
    import uuid
    timestamp = datetime.now()
    run_id = timestamp.strftime("%Y%m%d%H%M%S") + str(uuid.uuid4().int)[:6]
    return f"{run_id}_{task_id}"


def parse_cron_expression(cron_str: str) -> dict:
    """
    解析crontab表达式

    Args:
        cron_str: crontab表达式，如 "*/5 * * * *"

    Returns:
        解析后的字典
    """
    parts = cron_str.split()
    if len(parts) != 5:
        raise ValueError("无效的crontab表达式")

    return {
        "minute": parts[0],
        "hour": parts[1],
        "day": parts[2],
        "month": parts[3],
        "day_of_week": parts[4],
    }


def format_duration(seconds: float) -> str:
    """
    格式化时间间隔

    Args:
        seconds: 秒数

    Returns:
        格式化的时间字符串
    """
    if seconds < 60:
        return f"{seconds:.2f}秒"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.2f}分钟"
    elif seconds < 86400:
        hours = seconds / 3600
        return f"{hours:.2f}小时"
    else:
        days = seconds / 86400
        return f"{days:.2f}天"
