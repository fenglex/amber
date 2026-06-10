"""
任务调度框架主入口 - FastAPI版本
"""

import argparse
import sys
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import uvicorn
import asyncio
import json
from datetime import datetime

from .core.database import init_db, get_session, Task, TaskRun, TaskLog, RunStatus
from .core.scheduler import scheduler
from .core.task import task
from sqlalchemy import desc

from . import __version__


import os

templates = None

WEB_DIR = Path(__file__).resolve().parent / "web"

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")


def format_datetime(value):
    """格式化时间为本地时间显示"""
    if not value:
        return ""
    if isinstance(value, str):
        try:
            from dateutil import parser
            dt = parser.parse(value)
        except:
            return value[:19].replace('T', ' ')
    else:
        dt = value
    
    return dt.strftime("%Y-%m-%d %H:%M:%S")


@asynccontextmanager
async def lifespan(app: FastAPI, db_path: str = None):
    """应用生命周期管理"""
    global templates
    templates = Jinja2Templates(directory=str(WEB_DIR / "templates"))
    templates.env.filters["local_time"] = format_datetime
    
    init_db(db_path)
    task.register_all()
    
    scheduler.init_database(db_path)
    scheduler.set_app(app)
    
    scheduler.start()
    
    yield
    
    scheduler.stop()


def create_app(db_path: str = None) -> FastAPI:
    """创建FastAPI应用"""
    app = FastAPI(
        title="Task Schedule",
        description="Python定时任务调度框架",
        version=__version__,
        lifespan=lambda app: lifespan(app, db_path),
    )
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    app.mount("/static", StaticFiles(directory=str(WEB_DIR / "static")), name="static")
    
    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        """首页"""
        session = get_session()
        try:
            tasks = session.query(Task).all()
            tasks_data = []
            for t in tasks:
                last_run = (
                    session.query(TaskRun)
                    .filter_by(task_id=t.id)
                    .order_by(desc(TaskRun.created_at))
                    .first()
                )
                tasks_data.append({
                    "task": t.to_dict(),
                    "last_run": last_run.to_dict() if last_run else None,
                })
            return templates.TemplateResponse("index.html", {"request": request, "tasks": tasks_data})
        finally:
            session.close()
    
    @app.get("/task/{task_id}", response_class=HTMLResponse)
    async def task_detail(request: Request, task_id: str):
        """任务详情页"""
        session = get_session()
        try:
            t = session.query(Task).filter_by(task_id=task_id).first()
            if not t:
                return templates.TemplateResponse("404.html", {"request": request, "message": f"任务 {task_id} 不存在"}, status_code=404)
            
            runs = (
                session.query(TaskRun)
                .filter_by(task_id=t.id)
                .order_by(desc(TaskRun.created_at))
                .limit(50)
                .all()
            )
            
            return templates.TemplateResponse(
                "task_detail.html",
                {
                    "request": request,
                    "task": t.to_dict(),
                    "runs": [r.to_dict() for r in runs],
                }
            )
        finally:
            session.close()
    
    @app.get("/run/{run_id}", response_class=HTMLResponse)
    async def run_detail(request: Request, run_id: str):
        """运行记录详情页"""
        session = get_session()
        try:
            run = session.query(TaskRun).filter_by(run_id=run_id).first()
            if not run:
                return templates.TemplateResponse("404.html", {"request": request, "message": f"运行记录 {run_id} 不存在"}, status_code=404)
            
            task_info = session.query(Task).filter_by(id=run.task_id).first()
            
            logs = (
                session.query(TaskLog)
                .filter_by(run_id=run.id)
                .order_by(TaskLog.log_time)
                .all()
            )
            
            return templates.TemplateResponse(
                "run_detail.html",
                {
                    "request": request,
                    "run": run.to_dict(),
                    "task": task_info.to_dict() if task_info else None,
                    "logs": [log.to_dict() for log in logs],
                }
            )
        finally:
            session.close()
    
    @app.get("/api/tasks")
    async def api_tasks():
        """获取任务列表"""
        session = get_session()
        try:
            tasks = session.query(Task).all()
            result = []
            for t in tasks:
                last_run = (
                    session.query(TaskRun)
                    .filter_by(task_id=t.id)
                    .order_by(desc(TaskRun.created_at))
                    .first()
                )
                result.append({
                    "task": t.to_dict(),
                    "last_run": last_run.to_dict() if last_run else None,
                })
            return result
        finally:
            session.close()
    
    @app.get("/api/task/{task_id}")
    async def api_task_detail(task_id: str):
        """获取任务详情"""
        session = get_session()
        try:
            t = session.query(Task).filter_by(task_id=task_id).first()
            if not t:
                raise HTTPException(status_code=404, detail="任务不存在")
            
            runs = (
                session.query(TaskRun)
                .filter_by(task_id=t.id)
                .order_by(desc(TaskRun.created_at))
                .limit(50)
                .all()
            )
            
            return {
                "task": t.to_dict(),
                "runs": [r.to_dict() for r in runs],
            }
        finally:
            session.close()
    
    @app.get("/api/run/{run_id}/logs")
    async def api_run_logs(run_id: str, last_log_id: int = 0):
        """获取运行日志"""
        session = get_session()
        try:
            run = session.query(TaskRun).filter_by(run_id=run_id).first()
            if not run:
                raise HTTPException(status_code=404, detail="运行记录不存在")
            
            logs = (
                session.query(TaskLog)
                .filter(TaskLog.run_id == run.id, TaskLog.id > last_log_id)
                .order_by(TaskLog.log_time)
                .all()
            )
            
            return {
                "status": run.status,
                "logs": [log.to_dict() for log in logs],
                "is_running": run.status == RunStatus.RUNNING.value,
            }
        finally:
            session.close()
    
    @app.post("/api/task/{task_id}/pause")
    async def api_pause_task(task_id: str):
        """暂停任务"""
        scheduler.pause_task(task_id)
        return {"success": True}
    
    @app.post("/api/task/{task_id}/resume")
    async def api_resume_task(task_id: str):
        """恢复任务"""
        scheduler.resume_task(task_id)
        return {"success": True}
    
    @app.post("/api/task/{task_id}/run")
    async def api_run_task(task_id: str):
        """立即运行任务"""
        scheduler.run_task_now(task_id)
        return {"success": True}
    
    @app.get("/api/logs/stream/{run_id}")
    async def log_stream(run_id: str):
        """日志流"""
        session = get_session()
        try:
            run = session.query(TaskRun).filter_by(run_id=run_id).first()
            if not run:
                raise HTTPException(status_code=404, detail="运行记录不存在")
            
            async def generate():
                last_log_id = 0
                while True:
                    logs = (
                        session.query(TaskLog)
                        .filter(TaskLog.run_id == run.id, TaskLog.id > last_log_id)
                        .order_by(TaskLog.log_time)
                        .all()
                    )
                    
                    for log in logs:
                        yield f"[{log.log_time.strftime('%Y-%m-%d %H:%M:%S')}] [{log.log_level}] {log.log_message}\n"
                        last_log_id = log.id
                    
                    run = session.query(TaskRun).filter_by(run_id=run_id).first()
                    if run and run.status != RunStatus.RUNNING.value:
                        break
                    
                    await asyncio.sleep(1)
            
            return StreamingResponse(generate(), media_type="text/plain")
        finally:
            session.close()
    
    @app.get("/api/websocket")
    async def websocket_endpoint():
        """WebSocket端点（保留用于未来扩展）"""
        return {"message": "WebSocket endpoint - use SSE for real-time logs"}
    
    return app


def run(
    host: str = "0.0.0.0",
    port: int = 5000,
    db_path: str = None,
    debug: bool = False,
):
    """
    运行任务调度服务

    Args:
        host: 绑定地址 (默认: 0.0.0.0)
        port: 端口号 (默认: 5000)
        db_path: 数据库路径 (默认: task_schedule.db)
        debug: 开启调试模式
    """
    app = create_app(db_path=db_path)

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║                   Task Schedule {__version__}                        ║
║                                                              ║
║   数据库: {db_path if db_path else 'task_schedule.db'}                          ║
║   监听: http://{host}:{port}                        ║
║                                                              ║
║   使用 Ctrl+C 停止服务                                        ║
╚══════════════════════════════════════════════════════════════╝
    """)

    uvicorn.run(
        app,
        host=host,
        port=port,
        reload=debug,
    )


def main():
    """主函数（命令行入口）"""
    parser = argparse.ArgumentParser(description="Task Schedule - Python定时任务调度框架")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="绑定地址 (默认: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=5000, help="端口号 (默认: 5000)")
    parser.add_argument("--debug", action="store_true", help="开启调试模式")
    parser.add_argument("--db-path", type=str, default=None, help="数据库路径 (默认: task_schedule.db)")

    args = parser.parse_args()

    run(
        host=args.host,
        port=args.port,
        db_path=args.db_path,
        debug=args.debug,
    )


if __name__ == "__main__":
    main()

