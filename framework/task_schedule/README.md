# Task Schedule v1.7.0

一个功能强大的Python定时任务调度框架，支持通过装饰器定义任务，提供美观现代的Web界面进行任务管理和监控。

## 更新日志

### v1.7.0 (2026-01-26)

**修复问题：**
- 修复立即运行按钮无效问题：使用包装后的函数执行任务，确保运行记录正确创建
- 修复暂停状态无法手动运行问题：新增 `force_run` 参数，暂停时允许手动触发运行
- 新增 `get_wrapped_function()` 方法：获取包装后的任务函数

**技术优化：**
- `run_task_now()` 使用 `functools.partial` 传递 `force_run=True` 参数
- 任务装饰器 wrapper 增加 `force_run=False` 参数

### v1.6.0 (2026-01-26)

**功能改进：**
- 删除 `log/` 目录：日志功能统一使用 `utils/log_capture.py`
- 删除 `requirements.txt`：依赖信息统一由 pyproject.toml 管理

**修复问题：**
- 修复时间显示问题：统一使用 `datetime.now()` 替代已弃用的 `datetime.utcnow()`
- 修复 `format_datetime` 过滤器：直接显示本地时间，不再错误转换时区
- 数据库模型中所有时间字段统一使用 `datetime.now()` 作为默认值

**技术优化：**
- 移除 `__main__.py` 中未使用的 `timedelta` 导入
- 代码一致性：所有时间相关操作统一使用 `datetime.now()`

### v1.5.0 (2026-01-26)

**重大变更：**
- 重构 Web 框架：从 Flask 迁移到 FastAPI，提升性能和异步支持
- 新增 `run()` 函数：支持直接传参调用 `run(host, port, db_path, debug)`
- 优化启动界面：显示当前配置信息

**功能改进：**
- 统一依赖配置：删除 Flask 相关依赖，统一使用 FastAPI 生态
- 合并日志系统：删除重复的 `log/logger.py`，统一使用 `utils/log_capture.py`
- 删除冗余代码：移除 Flask 版本的 `web/routes.py`
- 修复 CORS 配置：支持通过 `ALLOWED_ORIGINS` 环境变量控制

**修复问题：**
- 修复版本号硬编码问题，启动界面显示正确版本
- 修复数据库路径参数未正确传递的问题

### v1.4.0 (2026-01-26)
- 初始版本发布
- 支持装饰器定义任务
- 提供 Web 管理界面
- 支持日志捕获和实时推送

## 功能特性

### 核心功能

- **装饰器定义任务** - 使用 `@task` 装饰器轻松定义定时任务，代码简洁直观
- **多种触发方式** - 支持间隔执行(interval)和Crontab两种触发方式，灵活配置执行计划
- **任务跳过机制** - 如果任务正在运行且未结束，到时间的新任务会自动跳过，避免任务重叠执行
- **SQLite存储** - 使用轻量级SQLite数据库存储任务配置和运行记录，无需额外数据库服务
- **日志持久化** - 所有任务执行日志都会自动保存到数据库，便于追溯和排查问题

### 日志系统

- **自动日志捕获** - 无需手动配置，框架自动捕获任务中通过 `loguru` 输出的日志
- **实时日志推送** - 支持WebSocket实时推送正在运行任务的日志
- **日志级别显示** - 日志按级别(INFO/WARNING/ERROR/DEBUG)分类显示，清晰直观
- **时区转换** - UTC时间自动转换为本地时间(东八区)显示

### Web管理界面

- **响应式设计** - 美观现代的UI界面，完美支持桌面和移动设备
- **数据统计** - 首页展示任务总数、运行中任务数、成功/失败运行次数统计
- **操作控制** - 支持暂停/恢复任务、立即运行任务等操作

## 项目架构

```
task_schedule/
├── task_schedule/
│   ├── __init__.py           # 包初始化
│   ├── __main__.py           # 主入口，FastAPI应用
│   ├── core/
│   │   ├── __init__.py
│   │   ├── database.py       # 数据库模型定义
│   │   ├── task.py           # @task装饰器实现
│   │   └── scheduler.py      # 任务调度器核心
│   ├── web/
│   │   ├── __init__.py
│   │   └── templates/        # HTML模板
│   │       ├── index.html    # 首页
│   │       ├── task_detail.html      # 任务详情页
│   │       ├── run_detail.html       # 运行记录详情页
│   │       └── 404.html      # 404页面
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── helper.py         # 工具函数(run_id生成等)
│   │   └── log_capture.py    # 日志捕获系统
│   └── web/                  # 静态资源
├── examples/
│   └── example.py            # 使用示例
├── setup.py
├── pyproject.toml
└── README.md
```

## 快速开始

### 安装

```bash
# 从源码安装
pip install -e .
```

### 定义任务

创建一个Python文件（例如 `app.py`），使用 `@task` 装饰器定义任务：

```python
from task_schedule import task
from loguru import logger
import time
import random

@task(
    task_id="my_task",
    description="这是一个示例任务，每10秒执行一次",
    trigger="interval",
    interval=10,
)
def my_task():
    """我的任务函数"""
    logger.info("任务开始执行")
    time.sleep(2)
    logger.info(f"随机数: {random.randint(1, 100)}")
    logger.info("任务执行完成")

if __name__ == "__main__":
    from task_schedule.__main__ import run
    run(host="0.0.0.0", port=5000)
```

### 启动框架

```bash
python app.py
```

或在代码中直接指定参数：

```python
if __name__ == "__main__":
    from task_schedule.__main__ import run
    run(host="127.0.0.1", port=8080, db_path="custom.db", debug=True)
```

默认会在 `http://localhost:5000` 启动Web界面。

## 任务定义

### 间隔执行任务

```python
@task(
    task_id="interval_task",
    description="每10秒执行一次的任务",
    trigger="interval",
    interval=10,
)
def interval_task():
    logger.info("任务执行")
    time.sleep(5)
```

参数说明：
- `interval`: 间隔时间，单位为秒

### Crontab任务

```python
@task(
    task_id="crontab_task",
    description="每分钟执行一次的任务",
    trigger="crontab",
    cron="*/1 * * * *",
)
def crontab_task():
    logger.info("任务执行")
```

Crontab表达式格式：`分 时 日 月 周`
- `* * * * *` - 每分钟
- `0 * * * *` - 每小时
- `0 0 * * *` - 每天零点
- `0 0 * * 1` - 每周一
- `*/5 * * * *` - 每5分钟

## Web界面功能详解

### 首页 (/ - 任务列表页)

首页展示所有已注册任务的信息，提供任务状态概览和快速操作入口。

**页面元素：**

1. **统计卡片**
   - 总任务数：当前注册的任务总数
   - 运行中：当前状态为启用的任务数
   - 成功运行：最后运行状态为成功的任务数
   - 失败运行：最后运行状态为失败的任务数

2. **任务列表**
   - 任务ID：点击可进入任务详情页
   - 描述：任务的简要说明
   - 触发类型：显示为"间隔"或"Crontab"标签
   - 配置：间隔任务的间隔秒数，或Crontab表达式
   - 状态：启用/禁用状态标签
   - 最后运行：该任务最近一次的运行时间
   - 运行状态：成功/失败/运行中/跳过状态标签
   - 操作按钮：
     - 详情：进入任务详情页
     - 暂停：暂停任务（启用状态时显示）
     - 恢复：恢复任务（禁用状态时显示）
     - 运行：立即执行任务

3. **刷新按钮**
   - 页面右上角刷新按钮，手动刷新任务列表
   - 右下角悬浮刷新按钮，方便随时刷新
   - 页面30秒自动刷新

### 任务详情页 (/task/{task_id})

点击任务ID或详情按钮进入任务详情页，展示单个任务的详细信息和运行历史。

**页面元素：**

1. **任务头部信息**
   - 任务ID：任务唯一标识
   - 描述：任务详细说明
   - 触发类型：间隔执行/Crontab标签
   - 执行配置：间隔秒数或Crontab表达式
   - 当前状态：启用/禁用状态
   - 函数名：任务对应的Python函数名
   - 创建时间：任务注册到系统的时间
   - 操作按钮：暂停/恢复/立即运行

2. **运行历史表格**
   - 运行ID：点击可进入运行记录详情页
   - 开始时间：任务执行的开始时间（本地时间）
   - 结束时间：任务执行的结束时间
   - 持续时间：任务执行的总时长（自动格式化）
   - 状态：成功/失败/运行中/跳过/等待
   - 操作：查看日志按钮

3. **状态说明**
   - 成功：任务正常执行完成
   - 失败：任务执行过程中发生错误
   - 运行中：任务正在执行中
   - 跳过：因上一次任务未执行完毕而跳过本次执行
   - 等待：任务已加入执行队列，等待调度

### 运行记录详情页 (/run/{run_id})

点击运行ID或查看日志按钮进入运行详情页，查看单次任务执行的详细信息和日志输出。

**页面元素：**

1. **运行信息卡片**
   - 运行ID：本次运行的唯一标识
   - 状态：执行状态标签
   - 开始时间：任务开始执行的时间
   - 结束时间：任务执行完成的时间
   - 错误信息：如果任务失败，显示具体的错误堆栈

2. **日志输出区域**
   - 实时显示任务执行过程中产生的所有日志
   - 日志格式：`2026-01-23 14:38:41.728 | INFO | __main__:example_task:29 - 日志内容`
   - 级别标签颜色区分：
     - INFO：蓝色背景
     - WARNING：黄色背景
     - ERROR：红色背景
     - DEBUG：灰色背景
   - 实时更新：任务运行中时自动轮询获取新日志
   - 日志计数：显示日志总条数
   - 滚动到底部：自动滚动显示最新日志

3. **日志获取机制**
   - 页面加载时获取已有日志
   - 任务运行中时，每2秒轮询获取新日志
   - 任务结束后自动刷新页面获取最终日志
   - 日志按时间顺序排列

### 404页面

当访问不存在的任务或运行记录时显示，提供返回首页的链接。

## API接口

### 任务相关

- `GET /api/tasks` - 获取所有任务列表
- `GET /api/task/{task_id}` - 获取单个任务详情
- `POST /api/task/{task_id}/pause` - 暂停任务
- `POST /api/task/{task_id}/resume` - 恢复任务
- `POST /api/task/{task_id}/run` - 立即运行任务

### 日志相关

- `GET /api/run/{run_id}/logs` - 获取运行日志
- `GET /api/logs/stream/{run_id}` - 日志流式推送

## 命令行参数

```bash
python app.py --host 0.0.0.0 --port 5000 --debug
```

参数说明：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| --host | 0.0.0.0 | 绑定地址，0.0.0.0表示所有网络接口 |
| --port | 5000 | Web服务端口号 |
| --debug | False | 开启调试模式，自动重载 |
| --db-path | None | 数据库文件路径，默认在任务文件同目录 |

## 日志打印

在任务函数中，直接使用 `loguru` 打印日志即可：

```python
from task_schedule import task
from loguru import logger

@task(
    task_id="my_task",
    trigger="interval",
    interval=5,
)
def my_task():
    logger.info("这是一条INFO日志")
    logger.warning("这是一条WARNING日志")
    logger.error("这是一条ERROR日志")
    logger.debug("这是一条DEBUG日志")
```

**注意：**
- 不需要在任务中手动调用 `setup_logger()` 或 `restore_logger()`
- 框架会自动捕获 `loguru` 输出的所有日志
- 系统日志（如"任务开始执行"、"任务执行成功"）不会显示在运行详情页的日志输出中

## 运行状态说明

任务执行有以下几种状态：

| 状态 | 说明 |
|------|------|
| pending | 任务已加入执行队列，等待调度执行 |
| running | 任务正在执行中 |
| success | 任务执行成功完成 |
| failed | 任务执行失败 |
| skipped | 任务被跳过（因上次执行未完成） |

**任务跳过机制：**
- 当任务触发时间到达时，如果上一次执行仍在运行（未结束），则跳过本次执行
- 例如：任务配置为每5秒执行一次，但任务本身执行需要10秒，那么任务会每10秒执行一次，中间的执行会被跳过

## 数据库说明

框架使用SQLite数据库存储以下数据：

- **tasks表**：存储任务配置信息（ID、描述、触发类型、执行间隔等）
- **task_runs表**：存储每次任务运行的记录（运行ID、状态、开始/结束时间等）
- **task_logs表**：存储任务执行过程中产生的日志

数据库文件默认创建在与启动脚本同目录下，名为 `task_schedule.db`。

## 常见问题

### 1. 任务没有执行
- 检查任务状态是否为"启用"
- 检查触发时间是否已到达
- 查看日志是否有报错信息

### 2. 日志没有显示
- 确保使用 `logger.info()` 等方法输出日志，而非 `print()`
- 等待任务执行完成后刷新页面
- 检查日志是否被系统日志覆盖

### 3. 任务执行太频繁
- 检查任务配置是否正确
- 确认没有多个任务实例同时运行

### 4. 端口被占用
- 使用 `--port` 参数指定其他端口
- 或停止占用端口的进程

## 上传到PyPI

```bash
# 安装构建工具
pip install build twine

# 构建包
python -m build

# 上传到PyPI
twine upload dist/*
```

## 许可证

MIT License
