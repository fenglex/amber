# Amber - 开发框架集

一个集成常用开发框架的工具集，专注于提供高质量的开发框架。

## 项目概述

Amber 是一个专注于框架类工具的集合，目前主要包含一个功能强大的Python任务调度框架。

## 目录结构

```
amber/
├── framework/                 # 框架类工具
│   └── task_schedule/        # Python任务调度框架
├── .gitignore
├── LICENSE
└── README.md
```

## 核心项目

### [task_schedule](file:///D:/Workspace/amber/framework/task_schedule/README.md) - 任务调度框架
一个功能强大的Python定时任务调度框架，支持通过装饰器定义任务，提供现代化的Web界面进行任务管理和监控。

**主要特性：**
- 装饰器定义任务，代码简洁直观
- 支持间隔执行和Crontab两种触发方式
- 任务跳过机制，避免任务重叠执行
- SQLite存储任务配置和运行记录
- 自动日志捕获和实时推送
- 响应式Web管理界面

## 快速开始

### 环境要求
- Python 3.8+

### 安装和使用

1. 克隆项目
```bash
git clone https://github.com/fenglex/amber.git
cd amber
```

2. 安装task_schedule
```bash
cd framework/task_schedule
pip install -e .
```

3. 启动任务调度框架
```bash
cd framework/task_schedule
python -m task_schedule
```

访问 http://localhost:5000 即可使用Web界面管理任务。

## 许可证

此项目遵循MIT许可证 - 详见[LICENSE](file:///D:/Workspace/amber/LICENSE)文件。