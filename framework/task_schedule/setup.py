from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="task_schedule",
    version="1.7.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A powerful Python task scheduling framework with web UI",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/task_schedule",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.8",
    install_requires=[
        "fastapi>=0.104.0",
        "uvicorn>=0.24.0",
        "sqlalchemy>=2.0.0",
        "apscheduler>=3.10.0",
        "loguru>=0.7.0",
        "sse-starlette>=1.6.0",
        "aiofiles>=23.2.0",
        "jinja2>=3.1.0",
        "python-dateutil>=2.8.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "task-schedule=task_schedule.__main__:main",
        ],
    },
    include_package_data=True,
    package_data={
        "task_schedule": [
            "web/templates/*.html",
            "web/static/*",
        ],
    },
)
