@echo off
if "%1"=="h" goto begin
start mshta vbscript:createobject("wscript.shell").run("""%~nx0"" h",0)(window.close)&&exit
:begin
set RUN_ENV=dev
cd D:\haifeng\wind_index\
C:\Users\dell\AppData\Local\Programs\Python\Python39\python.exe index_quote.py
