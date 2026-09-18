@echo off
rem Chay giai doan chi tiet song ngu trong cua so rieng, doc lap voi VS Code / Claude.
rem Dong cua so nay = dung crawl (du lieu da ghi khong mat, chay lai se tiep tuc).
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
set REQUEST_TIMEOUT_SECONDS=90
set HUMAN_WAIT_SECONDS=900
title 1688 crawler - worker_nhat_anh
.venv\Scripts\python.exe -u scripts\run_pipeline.py detail --min-delay 6 --max-delay 10
pause
