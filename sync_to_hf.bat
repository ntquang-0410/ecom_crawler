@echo off
rem Dong bo lien tuc data/raw/ len Hugging Face (moi 20 phut), chay o cua so
rem rieng, doc lap voi run_crawl.bat. Dong cua so nay chi dung dong bo tu dong,
rem khong anh huong crawl dang chay.
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
title Dong bo HF - worker_nhat_anh
.venv\Scripts\python.exe -u scripts\sync_loop.py --minutes 20
pause
