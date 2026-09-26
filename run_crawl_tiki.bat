@echo off
rem Chay giai doan crawl bo sung tieng Viet tu Tiki (electronics/auto/food/home/beauty/mother_baby).
rem Dong cua so nay = dung crawl (du lieu da ghi khong mat, chay lai se tiep tuc).
rem Khong dung Chrome/anti-bot nhu 1688 nen chay doc lap, ai cung chay duoc song song.
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
title Tiki crawler (Vietnamese corpus)
.venv\Scripts\python.exe -u scripts\run_pipeline.py tiki --per-category 2200 --min-delay 1.0 --max-delay 2.0
pause
