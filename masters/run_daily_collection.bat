@echo off
REM Simple Daily NBA Data Collection
REM Run this batch file once per day with Windows Task Scheduler

echo Starting NBA Daily Data Collection...
echo Time: %date% %time%

cd /d "C:\Users\ajwin\Projects\Personal\NBA\thebigone\masters"
"C:/Users/ajwin/Projects/Personal/NBA/thebigone/.venv/Scripts/python.exe" daily_collection.py

echo.
echo Daily collection completed.
echo Time: %date% %time%

REM Optional: Keep window open for 10 seconds to see results
REM timeout /t 10
