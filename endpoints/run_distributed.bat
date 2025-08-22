@echo off
REM NBA Endpoint Processing - Simple Distribution
REM Each endpoint runs on a different server/IP

echo ===============================================
echo NBA ENDPOINT PROCESSING - SIMPLE DISTRIBUTION
echo ===============================================
echo.

if "%1"=="dry-run" (
    echo Running dry-run for BoxScoreAdvancedV3...
    python collectors\endpoint_processor.py --endpoint BoxScoreAdvancedV3 --dry-run --node-id test
) else if "%1"=="list" (
    echo Listing all available endpoints...
    python collectors\endpoint_processor.py
) else if "%1"=="" (
    echo Usage:
    echo   %0 dry-run              - Test with BoxScoreAdvancedV3
    echo   %0 list                 - Show all available endpoints
    echo   %0 [endpoint_name]      - Run specific endpoint
    echo.
    echo Examples:
    echo   %0 BoxScoreAdvancedV3
    echo   %0 PlayerGameLogs  
    echo   %0 TeamGameLogs
    echo.
    echo For distributed processing, run on different servers:
    echo   Server 1: %0 BoxScoreAdvancedV3
    echo   Server 2: %0 PlayerGameLogs
    echo   Server 3: %0 TeamGameLogs
) else (
    echo Running endpoint: %1
    python collectors\endpoint_processor.py --endpoint %1 --node-id %COMPUTERNAME%_%1
)

echo.
pause
