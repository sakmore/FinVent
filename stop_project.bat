@echo off

echo Stopping FinVent...

taskkill /F /IM java.exe

taskkill /F /IM python.exe

echo.
echo FinVent stopped.

pause