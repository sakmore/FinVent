@echo off

echo ==========================================
echo         Starting FinVent
echo ==========================================

REM -----------------------------------------
REM Start Kafka
REM -----------------------------------------

start "Kafka Server" cmd /k "cd /d C:\kafka\kafka_2.13-4.3.1 && set KAFKA_HEAP_OPTS=-Xmx512M -Xms256M && .\bin\windows\kafka-server-start.bat .\config\server.properties"

echo Waiting for Kafka...
timeout /t 10 > nul

REM -----------------------------------------
REM Storage Consumer
REM -----------------------------------------

start "Storage Consumer" cmd /k "cd /d D:\Projects\Finance_Project\FinVent && python -m consumers.storage_consumer"

REM -----------------------------------------
REM Fraud Consumer
REM -----------------------------------------

start "Fraud Consumer" cmd /k "cd /d D:\Projects\Finance_Project\FinVent && python -m consumers.fraud_consumer"

REM -----------------------------------------
REM Alert Consumer
REM -----------------------------------------

start "Alert Consumer" cmd /k "cd /d D:\Projects\Finance_Project\FinVent && python -m consumers.alert_consumer"

REM -----------------------------------------
REM Dashboard
REM -----------------------------------------

start "Dashboard" cmd /k "cd /d D:\Projects\Finance_Project\FinVent && streamlit run dashboard/app.py"
echo.
echo ==========================================
echo FinVent Started Successfully!
echo ==========================================
echo.
echo Only start the Producer separately.
pause