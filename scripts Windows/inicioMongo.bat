@echo off
sc query "MongoDB" | find "RUNNING" >nul || net start "MongoDB"

ping -n 10 127.0.0.1 >nul

pm2 list | find "Cotizador" >nul
if %errorlevel% equ 0 (
    echo El servidor en PM2 está en ejecución.
) else (
    cd C:\Users\user\Documents\Proyectos\server-node
    timeout /t 2 >nul
    npm start
)
