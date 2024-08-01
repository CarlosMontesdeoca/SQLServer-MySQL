
For /f "tokens=2-4 delims=/ " %%a in ('date /t') do (set mydate=%%c-%%a-%%b)
vboxmanage export "pruebas-lx" -o C:\archivos_sistemas\Respaldos_KAL\KALK_%mydate%.ova
