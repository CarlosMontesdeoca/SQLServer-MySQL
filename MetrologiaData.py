# CONEXION pyw
import pyodbc

server='192.168.9.221'
dbname='SisMetPrec'
user='sa'
password='Sistemas123*'

try:
    connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL server}; SERVER='+server+';DATABASE='+dbname+';UID='+user+';PWD='+password)
    print('connection succes')
except:
    print ('error to try connect the database')

# CONSULTAS

cursor = connection.cursor()
cursor.execute('''SELECT 
    DesBpr as descBl,
    MarBpr as marc,
    ModBpr as modl,
    SerBpr as ser,
    CapMaxBpr as maxCap,
    CapUsoBpr as usCap,
    DivEscBpr as  div_e,
    DivEsc_dBpr as dev_d,
    RanBpr as rang,
    IdeComBpr as codPro, 
    UbiBpr as ubi, 
    BalLimpBpr as evlBal1, 
    AjuBpr as evlBal2, 
    IRVBpr as evlBal3, 
    ObsVBpr as obs,
    CapCalBpr as uso, 
    RecPorCliBpr as recPor,
	fec_cal as fecCal,
	fec_proxBpr as fecProxCal
	FROM Balxpro  WHERE est_esc LIKE 'PR';''')

table = cursor.fetchone()
while table:
    print(table)
    table = cursor.fetchone()

cursor.close()
connection.close()