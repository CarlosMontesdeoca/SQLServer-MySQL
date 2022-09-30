# CONEXION pyw
import pyodbc
import pymysql

server1='192.168.9.221'
dbname1='SisMetPrec'
user1='sa'
password1='Sistemas123*'

try:
    SQLServerConnection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL server}; SERVER='+server1+';DATABASE='+dbname1+';UID='+user1+';PWD='+password1)
except:
    print ('error to try connect the database')

try:
    MySQLConnection = pymysql.connect(host="localhost",user="root",passwd="",database="test" )
except:
    print ('error to try connect the database')
cursormysql = MySQLConnection.cursor()

# CONSULTAS

cursorsqlsrv = SQLServerConnection.cursor()
cursorsqlsrv.execute('''SELECT 
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

data = cursorsqlsrv.fetchall()
# print (data)





MySQLConnection.close()
SQLServerConnection.close()