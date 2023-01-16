# CONEXION pyw 
## este escript esta creado para comuicar dos bases de datos que se actualizaran cuando un metrolog envie datos primarios de la tablet al servidor
import pyodbc
import pymysql
from datetime import date

today = date.today()

server1='tcp:192.168.9.221'
dbname1='SisMetPrec'
user1='sa'
password1='Sistemas123*'

logs = ''

try:
    SQLServerConnection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL server}; SERVER='+server1+';DATABASE='+dbname1+';UID='+user1+';PWD='+password1)
    print (" ==> CONNECCTION SUCCESS WITH SQL SERVER")
except:
    logs += "==> error to try connect the database SQL Server \n" 
    print ('error to try connect the database SQL Server')

try:
    MySQLConnection = pymysql.connect(host="127.0.0.1",user="root",passwd="",database="metrologia" )
    print (" ==> CONNECCTION SUCCESS WITH MYSQL")
except:
    logs += "==> error to try connect the database MySQL \n" 
    print ('error to try connect the database MySQL')


print('=========================================================================')
# CONSULTAS

## CUSORES DE CONSULTAS 
cursorsqlsrv = SQLServerConnection.cursor()
cursormysql = MySQLConnection.cursor()
## ==============================================================

print('SEARCHING PENDING DATA....')
## busca todos los proyectos pendientes de MySQL
cursorsqlsrv.execute("SELECT DISTINCT NomCer,IdeComBpr FROM Cert_Balxpro WHERE IdeComBpr LIKE '2212%' ")
data1 = cursorsqlsrv.fetchall()
# print(data1, len(data1))

cursormysql.execute("SELECT DISTINCT nom FROM cert_items")
data2 = cursormysql.fetchall()
# print(data2, len(data1))
i = 0
for cert in data1:
    find = False
    for MyCert in data2:
        if(cert[0] == MyCert[0]):
            find = True
    if find == False:
<<<<<<< HEAD:MY scripts/MetrologiaData.py
        i += 1
        print(f"Certificate: '{cert[0]}' not found, used in {cert[1]}")
print(i)
=======
        print(f"Certificate: '{cert[0]}' not found")
>>>>>>> 2f6116ea257331a7ae015f912a1e5a858b3a78c2:MY_scripts/MetrologiaData.py

MySQLConnection.close()
SQLServerConnection.close()

