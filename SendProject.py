# CONEXION pyw
import pyodbc
import pymysql
from datetime import date
import sys

codPro = sys.argv[1]

today = date.today()

server1='tcp:192.168.9.221'
dbname1='DevSisMetPrec'
user1='sa'
password1='Sistemas123*'

try:
    SQLServerConnection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL server}; SERVER='+server1+';DATABASE='+dbname1+';UID='+user1+';PWD='+password1)
except:
    print ('error to try connect the database SQL Server')

try:
    MySQLConnection = pymysql.connect(host="127.0.0.1",user="root",passwd="",database="test" )
except:
    print ('error to try connect the database MySQL')

## CUSORES DE CONSULTAS 
cursorsqlsrv = SQLServerConnection.cursor()
cursormysql = MySQLConnection.cursor()


## buscar la informacion del proyecto en mysql
cursormysql.execute(f"SELECT P.codPro, S.client_id  FROM projects P JOIN plants S ON P.plant_id=S.id WHERE P.codPro LIKE {codPro}")
project = cursormysql.fetchone()

print(project)


MySQLConnection.close()
SQLServerConnection.close()