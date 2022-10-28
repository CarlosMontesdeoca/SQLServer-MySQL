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


## buscar la informacion del proyecto en mysql informacion base 
cursormysql.execute(f"SELECT P.codPro, S.client_id, M.nom FROM projects P JOIN plants S ON P.plant_id=S.id JOIN metrologists M ON P.metrologist_id=M.id WHERE P.codPro LIKE {codPro}")
project = cursormysql.fetchone()
# print(project)

## busca el cliente en SisMetPrec
# cursorsqlsrv.execute(f"SELECT * FROM Clientes WHERE CiRucCli LIKE {project[1]}")
# client = cursorsqlsrv.fetchone()
print(f"SELECT * FROM Clientes WHERE CiRucCli LIKE {project[1]}")

##crea el registro en Identificadores 
qeurryIdentificadores = f"INSERT INTO Identificadores(codcli, idepro) VALUES (CODCLI, {project[0]})"


MySQLConnection.close()
SQLServerConnection.close()