# CONEXION pyw
import pyodbc
import pymysql
from datetime import date
import sys

codPro = sys.argv[1]
client = sys.argv[2]

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

cursormysql.execute(f"SELECT CO.id, C.nom, C.id, S.ciu, S.dir, CO.email, CO.telf, CO.nom, C.ind, S.prov FROM contacts CO JOIN plants S ON CO.plant_id = S.id JOIN clients C ON S.client_id = C.id WHERE CO.id LIKE {client}")
clientInfo = cursormysql.fetchone()

# busca el cliente en SisMetPrec y crealo si no existe
querryFindClient = f"IF NOT EXISTS ( SELECT * FROM Clientes WHERE codAux LIKE {client}) BEGIN INSERT INTO Clientes (codAux,  NomCli, CiRucCli, CiuCli, DirCli, EmaCli, TelCli, ConCli, EstCli, matProCli) VALUES ({clientInfo[0]}, '{clientInfo[1]}', '{clientInfo[2]}', '{clientInfo[3]}', '{clientInfo[4]}', '{clientInfo[5]}', '{clientInfo[6]}', '{clientInfo[7]}', 'A', '{clientInfo[8]}' ) END"
cursorsqlsrv.execute(querryFindClient)
SQLServerConnection.commit() 

#Guarda el codigo del Cliente
cursorsqlsrv.execute(f"SELECT CodCli FROM Clientes WHERE codAux LIKE {client}")
CodCli = cursorsqlsrv.fetchone()


##Obtener la informacion de el Proyecto

cursormysql.execute(f"SELECT * FROM projects WHERE codPro LIKE {codPro}") 
project = cursormysql.fetchone()
print(project)
# qeurryIdentificadores = f"INSERT INTO Identificadores(codcli, idepro) VALUES (CODCLI, {project[0]})"



## busca el cliente en SisMetPrec
# SELECT P.contact_id, C.nom, C.id, S.ciu, S.dir, C.ind, S.prov FROM projects P JOIN plants S ON P.plant_id = S.id JOIN clients C ON S.client_id = C.id JOIN contacts CO ON P.contact_id = CO.id WHERE P.codPro LIKE 221101
# cursorsqlsrv.execute(f"SELECT * FROM Clientes WHERE CiRucCli LIKE {project[1]}")
# client = cursorsqlsrv.fetchone()

##crea el registro en Identificadores 
# qeurryIdentificadores = f"INSERT INTO Identificadores(codcli, idepro) VALUES (CODCLI, {project[0]})"


MySQLConnection.close()
SQLServerConnection.close()