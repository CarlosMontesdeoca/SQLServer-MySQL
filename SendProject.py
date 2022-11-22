# CONEXION pyw
import pyodbc
import pymysql
from datetime import date
import sys
from datetime import datetime

datenow = datetime.now()

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

cursormysql.execute(f"SELECT P.codPro, M.id FROM projects P JOIN metrologists M ON P.metrologist_id=M.id WHERE P.codPro LIKE {codPro}") 
project = cursormysql.fetchone()
# qeurryIdentificadores = f"INSERT INTO Identificadores(codcli, idepro) VALUES (CODCLI, {project[0]})"

## INSERTAR IDENTIFICADORES
cursorsqlsrv.execute(f"IF NOT EXISTS ( SELECT * FROM Identificadores WHERE idepro LIKE {codPro} ) BEGIN INSERT INTO Identificadores (codcli, idepro) VALUES ({CodCli[0]}, {codPro}) END")
SQLServerConnection.commit()

strdate = str(datenow)
dateLong = f"{datenow.strftime('%B %d %Y')} {datenow.time().strftime('%H:%M')}"

cursorsqlsrv.execute(f"SELECT CodMet FROM Metrologos WHERE CodAux LIKE {project[1]}")
CodMet = cursorsqlsrv.fetchone()

## INSERTAR PROYECTOS
cursorsqlsrv.execute(f"IF NOT EXISTS ( SELECT * FROM Proyectos WHERE idepro LIKE {codPro} ) BEGIN INSERT INTO Proyectos (EstPro, FecPro, FecSigCalPro, CodCli, Idepro, CodMet) VALUES ('NU','{strdate[0:10]}','{dateLong}',{CodCli[0]}, {codPro}, {CodMet[0]}) END")
SQLServerConnection.commit()

## INSERTAR BALXPRO
# cursorsqlsrv.execute(f"IF NOT EXISTS ( SELECT * FROM Proyectos WHERE idepro LIKE {codPro} ) BEGIN INSERT INTO Proyectos (EstPro, FecPro, FecSigCalPro, CodCli, Idepro, CodMet) VALUES ('NU','{strdate[0:10]}','{dateLong}',{CodCli[0]}, {codPro}, {CodMet[0]}) END")
# SQLServerConnection.commit()


MySQLConnection.close()
SQLServerConnection.close()