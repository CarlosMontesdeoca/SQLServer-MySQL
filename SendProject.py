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
dbname1='SisMetPrec'
user1='sa'
password1='Sistemas123*'

try:
    SQLServerConnection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL server}; SERVER='+server1+';DATABASE='+dbname1+';UID='+user1+';PWD='+password1)
except:
    print ('error to try connect the database SQL Server')

try:
    MySQLConnection = pymysql.connect(host="127.0.0.1",user="root",passwd="",database="metrologia" )
except:
    print ('error to try connect the database MySQL')

## CUSORES DE CONSULTAS 
cursorsqlsrv = SQLServerConnection.cursor()
cursormysql = MySQLConnection.cursor()

cursormysql.execute(f"SELECT CO.id, C.nom, C.id, S.ciu, S.dir, CO.email, CO.telf, CO.nom, C.ind, S.prov, S.cost FROM contacts CO JOIN plants S ON CO.plant_id = S.id JOIN clients C ON S.client_id = C.id WHERE CO.id LIKE {client}")
clientInfo = cursormysql.fetchone()

# busca el cliente en SisMetPrec y crealo si no existe
querryFindClient = f"IF NOT EXISTS ( SELECT * FROM Clientes WHERE codAux LIKE {client}) BEGIN INSERT INTO Clientes (codAux,  NomCli, CiRucCli, CiuCli, DirCli, EmaCli, TelCli, ConCli, EstCli, matProCli) VALUES ({clientInfo[0]}, '{clientInfo[1]}', '{clientInfo[2]}', '{clientInfo[3]}', '{clientInfo[4]}', '{clientInfo[5]}', '{clientInfo[6]}', '{clientInfo[7]}', 'A', '{clientInfo[8]}' ) END"
try:
    cursorsqlsrv.execute(querryFindClient)
    SQLServerConnection.commit() 
except:
    print('ERROR AL CREAR EL CLIENTE')

#Guarda el codigo del Cliente
cursorsqlsrv.execute(f"SELECT CodCli FROM Clientes WHERE codAux LIKE {client}")
CodCli = cursorsqlsrv.fetchone()


##Obtener la informacion de el Proyecto

cursormysql.execute(f"SELECT P.codPro, M.id FROM projects P JOIN metrologists M ON P.metrologist_id=M.id WHERE P.codPro LIKE {codPro}") 
project = cursormysql.fetchone()
# qeurryIdentificadores = f"INSERT INTO Identificadores(codcli, idepro) VALUES (CODCLI, {project[0]})"

## INSERTAR IDENTIFICADORES
try:
    cursorsqlsrv.execute(f"IF NOT EXISTS ( SELECT * FROM Identificadores WHERE idepro LIKE {codPro} ) BEGIN INSERT INTO Identificadores (codcli, idepro) VALUES ({CodCli[0]}, {codPro}) END")
    SQLServerConnection.commit()
except:
    print("ERROR AL CREAR LOS IDENTIFICADORES")

strdate = str(datenow)
dateLong = f"{datenow.strftime('%B %d %Y')} {datenow.time().strftime('%H:%M')}"

cursorsqlsrv.execute(f"SELECT CodMet FROM Metrologos WHERE CodAux LIKE {project[1]}")
CodMet = cursorsqlsrv.fetchone()

## INSERTAR PROYECTOS
loc = 'GYE/MTA'
if(clientInfo[10] == 'QUITO'):
    loc = 'UIO'

cursorsqlsrv.execute(f"IF NOT EXISTS ( SELECT * FROM Proyectos WHERE idepro LIKE {codPro} ) BEGIN INSERT INTO Proyectos (EstPro, FecPro, FecSigCalPro, CodCli, Idepro, CodMet, LocPro) VALUES ('A','{strdate[0:10]}','{dateLong}',{CodCli[0]}, {codPro}, {CodMet[0]}, '{loc}') END")
SQLServerConnection.commit()

cursorsqlsrv.execute(f"SELECT CodPro FROM Proyectos WHERE Idepro LIKE {codPro}")
IdePro = cursorsqlsrv.fetchone()

## INSERTAR BALXPRO
# busca los certificados del proyecto
cursormysql.execute(f"SELECT B.descBl, B.ident, B.marc, B.modl, B.ser, B.maxCap, B.usCap, B.div_e, B.div_d, B.uni, C.codPro, B.cls FROM certificates C JOIN balances B ON C.balance_id = B.id WHERE C.codPro LIKE '{codPro}%'")
certificates = cursormysql.fetchall()

querryInsertBalxpro = "INSERT INTO Balxpro (NumBpr, DesBpr, IdentBpr, MarBpr, ModBpr, SerBpr, CapMaxBpr, CapUsoBpr, DivEscBpr, UniDivEscBpr,DivEsc_dBpr, UniDivEsc_dBpr, CodPro, CodMet, IdeBpr, EstBpr, LitBpr, IdeComBpr, DivEscCalBpr, CapCalBpr, ClaBpr) VALUES "
for num in range(0,len(certificates)):
    if certificates[num][11] == 'CAM':
        clsBl = "Camionera"
    else:
        clsBl = certificates[num][11]
    querryInsertBalxpro = querryInsertBalxpro + f"({num+1},'{certificates[num][0]}','{certificates[num][1]}','{certificates[num][2]}','{certificates[num][3]}','{certificates[num][4]}',{certificates[num][5]},{certificates[num][6]},{certificates[num][7]},'{certificates[num][9]}',{certificates[num][8]},'{certificates[num][9]}',{IdePro[0]},{CodMet[0]},{codPro},'A','{certificates[num][10][6:]}','{certificates[num][10]}','e','max','{clsBl}'),"

cursorsqlsrv.execute(f"IF NOT EXISTS ( SELECT * FROM Balxpro WHERE IdeBpr LIKE {codPro} ) BEGIN {querryInsertBalxpro[:-1]} END")
SQLServerConnection.commit()


MySQLConnection.close()
SQLServerConnection.close()