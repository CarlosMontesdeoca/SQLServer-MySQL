# CONEXION pyw
import pyodbc
import pymysql
from datetime import date

today = date.today()

server1='tcp:192.168.9.221'
dbname1='DevSisMetPrec'
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
    MySQLConnection = pymysql.connect(host="localhost",user="root",passwd="",database="metrologia" )
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
## selecciona los proyectos de que han recibido datos en SQL Server
cursormysql.execute("SELECT codPro FROM certificates  WHERE est LIKE 'P' AND codPro IS NOT NULL;")
data1 = cursormysql.fetchall()

print(data1)
codPro ='221001A'
print('=========================================================================')

print('CHECKING.......')
## verifica que los proyectos de SQL server esten pendientes en MySQL para enviar datos 
# for codtb in data1:
querry = f"SELECT UbiBpr, BalLimpBpr, AjuBpr, IRVBpr, CapCalBpr, fec_cal, fec_proxBpr FROM Balxpro WHERE IdeComBpr LIKE '{codPro}'"
cursorsqlsrv.execute(querry)
certificate = cursorsqlsrv.fetchone()

cursormysql.execute(f"SELECT C.id, B.cls FROM certificates C JOIN balances B ON C.balance_id=B.id WHERE C.codPro LIKE '{codPro}'")
codCert = cursormysql.fetchone()
print(codCert)

if certificate:  ## SI  HAY DATOS POR LO QUE SE ENVIARAN LOS DATOS PRIMARIOS
    print (f"  ==> UPLOAD DATA FROM CERTIFICATE: {codPro}")
    try:
        # cursormysql.execute(f"UPDATE certificates SET ubi='{certificate[0]}', luCal='{certificate[0]}',evlBal1 = '{certificate[1]}',evlBal2 = '{certificate[2]}',evlBal3 = '{certificate[3]}', uso = '{certificate[4]}', fecCal = '{certificate[5]}', fecProxCal = '{certificate[6]}', est = 'L' WHERE codPro  LIKE '{codPro}'")
        # MySQLConnection.commit()  
        print ("  ==> SUCCESSFULLY LOADED CERTIFICATE DATA ✅")
    except:
        print ("  ==> ERROR LADING CERTIFICATE DATA ⚠")
        
    ###_____________________________________________________AMBIENTALES________________________________________________________________________###
    cursorsqlsrv.execute(f"SELECT * FROM Ambientales WHERE IdeComBpr LIKE '{codPro}'")
    envir = cursorsqlsrv.fetchone()
    # print(envir) 
        
    ## creacion de datos ambientales de la balanza.
    try:
        # cursormysql.execute(f"INSERT INTO enviroments(codPro,certificate_id,tempIn,tempFn,humIn,humFn)VALUES('{codPro}',{codCert[0]},{round(envir[1],2)},{round(envir[2],2)},{round(envir[3],2)},{round(envir[4],2)})")
        # MySQLConnection.commit()  
        print (f"  ==> SUCCESSFULLY LOADED ENVIROMENTALS DATA ✅")

    except:
        print ("  ==> ERROR LADING ENVIROMENT DATA ⚠") 

    ####### -------------- INSERTA LOS DATOS DE LAS PRUEBAS DE CALIBRACION ------------------------------#######
        
    if codCert[1] == 'III' or codCert[1] == 'IIII':
        ## consulta para ver las pruebas de exentricidad
        print ('tres')
        # querryExcCad = f"SELECT * FROM ExecII_Cab WHERE IdeComBpr LIKE '{codtb[0]}' ORDER BY PrbEii ASC"
        # querryExcDet = f"SELECT * FROM ExecII_Det WHERE CodEii_c LIKE '{codtb[0]}%' ORDER BY RIGHT(CodEii_c,1) ASC"

        # querryInsertExc = "INSERT INTO excentests(codPro, certificate_id, intCarg, numPr, maxExec, maxErr, pos1, pos1_r, pos2, pos2_r, pos3, evl) VALUES "
        # ## consulta para ver las pruebas de repetibilidad
        # querryRepet = f"SELECT * FROM RepetIII_Cab C JOIN RepetIII_Det D ON C.IdeComBpr = D.CodRiii_C WHERE C.IdeComBpr LIKE '{codtb[0]}'"
    elif codCert[1] == 'II':
        ## consulta para ver las pruebas de exentricidad
        querryExcCad = f"SELECT * FROM ExecII_Cab WHERE IdeComBpr LIKE '{codPro}' ORDER BY PrbEii ASC"
        querryExcDet = f"SELECT * FROM ExecII_Det WHERE CodEii_c LIKE '{codPro}%' ORDER BY RIGHT(CodEii_c,1) ASC"
        querryInsertExc = "INSERT INTO excentests(codPro, certificate_id, intCarg, numPr, maxExec, maxErr, pos1, pos1_r, pos2, pos2_r, pos3, evl) VALUES "
        
        ## consulta para ver las pruebas de repetibilidad
        querryRepet = f"SELECT * FROM RepetII_Cab C JOIN RepetII_Det D ON C.IdeComBpr = D.CodRii_C WHERE C.IdeComBpr LIKE '{codPro}'"
    elif codCert[1] == 'Camionera':
        print('cam')
        ## consulta para ver las pruebas de exentricidad
        # querryExcCad = f"SELECT * FROM ExecCam_Cab WHERE IdeComBpr LIKE '{codtb[0]}' ORDER BY PrbCam_c ASC"
        # querryExcDet = f"SELECT * FROM ExecCam_Det WHERE CodCam_c LIKE '{codtb[0]}%' ORDER BY RIGHT(CodCam_c,1) ASC"

        # querryInsertExc = "INSERT INTO excentests(codPro, certificate_id, intCarg, numPr, maxExec, maxErr, pos1, pos1_r, pos2, pos2_r, pos3, pos3_r, evl) VALUES "
        # ## consulta para ver las pruebas de repetibilidad
        # querryRepet = f"SELECT * FROM RepetIII_Cab C JOIN RepetIII_Det D ON C.IdeComBpr = D.CodRiii_C WHERE C.IdeComBpr LIKE '{codtb[0]}'"

    ## ------ Datos de Pruebas de exentricidad
    cursorsqlsrv.execute(querryExcCad)
    exectCad = cursorsqlsrv.fetchall()
    cursorsqlsrv.execute(querryExcDet)
    exectDet = cursorsqlsrv.fetchall()
    # print(exectCad)
    # print(exectDet)

    ## ------ crear el querry para insertar los datos de pruebas de exentricidad
    for pex in [0,1]:
        if codCert[1] == 'Camionera':
            querryInsertExc = querryInsertExc + f"('{codPro}{exectCad[pex][2]}',{codCert[0]},{exectCad[pex][1]},{exectCad[pex][2]},{round(exectDet[pex][6],5)},{round(exectDet[pex][7],5)},{round(exectDet[pex][1],5)},{round(exectDet[pex][2],5)},{round(exectDet[pex][3],5)},{round(exectDet[pex][4],5)},{round(exectDet[pex][5],5)},{round(exectDet[pex][6],5)},'{exectCad[pex][4]}' ),"
        else :
            querryInsertExc = querryInsertExc + f"('{codPro}{exectCad[pex][2]}',{codCert[0]},{exectCad[pex][1]},{exectCad[pex][2]},{round(exectDet[pex][6],5)},{round(exectDet[pex][7],5)},{round(exectDet[pex][1],5)},{round(exectDet[pex][2],5)},{round(exectDet[pex][3],5)},{round(exectDet[pex][4],5)},{round(exectDet[pex][5],5)},'{exectCad[pex][4]}' ),"
    
    ## ------ insercion de pruebas de exentricidad
    try:
        # print(querryInsertExc[:-1])
        # cursormysql.execute(querryInsertExc[:-1])
        # MySQLConnection.commit() 
        print ("  ==> SUCCESSFULLY LOADED ECCENTRICITY TEST DATA ✅")
    except:
        print ("  ==> ERROR LADING ECCENTRICITY TEST DATA ⚠")

    ## ------ Datos de Pruebas de repetibilidad
    cursorsqlsrv.execute(querryRepet)
    repet = cursorsqlsrv.fetchone()

    try:
        ## --- creacion de querry para insertar datos de repetbilidad
        if codCert[1] == 'II':
            querryInsertRept = f"INSERT INTO repeatests(codPro,certificate_id,intCarg,maxDif,maxErr,lec1,lec1_0,lec2,lec2_0,lec3,lec3_0,lec4,lec4_0,lec5,lec5_0,lec6,lec6_0,evl) VALUES ('{codPro}',{codCert[0]},{repet[1]},{round(repet[2],5)},{round(repet[3],5)},{round(repet[7],5)},{round(repet[8],5)},{round(repet[9],5)},{round(repet[10],5)},{round(repet[11],5)},{round(repet[12],5)},{round(repet[13],5)},{round(repet[14],5)},{round(repet[15],5)},{round(repet[16],5)},{round(repet[17],5)},{round(repet[18],5)},'{repet[4]}')"
        else: 
            querryInsertRept = f"INSERT INTO repeatests(codPro,certificate_id,intCarg,maxDif,maxErr,lec1,lec1_0,lec2,lec2_0,lec3,lec3_0,evl) VALUES ('{codPro}',{codCert[0]},{repet[1]},{round(repet[2],5)},{round(repet[3],5)},{round(repet[7],5)},{round(repet[8],5)},{round(repet[9],5)},{round(repet[10],5)},{round(repet[11],5)},{round(repet[12],5)},'{repet[4]}')"
        
        # print(querryInsertRept)
        # cursormysql.execute(querryInsertRept)
        # MySQLConnection.commit() 
        print ("  ==> SUCCESSFULLY LOADED REPETIBILITY TEST DATA ✅")
    except:
        logs += "==> ERROR LADING REPEATABILITY TEST DATA \n" 
        print ("  ==> ERROR LADING REPEATABILITY TEST DATA ⚠")

    ## ------ Datos de pruebas de Carga
    cursorsqlsrv.execute(f"SELECT * FROM PCarga_Cab WHERE IdeComBpr LIKE '{codPro}' ORDER BY NumPca ASC")
    cargCad = cursorsqlsrv.fetchall()
    cursorsqlsrv.execute(f"SELECT * FROM PCarga_Det WHERE CodPca_C LIKE '{codPro}%' ORDER BY RIGHT(CodPca_C,1) ASC")
    cargDet = cursorsqlsrv.fetchall()

    ## ------ Querry para pruebas de Carga
    querryInsertCrg = "INSERT INTO cargtests(codPro, certificate_id, numPr, intCarg, lecAsc, lecDesc, errAsc, errDesc, maxErr, evl) VALUES "
    for pcar in range(0,len(cargCad)):
        querryInsertCrg = querryInsertCrg + f"('{codPro}{cargCad[pcar][2]}',{codCert[0]},{cargCad[pcar][2]},{round(cargCad[pcar][1],3)},{round(cargDet[pcar][1],3)},{round(cargDet[pcar][2],3)},{round(cargDet[pcar][3],3)},{round(cargDet[pcar][4],3)},{round(cargDet[pcar][5],3)},'{cargCad[pcar][4]}'),"
    
    ## ------ Insercion de pruebas de Carga
    try:
        # cursormysql.execute(querryInsertCrg[:-1])
        # MySQLConnection.commit()  
        print ("  ==> SUCCESSFULLY LOADED WEIGTH TEST DATA ✅")
    except:
        logs += "==> ERROR LADING WEIGTH TEST DATA \n" 
        print ("  ==> ERROR LADING WEIGTH TEST DATA ⚠")

    ## ------ Datos de pruebas de Pesas
    cursorsqlsrv.execute(f"SELECT ideComBpr,TipPxp,NonCerPxp,SUM(N1) AS N1,SUM(N2) AS N2,SUM(N2A) AS N2A,SUM(N5) AS N5,SUM(N10) AS N10,SUM(N20) AS N20,SUM(N20A) AS N20A,SUM(N50) AS N50,SUM(N100) AS N100,SUM(N200) AS N200,SUM(N200A) AS N200A,SUM(N500) AS N500,SUM(N1000) AS N1000,SUM(N2000) AS N2000,SUM(N2000A) AS N2000A,SUM(N5000) AS N5000,SUM(N10000) AS N10000,SUM(N20000) AS N20000,SUM(N500000) AS N500000,SUM(N1000000) AS N1000000,SUM(CrgPxp1) AS CrgPxp1,SUM(CrgPxp2) AS CrgPxp2,SUM(CrgPxp3) AS CrgPxp3,SUM(CrgPxp4) AS CrgPxp4,SUM(CrgPxp5) AS CrgPxp5,SUM(CrgPxp6) AS CrgPxp6,SUM(AjsPxp) AS AjsPxp FROM Pesxpro WHERE IdeComBpr LIKE '{codPro}' GROUP BY IdeComBpr, NonCerPxp, TipPxp")
    pesxpro = cursorsqlsrv.fetchall()  
    cursorsqlsrv.execute(f"SELECT NomCer FROM Cert_Balxpro WHERE IdeComBpr LIKE '{codPro}'")
    certItems = cursorsqlsrv.fetchall()



    # print(pesxpro)
    listCert = {}
    for crt in range(0,len(certItems)):
        cursormysql.execute(f"SELECT id FROM cert_items WHERE nom LIKE '{certItems[crt][0]}'")
        idCert = cursormysql.fetchone()
        if idCert:
            try: 
                cursormysql.execute(f"INSERT INTO cert_item_certificate(cert_item_id,certificate_id)VALUES({idCert[0]},{certificate[0]})")
                MySQLConnection.commit()  
                listCert[certItems[crt][0]] = idCert[0]
            except:
                print(f"  ==> ERROR IN FIND CERTITEMS {certItems[crt][0]}⚠")
    querryInsertPex = "INSERT INTO cargpesxes(codPro, cert_item_id, tip, keyJ, N1, N2, N2A, N5, N10, N20, N20A, N50, N100, N200, N200A, N500, N1000, N2000, N2000A, N5000, N10000, N20000, N500000, N1000000, CrgPxp1, CrgPxp2, CrgPxp3, CrgPxp4, CrgPxp5, CrgPxp6, AjsPxp) VALUES"
    for pexs in range(0,len(pesxpro)):
        aux = pesxpro[pexs][1]
        if aux[0] == 'C':
            aux = aux[1:-1].rjust(3, '0')
        elif aux[0] == 'I':
            aux = '001'
        querryInsertPex = querryInsertPex + f"('{codPro}',{listCert[pesxpro[pexs][2]]},'{pesxpro[pexs][1]}','{aux}',{pesxpro[pexs][3]},{pesxpro[pexs][4]},{pesxpro[pexs][5]},{pesxpro[pexs][6]},{pesxpro[pexs][7]},{pesxpro[pexs][8]},{pesxpro[pexs][9]},{pesxpro[pexs][10]},{pesxpro[pexs][11]},{pesxpro[pexs][12]},{pesxpro[pexs][13]},{pesxpro[pexs][14]},{pesxpro[pexs][15]},{pesxpro[pexs][16]},{pesxpro[pexs][17]},{pesxpro[pexs][18]},{pesxpro[pexs][19]},{pesxpro[pexs][20]},{pesxpro[pexs][21]},{pesxpro[pexs][22]},{pesxpro[pexs][23]},{pesxpro[pexs][24]},{pesxpro[pexs][25]},{pesxpro[pexs][26]},{pesxpro[pexs][27]},{pesxpro[pexs][28]},{pesxpro[pexs][29]}),"
    
    try:
        cursormysql.execute(querryInsertPex[:-1])
        MySQLConnection.commit()  
        print ("  ==> SUCCESSFULLY LOADED PESXPRO TEST DATA ✅")
    except:
        logs += "==> ERROR LADING PESXPRO TEST DATA \n" 
        print ("  ==> ERROR LADING PESXPRO TEST DATA ⚠")

    print(' ======================================================= \n =======================================================')  


MySQLConnection.close()
SQLServerConnection.close()

