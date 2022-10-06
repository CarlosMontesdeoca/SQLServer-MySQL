# CONEXION pyw
import pyodbc
import pymysql
# import os
# from datetime import date

# #Día actual
# today = date.today()

# print(today)

# file = open(f"./LogMySQL-{today}.txt", "w")

server1='tcp:192.168.9.221'
dbname1='SisMetPrec'
user1='sa'
password1='Sistemas123*'

try:
    SQLServerConnection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL server}; SERVER='+server1+';DATABASE='+dbname1+';UID='+user1+';PWD='+password1)
    print (" ==> CONNECCTION SUCCESS WITH SQL SERVER")
except:
    # file.write("==> error to try connect the database SQL Server")
    print ('error to try connect the database SQL Server')

try:
    MySQLConnection = pymysql.connect(host="127.0.0.1",user="root",passwd="",database="test" )
    print (" ==> CONNECCTION SUCCESS WITH MYSQL")
except:
    print ('error to try connect the database MySQL')

print('=========================================================================')
# CONSULTAS

## CUSORES DE CONSULTAS 
cursorsqlsrv = SQLServerConnection.cursor()
cursormysql = MySQLConnection.cursor()
## ==============================================================

print('SEARCHING PENDING DATA....')
## selecciona los proyectos de que han recibido datos en SQL Server
cursorsqlsrv.execute('''SELECT IdeComBpr FROM Balxpro  WHERE est_esc LIKE 'PR';''')
data1 = cursorsqlsrv.fetchall()
print('=========================================================================')

print('CHECKING.......')
## verifica que los proyectos de SQL server esten pendientes en MySQL para enviar datos 
for codtb in data1:
    querry = f"SELECT * FROM certificates WHERE est LIKE 'P' AND codPro LIKE '{codtb[0]}'"
    cursormysql.execute(querry)
    certificate = cursormysql.fetchone()

    if certificate:  ## SI  HAY DATOS POR LO QUE SE ENVIARAN LOS DATOS PRIMARIOS
        print (f"  ==> UPLOAD DATA FROM CERTIFICATE: {codtb[0]}")
        cursorsqlsrv.execute(f"SELECT ClaBpr,DesBpr,MarBpr,ModBpr,SerBpr,CapMaxBpr,CapUsoBpr,DivEscBpr,DivEsc_dBpr,RanBpr,IdeComBpr, UbiBpr, BalLimpBpr, AjuBpr, IRVBpr, ObsVBpr,CapCalBpr, RecPorCliBpr,fec_cal,fec_proxBpr FROM Balxpro  WHERE ideComBpr LIKE '{codtb[0]}'")
        balxpro = cursorsqlsrv.fetchone()  
        
        ## modifica los datos del certificado con los datos calculados
        try:
            cursormysql.execute(f"UPDATE certificates SET ubi = '{balxpro[11].upper()}', luCal = '{balxpro[11].upper()}', est = 'RH', evlBal1 = '{balxpro[12]}', evlBal2 = '{balxpro[13]}', evlBal3 = '{balxpro[14]}', obs = '{balxpro[15].upper()}', uso = '{balxpro[16]}', recPor = '{balxpro[17].upper()}', fecCal = '{balxpro[18]}', fecProxCal = '{balxpro[19]}', frmt = 12, motr = 11  WHERE codPro LIKE '{codtb[0]}'")
            MySQLConnection.commit()  
            print ("  ==> SUCCESSFULLY LOADED CERTIFICATE DATA ✅")
        except:
            print ("  ==> ERROR LADING CERTIFICATE DATA ⚠")

        ## modifica la informacion de la balanza.
        try:
            cursormysql.execute(f"UPDATE balances SET descBl = '{balxpro[1].upper()}', marc = '{balxpro[2].upper()}', modl = '{balxpro[3].upper()}', ser = '{balxpro[4].upper()}', maxCap = {balxpro[5]}, usCap = {balxpro[6]}, div_e = {balxpro[7]}, div_d = {balxpro[8]}, rang = {balxpro[9]} WHERE id LIKE {certificate[2]}")
            MySQLConnection.commit() 
            print ("  ==> SUCCESSFULLY LOADED BALANCE DATA ✅")

        except:
            print ("  ==> ERROR LADING BALANCE DATA ⚠")
        
        ###_____________________________________________________AMBIENTALES________________________________________________________________________###
        cursorsqlsrv.execute(f"SELECT * FROM Ambientales WHERE IdeComBpr LIKE '{codtb[0]}'")
        envir = cursorsqlsrv.fetchone() 
        
        ## creacion de datos ambientales de la balanza.
        try:
            cursormysql.execute(f"INSERT INTO enviroments(codPro,certificate_id,tempIn,tempFn,humIn,humFn)VALUES('{codtb[0]}',{certificate[0]},{envir[1]},{envir[2]},{envir[3]},{envir[4]})")
            MySQLConnection.commit()  
            print (f"  ==> SUCCESSFULLY LOADED ENVIROMENTALS DATA ✅")

        except:
            print ("  ==> ERROR LADING BALANCE DATA ⚠") 

        ####### -------------- INSERTA LOS DATOS DE LAS PRUEBAS DE CALIBRACION ------------------------------#######
        
        if balxpro[0] == 'III' or balxpro[0] == 'IIII':
            ## consulta para ver las pruebas de exentricidad
            querryExcCad = f"SELECT * FROM ExecII_Cab WHERE IdeComBpr LIKE '{codtb[0]}' ORDER BY PrbEii ASC"
            querryExcDet = f"SELECT * FROM ExecII_Det WHERE CodEii_c LIKE '{codtb[0]}%' ORDER BY RIGHT(CodEii_c,1) ASC"

            querryInsertExc = "INSERT INTO excentests(codPro, certificate_id, intCarg, numPr, maxExec, maxErr, pos1, pos1_r, pos2, pos2_r, pos3, evl) VALUES "
            ## consulta para ver las pruebas de repetibilidad
            querryRepet = f"SELECT * FROM RepetIII_Cab C JOIN RepetIII_Det D ON C.IdeComBpr = D.CodRiii_C WHERE C.IdeComBpr LIKE '{codtb[0]}'"
        elif balxpro[0] == 'II':
            ## consulta para ver las pruebas de exentricidad
            querryExcCad = f"SELECT * FROM ExecII_Cab WHERE IdeComBpr LIKE '{codtb[0]}' ORDER BY PrbEii ASC"
            querryExcDet = f"SELECT * FROM ExecII_Det WHERE CodEii_c LIKE '{codtb[0]}%' ORDER BY RIGHT(CodEii_c,1) ASC"

            querryInsertExc = "INSERT INTO excentests(codPro, certificate_id, intCarg, numPr, maxExec, maxErr, pos1, pos1_r, pos2, pos2_r, pos3, evl) VALUES "
            ## consulta para ver las pruebas de repetibilidad
            querryRepet = f"SELECT * FROM RepetII_Cab C JOIN RepetII_Det D ON C.IdeComBpr = D.CodRii_C WHERE C.IdeComBpr LIKE '{codtb[0]}'"
        elif balxpro[0] == 'Camionera':
            ## consulta para ver las pruebas de exentricidad
            querryExcCad = f"SELECT * FROM ExecCam_Cab WHERE IdeComBpr LIKE '{codtb[0]}' ORDER BY PrbEii ASC"
            querryExcDet = f"SELECT * FROM ExecCam_Det WHERE CodCam_c LIKE '{codtb[0]}%' ORDER BY RIGHT(CodCam_c,1) ASC"

            querryInsertExc = "INSERT INTO excentests(codPro, certificate_id, intCarg, numPr, maxExec, maxErr, pos1, pos1_r, pos2, pos2_r, pos3, pos3_r, evl) VALUES "
            ## consulta para ver las pruebas de repetibilidad
            querryRepet = f"SELECT * FROM RepetIII_Cab C JOIN RepetIII_Det D ON C.IdeComBpr = D.CodRiii_C WHERE C.IdeComBpr LIKE '{codtb[0]}'"

        ## ------ Datos de Pruebas de exentricidad
        cursorsqlsrv.execute(querryExcCad)
        exectCad = cursorsqlsrv.fetchall()
        cursorsqlsrv.execute(querryExcDet)
        exectDet = cursorsqlsrv.fetchall()

        ## ------ crear el querry para insertar los datos de pruebas de exentricidad
        for pex in [0,1]:
            if balxpro[0] == 'Camionera':
                querryInsertExc = querryInsertExc + f"('{codtb[0]}',{certificate[0]},{exectCad[pex][1]},{exectCad[pex][2]},{exectDet[pex][6]},{exectDet[pex][7]},{exectDet[pex][1]},{exectDet[pex][2]},{exectDet[pex][3]},{exectDet[pex][4]},{exectDet[pex][5]},{exectDet[pex][6]},'{exectCad[pex][4]}' ),"
            else :
                querryInsertExc = querryInsertExc + f"('{codtb[0]}',{certificate[0]},{exectCad[pex][1]},{exectCad[pex][2]},{exectDet[pex][6]},{exectDet[pex][7]},{exectDet[pex][1]},{exectDet[pex][2]},{exectDet[pex][3]},{exectDet[pex][4]},{exectDet[pex][5]},'{exectCad[pex][4]}' ),"
        
        ## ------ insercion de pruebas de exentricidad
        try:
            cursormysql.execute(querryInsertExc[:-1])
            MySQLConnection.commit() 
            print ("  ==> SUCCESSFULLY LOADED ECCENTRICITY TEST DATA ✅")
        except:
            print ("  ==> ERROR LADING ECCENTRICITY TEST DATA ⚠")

        ## ------ Datos de Pruebas de repetibilidad
        cursorsqlsrv.execute(querryRepet)
        repet = cursorsqlsrv.fetchone()

        try:
            ## --- creacion de querry para insertar datos de repetbilidad
            if balxpro[0] == 'II':
                querryInsertRept = "INSERT INTO repeatests(codPro,certificate_id,intCarg,maxDif,maxErr,lec1,lec1_0,lec2,lec2_0,lec3,lec3_0,lec4,lec4_0,lec5,lec5_0,lec6,lec6_0,evl) VALUES ('{codtb[0]}',{certificate[0]},{repet[1]},{repet[2]},{repet[3]},{repet[7]},{repet[8]},{repet[9]},{repet[10]},{repet[11]},{repet[12]},{repet[13]},{repet[14]},{repet[15]},{repet[16]},{repet[17]},{repet[18]},'{repet[4]}')"
            else: 
                querryInsertRept = f"INSERT INTO repeatests(codPro,certificate_id,intCarg,maxDif,maxErr,lec1,lec1_0,lec2,lec2_0,lec3,lec3_0,evl) VALUES ('{codtb[0]}',{certificate[0]},{repet[1]},{repet[2]},{repet[3]},{repet[7]},{repet[8]},{repet[9]},{repet[10]},{repet[11]},{repet[12]},'{repet[4]}')"
            cursormysql.execute(querryInsertRept)
            MySQLConnection.commit() 
            print ("  ==> SUCCESSFULLY LOADED REPETIBILITY TEST DATA ✅")
        except:
            print ("  ==> ERROR LADING REPEATABILITY TEST DATA ⚠")

        ## ------ Datos de pruebas de Carga
        cursorsqlsrv.execute(f"SELECT * FROM PCarga_Cab WHERE IdeComBpr LIKE '{codtb[0]}' ORDER BY NumPca ASC")
        cargCad = cursorsqlsrv.fetchall()
        cursorsqlsrv.execute(f"SELECT * FROM PCarga_Det WHERE CodPca_C LIKE '{codtb[0]}%' ORDER BY RIGHT(CodPca_C,1) ASC")
        cargDet = cursorsqlsrv.fetchall()

        ## ------ Querry para pruebas de Carga
        querryInsertCrg = "INSERT INTO cargtests(codPro, certificate_id, numPr, intCarg, lecAsc, lecDesc, errAsc, errDesc, maxErr, evl) VALUES "
        for pcar in range(0,len(cargCad)):
            querryInsertCrg = querryInsertCrg + f"('{codtb[0]}',{certificate[0]},{cargCad[pcar][2]},{cargCad[pcar][1]},{cargDet[pcar][1]},{cargDet[pcar][2]},{cargDet[pcar][3]},{cargDet[pcar][4]},{cargDet[pcar][5]},'{cargCad[pcar][4]}'),"
        
        ## ------ Insercion de pruebas de Carga
        try:
            cursormysql.execute(querryInsertCrg[:-1])
            MySQLConnection.commit()  
            print ("  ==> SUCCESSFULLY LOADED WEIGTH TEST DATA ✅")
        except:
            print ("  ==> ERROR LADING WEIGTH TEST DATA ⚠")

        ## ------ Datos de pruebas de Pesas
        cursorsqlsrv.execute(f"SELECT * FROM Pesxpro WHERE IdeComBpr LIKE '{codtb[0]}'")
        pesxpro = cursorsqlsrv.fetchall()  
        cursorsqlsrv.execute(f"SELECT DISTINCT NonCerPxp FROM Pesxpro WHERE IdeComBpr LIKE '{codtb[0]}'")
        certItems = cursorsqlsrv.fetchall()  
        
        listCert = {}
        for crt in range(0,len(certItems)):
            # print(certItems[crt][0])
            cursormysql.execute(f"SELECT id FROM cert_items WHERE nom LIKE '{certItems[crt][0]}'")
            idCert = cursormysql.fetchone()
            if idCert:
                cursormysql.execute(f"INSERT INTO cert_item_certificate(cert_item_id,certificate_id)VALUES({idCert[0]},{certificate[0]})")
                MySQLConnection.commit()  
                listCert[certItems[crt][0]] = idCert[0]

        querryInsertPex = "INSERT INTO cargpesxes(codPro, cert_item_id, tip, N1, N2, N2A, N5, N10, N20, N20A, N50, N100, N200, N200A, N500, N1000, N2000, N2000A, N5000, N10000, N20000, N500000, N1000000, CrgPxp1, CrgPxp2, CrgPxp3, CrgPxp4, CrgPxp5, CrgPxp6, CrgPxp7, CrgPxp8, CrgPxp9, CrgPxp10, CrgPxp11, CrgPxp12, AjsPxp) VALUES"
        for pexs in range(0,len(pesxpro)):
            querryInsertPex = querryInsertPex + f"('{codtb[0]}',{listCert[pesxpro[pexs][3]]},'{pesxpro[pexs][2]}',{pesxpro[pexs][4]},{pesxpro[pexs][5]},{pesxpro[pexs][6]},{pesxpro[pexs][7]},{pesxpro[pexs][8]},{pesxpro[pexs][9]},{pesxpro[pexs][10]},{pesxpro[pexs][11]},{pesxpro[pexs][12]},{pesxpro[pexs][13]},{pesxpro[pexs][14]},{pesxpro[pexs][15]},{pesxpro[pexs][16]},{pesxpro[pexs][17]},{pesxpro[pexs][18]},{pesxpro[pexs][19]},{pesxpro[pexs][20]},{pesxpro[pexs][21]},{pesxpro[pexs][22]},{pesxpro[pexs][23]},{pesxpro[pexs][24]},{pesxpro[pexs][25]},{pesxpro[pexs][26]},{pesxpro[pexs][27]},{pesxpro[pexs][28]},{pesxpro[pexs][29]},{pesxpro[pexs][30]},{pesxpro[pexs][31]},{pesxpro[pexs][32]},{pesxpro[pexs][33]},{pesxpro[pexs][34]},{pesxpro[pexs][35]},{pesxpro[pexs][36]}),"
        
        try:
            cursormysql.execute(querryInsertPex[:-1])
            MySQLConnection.commit()  
            print ("  ==> SUCCESSFULLY LOADED PESXPRO TEST DATA ✅")
        except:
            print ("  ==> ERROR LADING PESXPRO TEST DATA ⚠")

        print(' ======================================================= \n =======================================================')  

# file.close()

MySQLConnection.close()
SQLServerConnection.close()

