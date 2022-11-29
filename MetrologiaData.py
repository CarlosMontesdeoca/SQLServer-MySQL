# CONEXION pyw 
## este escript esta creado para comuicar dos bases de datos que se actualizaran cuando un metrolog envie datos primarios de la tablet al servidor
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
    MySQLConnection = pymysql.connect(host="127.0.0.1",user="root",passwd="",database="test" )
    print (" ==> CONNECCTION SUCCESS WITH MYSQL")
except:
    logs += "==> error to try connect the database MySQL \n" 
    print ('error to try connect the database MySQL')

##============================================================================================================================================
##============================================================================================================================================
##============================================================================================================================================
   
def migrateSimple(codPro):
##  datos primarios del certificados Balxpro    
    cursorsqlsrv.execute(f"SELECT IdeComBpr,est_esc,ClaBpr,DesBpr,identBpr,MarBpr,ModBpr,SerBpr,CapMaxBpr,CapUsoBpr,DivEscBpr,DivEsc_dBpr,RanBpr,IdeComBpr,UbiBpr,BalLimpBpr,AjuBpr,IRVBpr,ObsVBpr,CapCalBpr,RecPorCliBpr,fec_cal FROM Balxpro WHERE IdeBpr LIKE {codPro} AND est_esc LIKE 'PR'")
    data_cert = cursorsqlsrv.fetchall()
    for cert in data_cert:
        if cert[1] == 'DS':
            cursormysql.execute(f"UPDATE certificates SET est='D' WHERE codPro LIKE '{cert[0]}'")
            MySQLConnection.commit()
        else:
            print (f"  ==> UPLOAD DATA FROM CERTIFICATE: {cert[0]}")
##  modifica los datos del certificado con los datos calculados
            try:
                cursormysql.execute(f"UPDATE certificates SET ubi='{cert[14]}',luCal='{cert[14]}',est='RH',evlBal1='{cert[15]}',evlBal2='{cert[16]}',evlBal3='{cert[17]}',obs='{cert[18]}',uso='{cert[19]}',recPor='{cert[20]}',fecCal='{cert[21]}',frmt=11,motr=11 WHERE codPro LIKE '{cert[0]}'")
                MySQLConnection.commit()  
                print ("  ==> SUCCESSFULLY LOADED CERTIFICATE DATA ✅")
            except:
                # logs += "==> ERROR LADING CERTIFICATE DATA \n" 
                print ("  ==> ERROR LADING CERTIFICATE DATA ⚠")
##  Seleccionamos el certificado para cargarle un suplemento con informacion de la balanza
            cursormysql.execute(f"SELECT id FROM certificates WHERE codPro LIKE '{cert[0]}'")
            codCert = cursormysql.fetchone()

## ____________________________________________________  BALANZAS   ________________________________________________________________
            querrySuplement = f"INSERT INTO suplements (certificate_id,cls,descBl,ident,marc,modl,ser,maxCap,usCap,div_e,div_d,rang,est)VALUES({codCert[0]},'{cert[2]}','{cert[3]}','{cert[4]}','{cert[5]}','{cert[6]}','{cert[7]}',{cert[8]},{cert[9]},{round(cert[10],6)},{round(cert[11],6)},{cert[12]},'A')"
            try:
                cursormysql.execute(querrySuplement)
                MySQLConnection.commit() 
                print ("  ==> SUCCESSFULLY LOADED BALANCE DATA ✅")

            except:
                # logs += "==> ERROR LADING BALANCE DATA \n" 
                print ("  ==> ERROR LADING BALANCE DATA ⚠")
###_____________________________________________________AMBIENTALES________________________________________________________________________###
        cursorsqlsrv.execute(f"SELECT * FROM Ambientales WHERE IdeComBpr LIKE '{cert[0]}'")
        envir = cursorsqlsrv.fetchone() 
        try:
            cursormysql.execute(f"INSERT INTO enviroments(certificate_id,codPro,tempIn,tempFn,humIn,humFn)VALUES({codCert[0]},'{cert[0]}',{round(envir[1],2)},{round(envir[2],2)},{round(envir[3],2)},{round(envir[4],2)})")
            MySQLConnection.commit()  
            print (f"  ==> SUCCESSFULLY LOADED ENVIROMENTALS DATA ✅")

        except:
            # logs += "==> ERROR LADING ENVIROMENT DATA \n" 
            print ("  ==> ERROR LADING ENVIROMENT DATA ⚠") 

####### ---------------------------------- INSERTA LOS DATOS DE LAS PRUEBAS DE CALIBRACION --------------------------------------------------#######
        
        if cert[2] == 'III' or cert[2] == 'IIII':
            ## consulta para ver las pruebas de exentricidad
            querryExcCad = f"SELECT * FROM ExecII_Cab WHERE IdeComBpr LIKE '{cert[0]}' ORDER BY PrbEii ASC"
            querryExcDet = f"SELECT * FROM ExecII_Det WHERE CodEii_c LIKE '{cert[0]}%' ORDER BY RIGHT(CodEii_c,1) ASC"

            querryInsertExc = "INSERT INTO excentests(codPro, certificate_id, intCarg, numPr, maxExec, maxErr, pos1, pos1_r, pos2, pos2_r, pos3, evl) VALUES "
            ## consulta para ver las pruebas de repetibilidad
            querryRepet = f"SELECT * FROM RepetIII_Cab C JOIN RepetIII_Det D ON C.IdeComBpr = D.CodRiii_C WHERE C.IdeComBpr LIKE '{cert[0]}'"
        elif cert[2] == 'II':
            ## consulta para ver las pruebas de exentricidad
            querryExcCad = f"SELECT * FROM ExecII_Cab WHERE IdeComBpr LIKE '{cert[0]}' ORDER BY PrbEii ASC"
            querryExcDet = f"SELECT * FROM ExecII_Det WHERE CodEii_c LIKE '{cert[0]}%' ORDER BY RIGHT(CodEii_c,1) ASC"

            querryInsertExc = "INSERT INTO excentests(codPro, certificate_id, intCarg, numPr, maxExec, maxErr, pos1, pos1_r, pos2, pos2_r, pos3, evl) VALUES "
            ## consulta para ver las pruebas de repetibilidad
            querryRepet = f"SELECT * FROM RepetII_Cab C JOIN RepetII_Det D ON C.IdeComBpr = D.CodRii_C WHERE C.IdeComBpr LIKE '{cert[0]}'"
        elif cert[2] == 'Camionera':
            ## consulta para ver las pruebas de exentricidad
            querryExcCad = f"SELECT * FROM ExecCam_Cab WHERE IdeComBpr LIKE '{cert[0]}' ORDER BY PrbCam_c ASC"
            querryExcDet = f"SELECT * FROM ExecCam_Det WHERE CodCam_c LIKE '{cert[0]}%' ORDER BY RIGHT(CodCam_c,1) ASC"

            querryInsertExc = "INSERT INTO excentests(codPro, certificate_id, intCarg, numPr, maxExec, maxErr, pos1, pos1_r, pos2, pos2_r, pos3, pos3_r, evl) VALUES "
            ## consulta para ver las pruebas de repetibilidad
            querryRepet = f"SELECT * FROM RepetIII_Cab C JOIN RepetIII_Det D ON C.IdeComBpr = D.CodRiii_C WHERE C.IdeComBpr LIKE '{cert[0]}'"

## __________________________________________________ EXENTRICIDAD __________________________________________________
        cursorsqlsrv.execute(querryExcCad)
        exectCad = cursorsqlsrv.fetchall()
        cursorsqlsrv.execute(querryExcDet)
        exectDet = cursorsqlsrv.fetchall()

        ## ------ crear el querry para insertar los datos de pruebas de exentricidad
        for pex in [0,1]:
            if cert[2] == 'Camionera':
                querryInsertExc = querryInsertExc + f"('{cert[0]}{exectCad[pex][2]}',{codCert[0]},{exectCad[pex][1]},{exectCad[pex][2]},{round(exectDet[pex][6],5)},{round(exectDet[pex][7],5)},{round(exectDet[pex][1],5)},{round(exectDet[pex][2],5)},{round(exectDet[pex][3],5)},{round(exectDet[pex][4],5)},{round(exectDet[pex][5],5)},{round(exectDet[pex][6],5)},'{exectCad[pex][4]}' ),"
            else :
                querryInsertExc = querryInsertExc + f"('{cert[0]}{exectCad[pex][2]}',{codCert[0]},{exectCad[pex][1]},{exectCad[pex][2]},{round(exectDet[pex][6],5)},{round(exectDet[pex][7],5)},{round(exectDet[pex][1],5)},{round(exectDet[pex][2],5)},{round(exectDet[pex][3],5)},{round(exectDet[pex][4],5)},{round(exectDet[pex][5],5)},'{exectCad[pex][4]}' ),"
        
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
            if cert[2] == 'II':
                querryInsertRept = f"INSERT INTO repeatests(codPro,certificate_id,intCarg,maxDif,maxErr,lec1,lec1_0,lec2,lec2_0,lec3,lec3_0,lec4,lec4_0,lec5,lec5_0,lec6,lec6_0,evl) VALUES ('{cert[0]}',{codCert[0]},{repet[1]},{round(repet[2],5)},{round(repet[3],5)},{round(repet[7],5)},{round(repet[8],5)},{round(repet[9],5)},{round(repet[10],5)},{round(repet[11],5)},{round(repet[12],5)},{round(repet[13],5)},{round(repet[14],5)},{round(repet[15],5)},{round(repet[16],5)},{round(repet[17],5)},{round(repet[18],5)},'{repet[4]}')"
            else: 
                querryInsertRept = f"INSERT INTO repeatests(codPro,certificate_id,intCarg,maxDif,maxErr,lec1,lec1_0,lec2,lec2_0,lec3,lec3_0,evl) VALUES ('{cert[0]}',{codCert[0]},{repet[1]},{round(repet[2],5)},{round(repet[3],5)},{round(repet[7],5)},{round(repet[8],5)},{round(repet[9],5)},{round(repet[10],5)},{round(repet[11],5)},{round(repet[12],5)},'{repet[4]}')"
            
            cursormysql.execute(querryInsertRept)
            MySQLConnection.commit() 
            print ("  ==> SUCCESSFULLY LOADED REPETIBILITY TEST DATA ✅")
        except:
            # logs += "==> ERROR LADING REPEATABILITY TEST DATA \n" 
            print ("  ==> ERROR LADING REPEATABILITY TEST DATA ⚠")

        ## ------ Datos de pruebas de Carga
        cursorsqlsrv.execute(f"SELECT * FROM PCarga_Cab WHERE IdeComBpr LIKE '{cert[0]}' ORDER BY NumPca ASC")
        cargCad = cursorsqlsrv.fetchall()
        cursorsqlsrv.execute(f"SELECT * FROM PCarga_Det WHERE CodPca_C LIKE '{cert[0]}%' ORDER BY RIGHT(CodPca_C,1) ASC")
        cargDet = cursorsqlsrv.fetchall()

        ## ------ Querry para pruebas de Carga
        querryInsertCrg = "INSERT INTO cargtests(codPro, certificate_id, numPr, intCarg, lecAsc, lecDesc, errAsc, errDesc, maxErr, evl) VALUES "
        for pcar in range(0,len(cargCad)):
            querryInsertCrg = querryInsertCrg + f"('{cert[0]}{cargCad[pcar][2]}',{codCert[0]},{cargCad[pcar][2]},{round(cargCad[pcar][1],3)},{round(cargDet[pcar][1],3)},{round(cargDet[pcar][2],3)},{round(cargDet[pcar][3],3)},{round(cargDet[pcar][4],3)},{round(cargDet[pcar][5],3)},'{cargCad[pcar][4]}'),"
        
        ## ------ Insercion de pruebas de Carga
        try:
            cursormysql.execute(querryInsertCrg[:-1])
            MySQLConnection.commit()  
            print ("  ==> SUCCESSFULLY LOADED WEIGTH TEST DATA ✅")
        except:
            print ("  ==> ERROR LADING WEIGTH TEST DATA ⚠")

        ## ------ Datos de pruebas de Pesas
        cursorsqlsrv.execute(f"SELECT ideComBpr,TipPxp,NonCerPxp,SUM(N1) AS N1,SUM(N2) AS N2,SUM(N2A) AS N2A,SUM(N5) AS N5,SUM(N10) AS N10,SUM(N20) AS N20,SUM(N20A) AS N20A,SUM(N50) AS N50,SUM(N100) AS N100,SUM(N200) AS N200,SUM(N200A) AS N200A,SUM(N500) AS N500,SUM(N1000) AS N1000,SUM(N2000) AS N2000,SUM(N2000A) AS N2000A,SUM(N5000) AS N5000,SUM(N10000) AS N10000,SUM(N20000) AS N20000,SUM(N500000) AS N500000,SUM(N1000000) AS N1000000,SUM(CrgPxp1) AS CrgPxp1,SUM(CrgPxp2) AS CrgPxp2,SUM(CrgPxp3) AS CrgPxp3,SUM(CrgPxp4) AS CrgPxp4,SUM(CrgPxp5) AS CrgPxp5,SUM(CrgPxp6) AS CrgPxp6,SUM(AjsPxp) AS AjsPxp FROM Pesxpro WHERE IdeComBpr LIKE '{cert[0]}' GROUP BY IdeComBpr, NonCerPxp, TipPxp")
        pesxpro = cursorsqlsrv.fetchall()  
        cursorsqlsrv.execute(f"SELECT NomCer FROM Cert_Balxpro WHERE IdeComBpr LIKE '{cert[0]}'")
        certItems = cursorsqlsrv.fetchall()

        # print(pesxpro)
        listCert = {}
        for crt in range(0,len(certItems)):
            cursormysql.execute(f"SELECT id FROM cert_items WHERE nom LIKE '{certItems[crt][0]}'")
            idCertItm = cursormysql.fetchone()
            if idCertItm:
                try: 
                    cursormysql.execute(f"INSERT INTO cert_item_certificate(cert_item_id,certificate_id,codUni)VALUES({idCertItm[0]},{codCert[0]},{idCertItm[0]}{codCert[0]})")
                    MySQLConnection.commit()  
                    listCert[certItems[crt][0]] = idCertItm[0]
                except:
                    # logs += "==> ERROR IN FIND CERTITEMS\n" 
                    print('  ==> ERROR IN FIND CERTITEMS ⚠')
        querryInsertPex = "INSERT INTO cargpesxes(codPro, cert_item_id, tip, keyJ, N1, N2, N2A, N5, N10, N20, N20A, N50, N100, N200, N200A, N500, N1000, N2000, N2000A, N5000, N10000, N20000, N500000, N1000000, CrgPxp1, CrgPxp2, CrgPxp3, CrgPxp4, CrgPxp5, CrgPxp6, AjsPxp) VALUES"
        for pexs in range(0,len(pesxpro)):
            aux = pesxpro[pexs][1]
            if aux[0] == 'C':
                aux = aux[1:-1].rjust(3, '0')
            elif aux[0] == 'I':
                aux = '001'
            querryInsertPex = querryInsertPex + f"('{cert[0]}',{listCert[pesxpro[pexs][2]]},'{pesxpro[pexs][1]}','{aux}',{pesxpro[pexs][3]},{pesxpro[pexs][4]},{pesxpro[pexs][5]},{pesxpro[pexs][6]},{pesxpro[pexs][7]},{pesxpro[pexs][8]},{pesxpro[pexs][9]},{pesxpro[pexs][10]},{pesxpro[pexs][11]},{pesxpro[pexs][12]},{pesxpro[pexs][13]},{pesxpro[pexs][14]},{pesxpro[pexs][15]},{pesxpro[pexs][16]},{pesxpro[pexs][17]},{pesxpro[pexs][18]},{pesxpro[pexs][19]},{pesxpro[pexs][20]},{pesxpro[pexs][21]},{pesxpro[pexs][22]},{pesxpro[pexs][23]},{pesxpro[pexs][24]},{pesxpro[pexs][25]},{pesxpro[pexs][26]},{pesxpro[pexs][27]},{pesxpro[pexs][28]},{pesxpro[pexs][29]}),"
        
        try:
            cursormysql.execute(querryInsertPex[:-1])
            MySQLConnection.commit()  
            print ("  ==> SUCCESSFULLY LOADED PESXPRO TEST DATA ✅")
        except:
            # logs += "==> ERROR LADING PESXPRO TEST DATA \n" 
            print ("  ==> ERROR LADING PESXPRO TEST DATA ⚠")
        print("♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠")

##============================================================================================================================================
##============================================================================================================================================
##============================================================================================================================================
    
def migrateWithNews(codPro):
    print("nuevos certificados")

##============================================================================================================================================
##============================================================================================================================================
##============================================================================================================================================

print('=========================================================================')
# CONSULTAS

## CUSORES DE CONSULTAS 
cursorsqlsrv = SQLServerConnection.cursor()
cursormysql = MySQLConnection.cursor()
## ==============================================================

print('SEARCHING PENDING DATA....')
## busca todos los proyectos pendientes de MySQL
cursormysql.execute("SELECT codPro FROM projects WHERE est LIKE 'P'")
data1 = cursormysql.fetchall()
print(data1)

print('=========================================================================')

print('CHECKING.......')
## Buscaremos todos los certificados si se han recivido datos SQLServer 
for codtb in data1:
    codPro = codtb[0]
    # cursorsqlsrv.execute(f"SELECT ClaBpr,DesBpr,identBpr,MarBpr,ModBpr,SerBpr,CapMaxBpr,CapUsoBpr,DivEscBpr,DivEsc_dBpr,RanBpr,IdeComBpr,UbiBpr,BalLimpBpr,AjuBpr,IRVBpr,ObsVBpr,CapCalBpr,RecPorCliBpr,fec_cal,fec_proxBpr FROM Balxpro WHERE IdeBpr LIKE {codPro} AND (est_esc LIKE 'PR' OR est_esc LIKE 'DS')")
    cursorsqlsrv.execute(f"SELECT COUNT(*) FROM Balxpro WHERE IdeBpr LIKE {codPro} AND (est_esc LIKE 'PR' OR est_esc LIKE 'DS')")
    certificates2 = cursorsqlsrv.fetchone()
## si el proyecto de MySql tiene varios certificados de SQLServer en cola  se concidera que ya recbio todos los datos y se puede migrar información    
    if certificates2[0] > 0 :
        cursormysql.execute(f"SELECT COUNT(*) FROM certificates WHERE codPro LIKE '{codPro}%'")
        certificates1 = cursormysql.fetchone()
        print(f"MIGRATING PROJECT: {codPro} ")
        if certificates2[0] == certificates1[0] :
            migrateSimple(codPro)
        else:
            migrateWithNews(codPro)
        print("========================================================================= \n\n")
        # print(f"Proyecto: {codPro} → {len(certificates2)} || {certificates1[0]}")
        # print(certificates)
    # cursormysql.execute(querry)
    # certificate = cursormysql.fetchone()

    # if certificate:  ## SI  HAY DATOS POR LO QUE SE ENVIARAN LOS DATOS PRIMARIOS
    #     print (f"  ==> UPLOAD DATA FROM CERTIFICATE: {codtb[0]}")
    #     cursorsqlsrv.execute(f"SELECT ClaBpr,DesBpr,MarBpr,ModBpr,SerBpr,CapMaxBpr,CapUsoBpr,DivEscBpr,DivEsc_dBpr,RanBpr,IdeComBpr, UbiBpr, BalLimpBpr, AjuBpr, IRVBpr, ObsVBpr, CapCalBpr, RecPorCliBpr, fec_cal, fec_proxBpr, identBpr FROM Balxpro  WHERE ideComBpr LIKE '{codtb[0]}'")
    #     balxpro = cursorsqlsrv.fetchone()  
        
    #     ## modifica los datos del certificado con los datos calculados
    #     try:
    #         # cursormysql.execute(f"UPDATE certificates SET ubi = '{balxpro[11].upper()}', luCal = '{balxpro[11].upper()}', est = 'RH', evlBal1 = '{balxpro[12]}', evlBal2 = '{balxpro[13]}', evlBal3 = '{balxpro[14]}', obs = '{balxpro[15].upper()}', uso = '{balxpro[16]}', recPor = '{balxpro[17].upper()}', fecCal = '{balxpro[18]}', fecProxCal = '{balxpro[19]}', frmt = 12, motr = 11  WHERE codPro LIKE '{codtb[0]}'")
    #         # MySQLConnection.commit()  
    #         print ("  ==> SUCCESSFULLY LOADED CERTIFICATE DATA ✅")
    #     except:
    #         logs += "==> ERROR LADING CERTIFICATE DATA \n" 
    #         print ("  ==> ERROR LADING CERTIFICATE DATA ⚠")

    #     ## modifica la informacion de la balanza.
    #     cursormysql.execute(f"SELECT C.id, S.id FROM certificates C LEFT JOIN suplements S on C.id=S.certificate_id WHERE C.id LIKE {certificate[0]}")
    #     suplement = cursormysql.fetchone()
    #     if suplement[1] == None:
    #         querrySupl = f"INSERT INTO suplements(certificate_id, descBl, ident, marc, modl, ser, maxCap, usCap, div_e, div_d, rang, est) VALUES ({suplement[0]}, '{balxpro[1].upper()}', '{balxpro[20].upper()}', '{balxpro[2].upper()}', '{balxpro[3].upper()}', '{balxpro[4].upper()}', {balxpro[5]}, {balxpro[6]}, {round(balxpro[7],6)}, {round(balxpro[8],6)}, {balxpro[9]}, 'A')"
    #     else:
    #         querrySupl = f"UPDATE suplements SET descBl = '{balxpro[1].upper()}', ident = '{balxpro[20].upper()}', marc = '{balxpro[2].upper()}', modl = '{balxpro[3].upper()}', ser = '{balxpro[4].upper()}', maxCap = {balxpro[5]}, usCap = {balxpro[6]}, div_e = {round(balxpro[7],6)}, div_d = {round(balxpro[8],6)}, rang = {balxpro[9]}, est = 'A' WHERE id LIKE {suplement[1]}"
    #     try:
    #         # cursormysql.execute(querrySupl)
    #         # MySQLConnection.commit() 
    #         print ("  ==> SUCCESSFULLY LOADED BALANCE DATA ✅")

    #     except:
    #         logs += "==> ERROR LADING BALANCE DATA \n" 
    #         print ("  ==> ERROR LADING BALANCE DATA ⚠")
        
    

    #     print(' ======================================================= \n =======================================================')  
    # if logs != '':
    #     file = open(f"./LogMySQL-{today}.txt", "a")
    #     file.write("=======================================================\n")
    #     file.write(f"PROJECT: {codtb[0]}")
    #     file.write(logs)
    #     file.write("=======================================================\n")
    #     file.close()
print("DATA MIGRATION COMPLETED SUCCESSFULLY")

MySQLConnection.close()
SQLServerConnection.close()

