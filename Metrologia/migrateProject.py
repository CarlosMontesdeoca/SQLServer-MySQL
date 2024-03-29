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
    MySQLConnection = pymysql.connect(host="127.0.0.1",user="root",passwd="AdminSistemas@",database="metrologia" )
    print (" ==> CONNECCTION SUCCESS WITH MYSQL")
except:
    logs += "==> error to try connect the database MySQL \n" 
    print ('error to try connect the database MySQL')


print('=========================================================================')
# CONSULTAS

## CUSORES DE CONSULTAS 
cursorsqlsrv = SQLServerConnection.cursor()
cursormysql = MySQLConnection.cursor()
## ===================================================6===========

print('SEARCHING PENDING DATA....')
## busca todos los proyectos pendientes de MySQL
cursormysql.execute("SELECT DISTINCT P.codPro FROM projects P JOIN certificates C on P.id=C.project_id WHERE P.est LIKE 'P' AND C.est LIKE 'P' AND P.tip LIKE 'ICC'")
data1 = cursormysql.fetchall()
print(data1, len(data1))

print('=========================================================================')

print('CHECKING.......')
# Buscaremos todos los certificados si se han recivido datos SQLServer 
for codtb in data1:
    idPro = codtb[0]
    # cursorsqlsrv.execute(f"SELECT ClaBpr,DesBpr,identBpr,MarBpr,ModBpr,SerBpr,CapMaxBpr,CapUsoBpr,DivEscBpr,DivEsc_dBpr,RanBpr,IdeComBpr,UbiBpr,BalLimpBpr,AjuBpr,IRVBpr,ObsVBpr,CapCalBpr,RecPorCliBpr,fec_cal,fec_proxBpr FROM Balxpro WHERE IdeBpr LIKE {codPro} AND (est_esc LIKE 'PR' OR est_esc LIKE 'DS')")
    # print(f"SELECT IdeComBpr FROM Balxpro WHERE IdeBpr LIKE {idPro} AND (est_esc LIKE 'PR' OR est_esc LIKE 'PL' OR est_esc LIKE 'DS')")
    cursorsqlsrv.execute(f"SELECT IdeComBpr FROM Balxpro WHERE IdeBpr LIKE {idPro} AND (est_esc LIKE 'PR' OR est_esc LIKE 'PL' OR est_esc LIKE 'DS')")
    certificates2 = cursorsqlsrv.fetchall()
## si el proyecto de MySql tiene varios certificados de SQLServer en cola  se concidera que ya recbio todos los datos y se puede migrar información    
    if len(certificates2) > 0 :
        cursormysql.execute(f"SELECT codPro, project_id FROM certificates WHERE codPro LIKE '{idPro}%'")
        certificates1 = cursormysql.fetchall()

        print(f"MIGRATING PROJECT: {idPro} |||| SQL:({len(certificates2)}) MySQL:({len(certificates1)}) \n")
        listCodPro = certificates2
        idPro = certificates1[0][1]
        for codPro in listCodPro:
            codPro = codPro[0]
            cursorsqlsrv.execute(f"SELECT IdeComBpr,est_esc,ClaBpr,DesBpr,identBpr,MarBpr,ModBpr,SerBpr,CapMaxBpr,CapUsoBpr,DivEscBpr,DivEsc_dBpr,RanBpr,UbiBpr,BalLimpBpr,AjuBpr,IRVBpr,ObsVBpr,CapCalBpr,RecPorCliBpr,fec_cal,FechaRecepcion,UniDivEscBpr,lugcalBpr,RecPorCliBpr FROM Balxpro WHERE IdeComBpr LIKE '{codPro}' AND (est_esc LIKE 'PR' OR est_esc LIKE 'PL' OR est_esc LIKE 'DS') ORDER BY IdeComBpr ASC")
            data_cert = cursorsqlsrv.fetchone()
            e = str(float(round(data_cert[10],8))).split('.')[1]
            if data_cert[17] == None:
                data_cert[17] = 'NULL'
            else : 
                data_cert[17] = f"'{data_cert[17]}'"
            if data_cert[2] == 'Camionera':
                data_cert[2] = 'CAM'
            rd = len(e)
            if (e == '0' or e == 0):
                rd = 0


        ## ___________COMPARA SI EXISTE EL CERTIFICADO
            cursormysql.execute(f"SELECT * FROM certificates WHERE codPro LIKE '{codPro}'")
            myCert = cursormysql.fetchone()
            print( f"\n=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|===== {codPro} =====|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|=|")
            if(myCert == None):
                print("   ==> CREATING NEW CERTIFICATE..")
                cursormysql.execute(f"SELECT id FROM balances WHERE ser LIKE '{data_cert[7]}' AND marc like '{data_cert[5]}'")
                myBals = cursormysql.fetchone()
                if(myBals == None):
                    print(f"     ==> CREATING NEW BALANCE..")
        # __________________create balance________________________
                    clsBl = (data_cert[2])
                    cursormysql.execute(f"SELECT plant_id FROM projects WHERE codPro LIKE {data_cert[0][0:-1]}")
                    idPlant = cursormysql.fetchone()
                    if (clsBl == 'Camionera'):
                        clsBl = 'CAM'
                    try: 
                        cursormysql.execute(f"INSERT INTO balances (tip,marc,modl,ser,cls,maxCap,usCap,div_e,div_d,uni,plant_id) VALUES ('{data_cert[3]}','{data_cert[5]}','{data_cert[6]}','{data_cert[7]}','{clsBl}',{data_cert[8]},{data_cert[9]},{round(data_cert[10],6)},{round(data_cert[11],6)},'kg',{idPlant[0]})")
                        MySQLConnection.commit()
                        print(f"        ==> SUCCESSFULLY TO CREATED NEW BALANCE FOR CERTIFICATE {codPro} ✅")
                    except:
                        logs += f"==> ERROR TO CREATED NEW BALANCE FOR CERTIFICATE {codPro} ⚠ \n" 
                        print('        ==> ERROR TO CREATED NEW BALANCE ⚠')
                    cursormysql.execute(f"SELECT id FROM balances WHERE ser LIKE '{data_cert[7]}' AND marc like '{data_cert[5]}'")
                    myBals = cursormysql.fetchone()
                print(f"     ==> CREATING NEW CERTIFICATE {codPro}..")
                try:
                    cursormysql.execute(f"INSERT INTO certificates (codPro,balance_id,project_id) VALUES ('{codPro}',{myBals[0]},{idPro})")
                    MySQLConnection.commit()
                    print(f"        ==> SUCCESSFULLY CREATED NEW CERTIFICATE {codPro} ✅")
                except:
                    logs += f"==> ERROR CREATED NEW CERTIFICATE DATA {codPro} \n" 
                    print ("        ==> ERROR CREATED NEW CERTIFICATE DATA ⚠")

    # ___________________________ SUBIDA DE DATOS PARA CADA CERTIFICADO INCLUIDO LOS NUEVOS ________________________________

            isDiscard = True
            if (data_cert[1] == 'DS'):
                isDiscard = False
                querryCertificate = f"UPDATE certificates SET est='D' WHERE codPro LIKE '{data_cert[0]}'"
            else :
                querryCertificate = f"UPDATE certificates SET ubi='{data_cert[13]}',luCal='{data_cert[23]}',est='RH',evlBal1='{data_cert[14]}',evlBal2='{data_cert[15]}',evlBal3='{data_cert[16]}',obs={data_cert[17]},uso='{data_cert[18]}',recPor='{data_cert[19]}',fecCal='{data_cert[20]}',fecRegDt='{data_cert[21]}',frmt=11,motr=11 WHERE codPro LIKE '{data_cert[0]}'"
            print(f"   ==> UPDATING CERTIFICATE DATA: {data_cert[0]}..")
            try:
                try: 
                    cursormysql.execute(f"UPDATE projects SET recPor = '{data_cert[24]}' WHERE codPro LIKE '{data_cert[0][0:-1]}'")
                except: 
                    logs += f"==> ERROR TO WRITE RECIVIER ⚠\n" 
                    print ("      ==> ERROR TO WRITE RECIVIER ⚠")
                cursormysql.execute(querryCertificate)
                MySQLConnection.commit()  
                print (f"      ==> SUCCESSFULLY LOADED CERTIFICATE DATA || EST: {data_cert[1]} ✅")
            except:
                print(querryCertificate)
                logs += f"==> ERROR LADING CERTIFICATE DATA ⚠\n" 
                print ("      ==> ERROR LADING CERTIFICATE DATA ⚠")

    ##  ______________________________ SELECCIONE Y SUBE LOS SUPLEMENTOS DE BALAZAS _______________________________________

            if (isDiscard):
                print(f"   ==> UPLOADING DATA TESTS FOR CERTIFICATE: {data_cert[0]}..")
                cursormysql.execute(f"SELECT id FROM certificates WHERE codPro LIKE '{data_cert[0]}'")
                codCert = cursormysql.fetchone()

    ## __________________________________________________  BALANZAS   ________________________________________________________________
                
                print("     ==> UPLOADING DATA SUPLEMENT..")
                try:
                    uniSpl = 'g'
                    if data_cert[22] == 'k':
                        uniSpl = 'kg'
                    querrySuplement = f"INSERT INTO suplements (certificate_id,cls,descBl,ident,marc,modl,ser,maxCap,usCap,div_e,div_d,rang,uni,est)VALUES({codCert[0]},'{data_cert[2]}','{data_cert[3]}','{data_cert[4]}','{data_cert[5]}','{data_cert[6]}','{data_cert[7]}',{data_cert[8]},{data_cert[9]},{round(data_cert[10],6)},{round(data_cert[11],6)},{data_cert[12]},'{uniSpl}','A')"
                    cursormysql.execute(querrySuplement)
                    MySQLConnection.commit() 
                    print ("        ==> SUCCESSFULLY LOADED BALANCE DATA ✅")

                except:
                    logs += f"==> ERROR LADING BALANCE DATA || {data_cert[0]}\n" 
                    print ("        ==> ERROR LADING BALANCE DATA ⚠")

    ## _____________________________________________________AMBIENTALES__________________________________________________________________

                cursorsqlsrv.execute(f"SELECT * FROM Ambientales WHERE IdeComBpr LIKE '{data_cert[0]}'")
                envir = cursorsqlsrv.fetchone() 
                print("     ==> UPLOADING DATA ENVIRONMENT..")
                try:
                    cursormysql.execute(f"INSERT INTO enviroments(certificate_id,codPro,tempIn,tempFn,humIn,humFn)VALUES({codCert[0]},'{data_cert[0]}',{round(envir[1],2)},{round(envir[2],2)},{round(envir[3],2)},{round(envir[4],2)})")
                    MySQLConnection.commit()  
                    print (f"        ==> SUCCESSFULLY LOADED ENVIROMENTALS DATA ✅")

                except:
                    logs += f"==> ERROR LADING ENVIROMENT DATA ⚠ || {data_cert[0]}\n" 
                    print ("        ==> ERROR LADING ENVIROMENT DATA ⚠") 

    ####### ---------------------------------- INSERTA LOS DATOS DE LAS PRUEBAS DE CALIBRACION --------------------------------------------------#######
                
                if data_cert[2] == 'III' or data_cert[2] == 'IIII':
                    ## consulta para ver las pruebas de exentricidad
                    querryExcCad = f"SELECT * FROM ExecII_Cab WHERE IdeComBpr LIKE '{data_cert[0]}' ORDER BY PrbEii ASC"
                    querryExcDet = f"SELECT * FROM ExecII_Det WHERE CodEii_c LIKE '{data_cert[0]}%' ORDER BY RIGHT(CodEii_c,1) ASC"

                    querryInsertExc = "INSERT INTO excentests(codPro, certificate_id, intCarg, numPr, maxExec, maxErr, pos1, pos1_r, pos2, pos2_r, pos3, evl) VALUES "
                    ## consulta para ver las pruebas de repetibilidad
                    querryRepet = f"SELECT * FROM RepetIII_Cab C JOIN RepetIII_Det D ON C.IdeComBpr = D.CodRiii_C WHERE C.IdeComBpr LIKE '{data_cert[0]}'"
                elif data_cert[2] == 'II':
                    ## consulta para ver las pruebas de exentricidad
                    querryExcCad = f"SELECT * FROM ExecII_Cab WHERE IdeComBpr LIKE '{data_cert[0]}' ORDER BY PrbEii ASC"
                    querryExcDet = f"SELECT * FROM ExecII_Det WHERE CodEii_c LIKE '{data_cert[0]}%' ORDER BY RIGHT(CodEii_c,1) ASC"

                    querryInsertExc = "INSERT INTO excentests(codPro, certificate_id, intCarg, numPr, maxExec, maxErr, pos1, pos1_r, pos2, pos2_r, pos3, evl) VALUES "
                    ## consulta para ver las pruebas de repetibilidad
                    querryRepet = f"SELECT * FROM RepetII_Cab C JOIN RepetII_Det D ON C.IdeComBpr = D.CodRii_C WHERE C.IdeComBpr LIKE '{data_cert[0]}'"
                elif data_cert[2] == 'Camionera' or data_cert[2] == 'CAM':
                    ## consulta para ver las pruebas de exentricidad
                    querryExcCad = f"SELECT * FROM ExecCam_Cab WHERE IdeComBpr LIKE '{data_cert[0]}' ORDER BY PrbCam_c ASC"
                    querryExcDet = f"SELECT * FROM ExecCam_Det WHERE CodCam_c LIKE '{data_cert[0]}%' ORDER BY RIGHT(CodCam_c,1) ASC"

                    querryInsertExc = "INSERT INTO excentests(codPro, certificate_id, intCarg, numPr, maxExec, maxErr, pos1, pos1_r, pos2, pos2_r, pos3, pos3_r, evl) VALUES "
                    ## consulta para ver las pruebas de repetibilidad
                    querryRepet = f"SELECT * FROM RepetIII_Cab C JOIN RepetIII_Det D ON C.IdeComBpr = D.CodRiii_C WHERE C.IdeComBpr LIKE '{data_cert[0]}'"

    ## __________________________________________________ EXENTRICIDAD __________________________________________________
                cursorsqlsrv.execute(querryExcCad)
                exectCad = cursorsqlsrv.fetchall()
                cursorsqlsrv.execute(querryExcDet)
                exectDet = cursorsqlsrv.fetchall()

    ## --------------------------- crear el querry para insertar los datos de pruebas de exentricidad
                for pex in [0,1]:
                    if data_cert[2] == 'Camionera' or data_cert[2] == 'CAM':
                        querryInsertExc = querryInsertExc + f"('{data_cert[0]}{exectCad[pex][2]}',{codCert[0]},{exectCad[pex][1]},{exectCad[pex][2]},{round(exectDet[pex][7],rd)},{round(exectDet[pex][8],rd)},{round(exectDet[pex][1],rd)},{round(exectDet[pex][2],rd)},{round(exectDet[pex][3],rd)},{round(exectDet[pex][4],rd)},{round(exectDet[pex][5],rd)},{round(exectDet[pex][6],rd)},'{exectCad[pex][3]}' ),"
                    else :
                        querryInsertExc = querryInsertExc + f"('{data_cert[0]}{exectCad[pex][2]}',{codCert[0]},{exectCad[pex][1]},{exectCad[pex][2]},{round(exectDet[pex][6],rd)},{round(exectDet[pex][7],rd)},{round(exectDet[pex][1],rd)},{round(exectDet[pex][2],rd)},{round(exectDet[pex][3],rd)},{round(exectDet[pex][4],rd)},{round(exectDet[pex][5],rd)},'{exectCad[pex][4]}' ),"
                
    ## -------------------------- insercion de pruebas de exentricidad
                print("     ==> UPLOADING DATA ECCENTRUCUTY TEST DATA..")
                try:
                    cursormysql.execute(querryInsertExc[:-1])
                    MySQLConnection.commit() 
                    print ("        ==> SUCCESSFULLY LOADED ECCENTRICITY TEST DATA ✅")
                except:
                    logs += f"==> ERROR LADING ECCENTRICITY TEST DATA ⚠ || {data_cert[0]}\n"
                    print ("        ==> ERROR LADING ECCENTRICITY TEST DATA ⚠")

    ## --------------------------- Datos de Pruebas de repetibilidad
                cursorsqlsrv.execute(querryRepet)
                repet = cursorsqlsrv.fetchone()
                
                print("     ==> UPLOADING DATA REPEATIBILITY TEST DATA..")
                try:
                    ## --- creacion de querry para insertar datos de repetbilidad
                    if data_cert[2] == 'II':
                        querryInsertRept = f"INSERT INTO repeatests(codPro,certificate_id,intCarg,maxDif,maxErr,lec1,lec1_0,lec2,lec2_0,lec3,lec3_0,lec4,lec4_0,lec5,lec5_0,lec6,lec6_0,evl) VALUES ('{data_cert[0]}',{codCert[0]},{repet[1]},{round(repet[2],rd)},{round(repet[3],rd)},{round(repet[7],rd)},{round(repet[8],rd)},{round(repet[9],rd)},{round(repet[10],rd)},{round(repet[11],rd)},{round(repet[12],rd)},{round(repet[13],rd)},{round(repet[14],rd)},{round(repet[15],rd)},{round(repet[16],rd)},{round(repet[17],rd)},{round(repet[18],rd)},'{repet[4]}')"
                    else: 
                        querryInsertRept = f"INSERT INTO repeatests(codPro,certificate_id,intCarg,maxDif,maxErr,lec1,lec1_0,lec2,lec2_0,lec3,lec3_0,evl) VALUES ('{data_cert[0]}',{codCert[0]},{repet[1]},{round(repet[2],rd)},{round(repet[3],rd)},{round(repet[7],rd)},{round(repet[8],rd)},{round(repet[9],rd)},{round(repet[10],rd)},{round(repet[11],rd)},{round(repet[12],rd)},'{repet[4]}')"
                    
                    cursormysql.execute(querryInsertRept)
                    MySQLConnection.commit() 
                    print ("        ==> SUCCESSFULLY LOADED REPETIBILITY TEST DATA ✅")
                except:
                    logs += f"==> ERROR LADING REPEATABILITY TEST DATA ⚠ || {data_cert[0]}\n" 
                    print ("        ==> ERROR LADING REPEATABILITY TEST DATA ⚠")

    ## --------------------------------- Datos de pruebas de Carga
                cursorsqlsrv.execute(f" SELECT C.NumPca,C.CarPca,D.LecAscPca,D.LecDscPca,D.ErrAscPca,D.ErrDscPca,D.EmpPca,D.SatPca_D FROM PCarga_Cab C LEFT OUTER JOIN( SELECT LecAscPca,LecDscPca,ErrAscPca,ErrDscPca,EmpPca,SUBSTRING(CodPca_C, 8, 2) AS NumPc,SatPca_D FROM PCarga_Det  WHERE CodPca_C LIKE '{data_cert[0]}%') D ON (D.NumPc=C.NumPca) WHERE C.IdeComBpr LIKE '{data_cert[0]}%'")
                carg = cursorsqlsrv.fetchall()

                ## ------ Querry para pruebas de Carga
                querryInsertCrg = "INSERT INTO cargtests(codPro, certificate_id, numPr, intCarg, lecAsc, lecDesc, errAsc, errDesc, maxErr, evl) VALUES "
                for pcar in range(0,len(carg)):
                    try:
                        querryInsertCrg = querryInsertCrg + f"('{data_cert[0]}{carg[pcar][0]}',{codCert[0]},{carg[pcar][0]},{round(carg[pcar][1],rd)},{round(carg[pcar][2],rd)},{round(carg[pcar][3],rd)},{round(carg[pcar][4],rd)},{round(carg[pcar][5],rd)},{round(carg[pcar][6],rd)},'{carg[pcar][7]}'),"
                    except:
                        print ('errordata')
                    
                
                ## ------ Insercion de pruebas de Carga
                print("     ==> UPLOADING DATA WEIGTH TEST DATA..")
                try:
                    cursormysql.execute(querryInsertCrg[:-1])
                    MySQLConnection.commit()  
                    print ("        ==> SUCCESSFULLY LOADED WEIGTH TEST DATA ✅")
                except:
                    logs += f"==> ERROR LADING WEIGTH TEST DATA ⚠ || {data_cert[0]}\n" 
                    print(querryInsertCrg[:-1])
                    print ("        ==> ERROR LADING WEIGTH TEST DATA ⚠")

    ## ------------------------------ Datos de pruebas de Pesas
                cursorsqlsrv.execute(f"SELECT ideComBpr,TipPxp,NonCerPxp,SUM(N1) AS N1,SUM(N2) AS N2,SUM(N2A) AS N2A,SUM(N5) AS N5,SUM(N10) AS N10,SUM(N20) AS N20,SUM(N20A) AS N20A,SUM(N50) AS N50,SUM(N100) AS N100,SUM(N200) AS N200,SUM(N200A) AS N200A,SUM(N500) AS N500,SUM(N1000) AS N1000,SUM(N2000) AS N2000,SUM(N2000A) AS N2000A,SUM(N5000) AS N5000,SUM(N10000) AS N10000,SUM(N20000) AS N20000,SUM(N500000) AS N500000,SUM(N1000000) AS N1000000,SUM(CrgPxp1) AS CrgPxp1,SUM(CrgPxp2) AS CrgPxp2,SUM(CrgPxp3) AS CrgPxp3,SUM(CrgPxp4) AS CrgPxp4,SUM(CrgPxp5) AS CrgPxp5,SUM(CrgPxp6) AS CrgPxp6,SUM(AjsPxp) AS AjsPxp FROM Pesxpro WHERE IdeComBpr LIKE '{data_cert[0]}' GROUP BY IdeComBpr, NonCerPxp, TipPxp")
                pesxpro = cursorsqlsrv.fetchall()  
                cursorsqlsrv.execute(f"SELECT NomCer FROM Cert_Balxpro WHERE IdeComBpr LIKE '{data_cert[0]}'")
                certItems = cursorsqlsrv.fetchall()

                # print(pesxpro)
                listCert = {}
                print("     ==> UPLOADING CERTIFICATES ITEMS..")
                for crt in range(0,len(certItems)):
                    cursormysql.execute(f"SELECT id FROM cert_items WHERE nom LIKE '{certItems[crt][0]}'")
                    idCertItm = cursormysql.fetchone()
                    if idCertItm:
                        listCert[certItems[crt][0]] = idCertItm[0]
                        try: 
                            cursormysql.execute(f"INSERT INTO cert_item_certificate(cert_item_id,certificate_id,codUni)VALUES({idCertItm[0]},{codCert[0]},{idCertItm[0]}{codCert[0]})")
                            MySQLConnection.commit()  
                            print ("        || ==> SUCCESSFULLY LOADED RELATION CERTIFICATE WITH CERT_ITEM ✅")
                        except:
                            logs += f"==> ERROR IN FIND CERTITEMS ⚠ || {data_cert[0]}\n" 
                            print('        || ==> ERROR IN FIND CERTITEMS ⚠')
                cursormysql.execute(f"SELECT COUNT(*) FROM cargpesxes WHERE codPro LIKE '{data_cert[0]}'")
                pexesMy = cursormysql.fetchone()
                print(pexesMy[0])
                if pexesMy[0] == 0 :
                    querryInsertPex = "INSERT INTO cargpesxes(codPro, cert_item_id, tip, keyJ, N1, N2, N2A, N5, N10, N20, N20A, N50, N100, N200, N200A, N500, N1000, N2000, N2000A, N5000, N10000, N20000, N500000, N1000000, CrgPxp1, CrgPxp2, CrgPxp3, CrgPxp4, CrgPxp5, CrgPxp6, AjsPxp) VALUES"
                    for pexs in range(0,len(pesxpro)):
                        aux = pesxpro[pexs][1]
                        if aux[0] == 'C':
                            aux = aux[1:-1].rjust(3, '0')
                        elif aux[0] == 'I':
                            aux = '001'
                        querryInsertPex = querryInsertPex + f"('{data_cert[0]}',{listCert[pesxpro[pexs][2]]},'{pesxpro[pexs][1]}','{aux}',{pesxpro[pexs][3]},{pesxpro[pexs][4]},{pesxpro[pexs][5]},{pesxpro[pexs][6]},{pesxpro[pexs][7]},{pesxpro[pexs][8]},{pesxpro[pexs][9]},{pesxpro[pexs][10]},{pesxpro[pexs][11]},{pesxpro[pexs][12]},{pesxpro[pexs][13]},{pesxpro[pexs][14]},{pesxpro[pexs][15]},{pesxpro[pexs][16]},{pesxpro[pexs][17]},{pesxpro[pexs][18]},{pesxpro[pexs][19]},{pesxpro[pexs][20]},{pesxpro[pexs][21]},{pesxpro[pexs][22]},{pesxpro[pexs][23]},{pesxpro[pexs][24]},{pesxpro[pexs][25]},{pesxpro[pexs][26]},{pesxpro[pexs][27]},{pesxpro[pexs][28]},{pesxpro[pexs][29]}),"
                    
                    print("     ==> UPLOADING PEXPROXS..")
                    try:
                        # cursormysql.execute(f"DELETE cargpesxes WHERE codPro LIKE '{data_cert[0]}'")
                        # MySQLConnection.commit()
                        cursormysql.execute(querryInsertPex[:-1])
                        MySQLConnection.commit()  
                        print ("        ==> SUCCESSFULLY LOADED PESXPRO TEST DATA ✅")
                    except:
                        logs += f"==> ERROR LADING PESXPRO TEST DATA ⚠ || {data_cert[0]}\n" 
                        print ("        ==> ERROR LADING PESXPRO TEST DATA ⚠")
                else :
                    logs += f"==> ALREADY EXIST PESXPRO TEST DATA ⚠ || {data_cert[0]}\n" 
                    print (" ==> ALREADY EXIST PESXPRO TEST DATA ⚠    -----------")

        
        print("""\n♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠
            \n♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠
            \n♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠-♠""")
        print("========================================================================= \n\n")
print("DATA MIGRATION COMPLETED SUCCESSFULLY")
# if logs != '':
#     file = open(f"./LogMySQL-{today}.txt", "a")
#     file.write(logs)
#     file.close()


MySQLConnection.close()
SQLServerConnection.close()

