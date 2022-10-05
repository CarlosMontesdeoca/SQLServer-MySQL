# CONEXION pyw
import pyodbc
import pymysql

server1='tcp:192.168.9.221'
dbname1='SisMetPrec'
user1='sa'
password1='Sistemas123*'

try:
    SQLServerConnection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL server}; SERVER='+server1+';DATABASE='+dbname1+';UID='+user1+';PWD='+password1)
    print (" ==> CONNECCTION SUCCESS WITH SQL SERVER")
except:
    print ('error to try connect the database')

try:
    MySQLConnection = pymysql.connect(host="127.0.0.1",user="root",passwd="",database="test" )
    print (" ==> CONNECCTION SUCCESS WITH MYSQL")
except:
    print ('error to try connect the database')

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
print( data1, '=========================================================================')

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
            print (f"  ==> SUCCESSFULLY LOADED CERTIFICATE DATA ✅")
        except:
            print ("  ==> ERROR LADING CERTIFICATE DATA ⚠")

        ## modifica la informacion de la balanza.
        try:
            cursormysql.execute(f"UPDATE balances SET descBl = '{balxpro[1].upper()}', marc = '{balxpro[2].upper()}', modl = '{balxpro[3].upper()}', ser = '{balxpro[4].upper()}', maxCap = {balxpro[5]}, usCap = {balxpro[6]}, div_e = {balxpro[7]}, div_d = {balxpro[8]}, rang = {balxpro[9]} WHERE id LIKE {certificate[2]}")
            MySQLConnection.commit()  # actualizacion de balanza calibrada MySQL
            print (f"  ==> SUCCESSFULLY LOADED BALANCE DATA ✅")

        except:
            print ("  ==> ERROR LADING BALANCE DATA ⚠")
        
        ###_____________________________________________________AMBIENTALES________________________________________________________________________###
        cursorsqlsrv.execute(f"SELECT * FROM Ambientales WHERE IdeComBpr LIKE '{codtb[0]}'")
        envir = cursorsqlsrv.fetchone() 
        
        ## creacion de datos ambientales de la balanza.
        try:
            cursormysql.execute(f"INSERT INTO enviroments(codPro,certificate_id,tempIn,tempFn,humIn,humFn)VALUES('{codtb[0]}',{certificate[0]},{envir[1]},{envir[2]},{envir[3]},{envir[4]})")
            MySQLConnection.commit()  # actualizacion de balanza calibrada MySQL
            print (f"  ==> SUCCESSFULLY LOADED ENVIROMENTALS DATA ✅")

        except:
            print ("  ==> ERROR LADING BALANCE DATA ⚠") 

        ####### -------------- INSERTA LOS DATOS DE LAS PRUEBAS DE CALIBRACION ------------------------------#######
        
        if balxpro[0] == 'III' or balxpro[0] == 'IIII':
            ## consulta para ver las pruebas de exentricidad
            querryExcCad = f"SELECT * FROM ExecII_Cab WHERE IdeComBpr LIKE '{codtb[0]}' ORDER BY PrbEii ASC"
            querryExcDet = f"SELECT * FROM ExecII_Det WHERE CodEii_c LIKE '{codtb[0]}%' ORDER BY RIGHT(CodEii_c,1) ASC"

            querryInsertExc = f"INSERT INTO excentests(codPro, certificate_id, intCarg, numPr, maxExec, maxErr, pos1, pos1_r, pos2, pos2_r, pos3, evl) VALUES "
            ## consulta para ver las pruebas de repetibilidad
            querryRepet = f"SELECT * FROM RepetIII_Cab C JOIN RepetIII_Det D ON C.IdeComBpr = D.CodRiii_C WHERE C.IdeComBpr LIKE '{codtb[0]}'"
        elif balxpro[0] == 'II':
            ## consulta para ver las pruebas de exentricidad
            querryExcCad = f"SELECT * FROM ExecII_Cab WHERE IdeComBpr LIKE '{codtb[0]}' ORDER BY PrbEii ASC"
            querryExcDet = f"SELECT * FROM ExecII_Det WHERE CodEii_c LIKE '{codtb[0]}%' ORDER BY RIGHT(CodEii_c,1) ASC"

            querryInsertExc = f"INSERT INTO excentests(codPro, certificate_id, intCarg, numPr, maxExec, maxErr, pos1, pos1_r, pos2, pos2_r, pos3, evl) VALUES "
            ## consulta para ver las pruebas de repetibilidad
            querryRepet = f"SELECT * FROM RepetII_Cab C JOIN RepetII_Det D ON C.IdeComBpr = D.CodRii_C WHERE C.IdeComBpr LIKE '{codtb[0]}'"
        elif balxpro[0] == 'CAM':
            ## consulta para ver las pruebas de exentricidad
            querryExcCad = f"SELECT * FROM ExecCam_Cab WHERE IdeComBpr LIKE '{codtb[0]}' ORDER BY PrbEii ASC"
            querryExcDet = f"SELECT * FROM ExecCam_Det WHERE CodCam_c LIKE '{codtb[0]}%' ORDER BY RIGHT(CodCam_c,1) ASC"

            querryInsertExc = f"INSERT INTO excentests(codPro, certificate_id, intCarg, numPr, maxExec, maxErr, pos1, pos1_r, pos2, pos2_r, pos3, pos3_r, evl) VALUES "
            ## consulta para ver las pruebas de repetibilidad
            querryRepet = f"SELECT * FROM RepetIII_Cab C JOIN RepetIII_Det D ON C.IdeComBpr = D.CodRiii_C WHERE C.IdeComBpr LIKE '{codtb[0]}'"

        cursorsqlsrv.execute(querryExcCad)
        exectCad = cursorsqlsrv.fetchall()

        cursorsqlsrv.execute(querryExcDet)
        exectDet = cursorsqlsrv.fetchall()

        
        for pex in [0,1]:
            querryInsertExc = querryInsertExc + f"('{codtb[0]}',{certificate[0]},{exectCad[pex][1]},{exectCad[pex][2]},{exectDet[pex][6]},{exectDet[pex][7]},{exectDet[pex][1]},{exectDet[pex][2]},{exectDet[pex][3]},{exectDet[pex][4]},{exectDet[pex][5]},'{exectCad[pex][4]}' ),"
        
        try:
            cursormysql.execute(querryInsertExc[:-1])
            MySQLConnection.commit()  # ingreso de valores a la tabla certificates MySQL
            print (f"  ==> SUCCESSFULLY LOADED ECCENTRICITY TEST DATA ✅")
        except:
            print ("  ==> ERROR LADING ECCENTRICITY TEST DATA ⚠")

        cursorsqlsrv.execute(querryRepet)
        repet = cursorsqlsrv.fetchone()

        try:
            cursormysql.execute(f"INSERT INTO repeatests(codPro,certificate_id,intCarg,maxDif,maxErr,lec1,lec1_0,lec2,lec2_0,lec3,lec3_0,evl) VALUES ('{codtb[0]}',{certificate[0]},{repet[1]},{repet[2]},{repet[3]},{repet[7]},{repet[8]},{repet[9]},{repet[10]},{repet[11]},{repet[12]},'{repet[4]}')")
            MySQLConnection.commit()  # ingreso de valores a la tabla certificates MySQL
            print (f"  ==> SUCCESSFULLY LOADED REPETIBILITY TEST DATA ✅")
        except:
            print ("  ==> ERROR LADING REPEATABILITY TEST DATA ⚠")

        cursorsqlsrv.execute(f"SELECT * FROM PCarga_Cab WHERE IdeComBpr LIKE '{codtb[0]}' ORDER BY NumPca ASC")
        cargCad = cursorsqlsrv.fetchall()
        cursorsqlsrv.execute(f"SELECT * FROM PCarga_Det WHERE CodPca_C LIKE '{codtb[0]}%' ORDER BY RIGHT(CodPca_C,1) ASC")
        cargDet = cursorsqlsrv.fetchall()

        querryInsertCrg = f"INSERT INTO cargtests(codPro, certificate_id, numPr, intCarg, lecAsc, lecDesc, errAsc, errDesc, maxErr, evl) VALUES "
        
        for pcar in range(0,len(cargCad)):
            querryInsertCrg = querryInsertCrg + f"('{codtb[0]}',{certificate[0]},{cargCad[pcar][2]},{cargCad[pcar][1]},{cargDet[pcar][1]},{cargDet[pcar][2]},{cargDet[pcar][3]},{cargDet[pcar][4]},{cargDet[pcar][5]},'{cargCad[pcar][4]}'),"
        try:
            cursormysql.execute(querryInsertCrg[:-1])
            MySQLConnection.commit()  # ingreso de valores a la tabla cargtests MySQL
            print (f"  ==> SUCCESSFULLY LOADED WEIGTH TEST DATA ✅")
        except:
            print ("  ==> ERROR LADING WEIGTH TEST DATA ⚠")
            

        print('======================================================= \n =======================================================')
        # # Select the updated row and print the updated column value
        # sqlSelectUpdated   = "SELECT * FROM certificates WHERE codPro LIKE '" + codtb[0] + "'"

        # # Execute the SQL SELECT query
        # cursormysql.execute(sqlSelectUpdated)

        # Fetch the updated row
        # updatedRow = cursormysql.fetchall()

        # Print the updated row...

        # for column in updatedRow:

        #     print(column)  

        # cursormysql.execute("UPDATE certificates SET est = 'X' WHERE codPro LIKE '" + codtb[0] + "';")
        # updatecert = cursormysql.fetchall()
        # print ('llegaron datos')
    # else : 
    #     print (codtb[0] + ':  no hay datos')
    # # for coddb in data2:
    # #     if codtb[0] == coddb[0]:
    # #         print( 'llegaron datos' )
    # #         break
    # #     


MySQLConnection.close()
SQLServerConnection.close()