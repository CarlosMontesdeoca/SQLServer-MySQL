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

print('SEARCHING PENDING DATA....')
cursorsqlsrv.execute('''SELECT IdeComBpr FROM Balxpro  WHERE est_esc LIKE 'PR';''')
data1 = cursorsqlsrv.fetchall()
print('=========================================================================')

print('CHECKING.......')
for codtb in data1:
    querry = f"SELECT * FROM certificates WHERE est LIKE 'P' AND codPro LIKE '{codtb[0]}'"
    cursormysql.execute(querry)
    certificate = cursormysql.fetchone()

    if certificate:  ## SI  HAY DATOS POR LO QUE SE ENVIARAN LOS DATOS PRIMARIOS
        print (f"  ==> UPLOAD DATA FROM CERTIFICATE: {codtb[0]}")
        cursorsqlsrv.execute(f"SELECT ClaBpr,DesBpr,MarBpr,ModBpr,SerBpr,CapMaxBpr,CapUsoBpr,DivEscBpr,DivEsc_dBpr,RanBpr,IdeComBpr, UbiBpr, BalLimpBpr, AjuBpr, IRVBpr, ObsVBpr,CapCalBpr, RecPorCliBpr,fec_cal,fec_proxBpr FROM Balxpro  WHERE ideComBpr LIKE '{codtb[0]}'")
        balxpro = cursorsqlsrv.fetchone()  
        
        # se obtiene los datos de balxpro  esto se envia a certificados
        try:
            cursormysql.execute(f"UPDATE certificates SET ubi = '{balxpro[11].upper()}', luCal = '{balxpro[11].upper()}', est = 'P', evlBal1 = '{balxpro[12]}', evlBal2 = '{balxpro[13]}', evlBal3 = '{balxpro[14]}', obs = '{balxpro[15].upper()}', uso = '{balxpro[16]}', recPor = '{balxpro[17].upper()}', fecCal = '{balxpro[18]}', fecProxCal = '{balxpro[19]}'  WHERE codPro LIKE '{codtb[0]}'")
            MySQLConnection.commit()  # ingreso de valores a la tabla certificates MySQL
            print (f"  ==> SUCCESSFULLY LOADED CERTIFICATE DATA ✅")
        except:
            print ("  ==> ERROR LADING CERTIFICATE DATA ⚠")

        try:
            cursormysql.execute(f"UPDATE balances SET descBl = '{balxpro[1].upper()}', marc = '{balxpro[2].upper()}', modl = '{balxpro[3].upper()}', ser = '{balxpro[4].upper()}', maxCap = {balxpro[5]}, usCap = {balxpro[6]}, div_e = {balxpro[7]}, div_d = {balxpro[8]}, rang = {balxpro[9]} WHERE id LIKE {certificate[2]}")
            MySQLConnection.commit()  # actualizacion de balanza calibrada MySQL
            print (f"  ==> SUCCESSFULLY LOADED BALANCE DATA ✅")

        except:
            print ("  ==> ERROR LADING BALANCE DATA ⚠")
        
        ####### -------------- INSERTA LOS DATOS DE LAS PRUEBAS DE CALIBRACION ------------------------------#######

        ## OBTENER PRUEBAS DE EXENTRICIDAD ....
        # cursorsqlsrv.execute()
        # exect = cursorsqlsrv.fetchone()
        # print(exect)
        
        if balxpro[0] == 'III':
            querryExcCad = f"SELECT * FROM ExecII_Cab WHERE IdeComBpr LIKE '{codtb[0]}' ORDER BY PrbEii ASC"
            querryExcDet = f"SELECT * FROM ExecII_Det WHERE CodEii_c LIKE '{codtb[0]}%' ORDER BY RIGHT(CodEii_c,1) ASC"
        #     querryRepet = "SELECT * FROM RepetIII_Cab C JOIN RepetIII_Det D on C.IdeCom
        # Bpr = D.CodRiii_C WHERE C.IdeComBpr LIKE '" + codtb[0] + "'"
        #     # cursorsqlsrv.execute()
        elif balxpro[0] =='II':
            querryExcCad = f"SELECT * FROM ExecII_Cab WHERE IdeComBpr LIKE '{codtb[0]}' ORDER BY PrbEii ASC"
            querryExcDet = f"SELECT * FROM ExecII_Det WHERE CodEii_c LIKE '{codtb[0]}%' ORDER BY RIGHT(CodEii_c,1) ASC"
            # querryRepet = "SELECT * FROM RepetII_Cab C JOIN RepetII_Det D on C.IdeComBpr = D.CodRii_C WHERE C.IdeComBpr LIKE '" + codtb[0] + "'"
            print('es 2')
        else:
            querryExcCad = f"SELECT * FROM ExecCam_Cab WHERE IdeComBpr LIKE '{codtb[0]}' ORDER BY PrbEii ASC"
            querryExcDet = f"SELECT * FROM ExecCam_Det WHERE CodCam_c LIKE '{codtb[0]}%' ORDER BY RIGHT(CodCam_c,1) ASC"
            # querryRepet = "SELECT * FROM RepetIII_Cab C JOIN RepetIII_Det D on C.IdeComBpr = D.CodRiii_C WHERE C.IdeComBpr LIKE '" + codtb[0] + "'"
            print('es camionera')
        # repet = cursorsqlsrv.fetchone()
        # print(repet)

        cursorsqlsrv.execute(querryExcCad)
        exectCad = cursorsqlsrv.fetchall()

        cursorsqlsrv.execute(querryExcDet)
        exectDet = cursorsqlsrv.fetchall()

        # print( exectCad, '\n', exectDet)
        querryInsertExc = f"INSERT INTO excentests(codPro, certificate_id, intCarg, numPr, maxExec, maxErr, pos1, pos1_r, pos2, pos2_r, pos3, evl) VALUES "
        for pex in [0,1]:
            querryInsertExc = querryInsertExc + f"('{codtb[0]}',{certificate[0]},{exectCad[pex][1]},{exectCad[pex][2]},{exectDet[pex][6]},{exectDet[pex][7]},{exectDet[pex][1]},{exectDet[pex][2]},{exectDet[pex][3]},{exectDet[pex][4]},{exectDet[pex][5]},'{exectCad[pex][4]}' ),"
        
        try:
            cursormysql.execute(querryInsertExc[:-1])
            MySQLConnection.commit()  # ingreso de valores a la tabla certificates MySQL
            print (f"  ==> SUCCESSFULLY LOADED ECCENTRICITY TEST DATA ✅")
        except:
            print ("  ==> ERROR LADING ECCENTRICITY TEST DATA ⚠")
    
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
    else : 
        print (codtb[0] + ':  no hay datos')
    # for coddb in data2:
    #     if codtb[0] == coddb[0]:
    #         print( 'llegaron datos' )
    #         break
    #     

    print('_____')

MySQLConnection.close()
SQLServerConnection.close()