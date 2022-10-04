# CONEXION pyw
from ssl import cert_time_to_seconds
import pyodbc
import pymysql

server1='tcp:192.168.9.221'
dbname1='SisMetPrec'
user1='sa'
password1='Sistemas123*'

try:
    SQLServerConnection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL server}; SERVER='+server1+';DATABASE='+dbname1+';UID='+user1+';PWD='+password1)
except:
    print ('error to try connect the database')

try:
    MySQLConnection = pymysql.connect(host="127.0.0.1",user="root",passwd="",database="test" )
except:
    print ('error to try connect the database')

# CONSULTAS

cursorsqlsrv = SQLServerConnection.cursor()
cursorsqlsrv.execute('''SELECT IdeComBpr FROM Balxpro  WHERE est_esc LIKE 'PR';''')

data1 = cursorsqlsrv.fetchall()


cursormysql = MySQLConnection.cursor()
cursormysql.execute("SELECT codPro FROM certificates WHERE est LIKE 'P'")
# cursormysql.execute('''SELECT * FROM certificates WHERE est LIKE 'P';''')

data2 = cursormysql.fetchall()


print (data1)
print( '________________________________________________________________________________________')
print(data2)

for codtb in data1:
    # print (codtb[0])
    querry = f"SELECT * FROM certificates WHERE est LIKE 'P' AND codPro LIKE '{codtb[0]}'"
    cursormysql.execute(querry)
    certificate = cursormysql.fetchone()
    print(certificate)

    if certificate:  ## SI  HAY DATOS POR LO QUE SE ENVIARAN LOS DATOS PRIMARIOS

        cursorsqlsrv.execute(f"SELECT ClaBpr,DesBpr,MarBpr,ModBpr,SerBpr,CapMaxBpr,CapUsoBpr,DivEscBpr,DivEsc_dBpr,RanBpr,IdeComBpr, UbiBpr, BalLimpBpr, AjuBpr, IRVBpr, ObsVBpr,CapCalBpr, RecPorCliBpr,fec_cal,fec_proxBpr FROM Balxpro  WHERE ideComBpr LIKE '{codtb[0]}'")
        balxpro = cursorsqlsrv.fetchone()  # se obtiene los datos de balxpro  esto se envia a certificados
        
        cursormysql.execute(f"UPDATE balances SET descBl = '{balxpro[1].upper()}', marc = '{balxpro[2].upper()}', modl = '{balxpro[3].upper()}', ser = '{balxpro[4].upper()}', maxCap = {balxpro[5]}, usCap = {balxpro[6]}, div_e = {balxpro[7]}, div_d = {balxpro[8]}, rang = {balxpro[9]} WHERE id LIKE {certificate[2]}")
        MySQLConnection.commit()  # actualizacion de balanza calibrada MySQL


        # if balxpro[0] == 'III':
        #     querryRepet = "SELECT * FROM RepetIII_Cab C JOIN RepetIII_Det D on C.IdeComBpr = D.CodRiii_C WHERE C.IdeComBpr LIKE '" + codtb[0] + "'"
        #     # cursorsqlsrv.execute()
        #     print('es 3')
        # elif balxpro[0] =='II':
        #     querryRepet = "SELECT * FROM RepetII_Cab C JOIN RepetII_Det D on C.IdeComBpr = D.CodRii_C WHERE C.IdeComBpr LIKE '" + codtb[0] + "'"
        #     print('es 2')
        # else:
        #     querryRepet = "SELECT * FROM RepetIII_Cab C JOIN RepetIII_Det D on C.IdeComBpr = D.CodRiii_C WHERE C.IdeComBpr LIKE '" + codtb[0] + "'"
        #     print('es camionera')
        # repet = cursorsqlsrv.fetchone()
        # print(repet)
    
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