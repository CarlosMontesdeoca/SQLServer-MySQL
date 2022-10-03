# CONEXION pyw
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
cursorsqlsrv.execute('''SELECT 
#     DesBpr as descBl,
#     MarBpr as marc,
#     ModBpr as modl,
#     SerBpr as ser,
#     CapMaxBpr as maxCap,
#     CapUsoBpr as usCap,
#     DivEscBpr as  div_e,
#     DivEsc_dBpr as dev_d,
#     RanBpr as rang,
#     IdeComBpr as codPro, 
#     UbiBpr as ubi, 
#     BalLimpBpr as evlBal1, 
#     AjuBpr as evlBal2, 
#     IRVBpr as evlBal3, 
#     ObsVBpr as obs,
#     CapCalBpr as uso, 
#     RecPorCliBpr as recPor,
# 	fec_cal as fecCal,
# 	fec_proxBpr as fecProxCal
# 	FROM Balxpro  WHERE est_esc LIKE 'PR';''')

data1 = cursorsqlsrv.fetchall()


cursormysql = MySQLConnection.cursor()
cursormysql.execute('''SELECT codPro FROM certificates WHERE est LIKE 'P';''')
# cursormysql.execute('''SELECT * FROM certificates WHERE est LIKE 'P';''')

data2 = cursormysql.fetchall()


print (data1)
print( '________________________________________________________________________________________')
print(data2)

for codtb in data1:
    # print (codtb[0])
    querry = "SELECT * FROM certificates WHERE est LIKE 'P' AND codPro LIKE '" + codtb[0] + "';"
    cursormysql.execute(querry)
    certificate = cursormysql.fetchall()

    if certificate:
        updateStatement = "UPDATE certificates SET est = 'X' WHERE codPro LIKE '" + codtb[0] + "'"
        cursormysql.execute(updateStatement) 
        MySQLConnection.commit()
        
        # Select the updated row and print the updated column value
        sqlSelectUpdated   = "SELECT * FROM certificates WHERE codPro LIKE '" + codtb[0] + "'"

        # Execute the SQL SELECT query
        cursormysql.execute(sqlSelectUpdated)

        # Fetch the updated row
        updatedRow = cursormysql.fetchall()

        # Print the updated row...

        for column in updatedRow:

            print(column)  

        # cursormysql.execute("UPDATE certificates SET est = 'X' WHERE codPro LIKE '" + codtb[0] + "';")
        # updatecert = cursormysql.fetchall()
        # print ('llegaron datos')
    else : 
        print (' no hay datos')
    # for coddb in data2:
    #     if codtb[0] == coddb[0]:
    #         print( 'llegaron datos' )
    #         break
    #     

    print('_____')

MySQLConnection.close()
SQLServerConnection.close()