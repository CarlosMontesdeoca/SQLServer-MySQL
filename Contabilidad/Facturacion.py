# CONEXION pyw 
## este escript esta creado para comuicar dos bases de datos que se actualizaran cuando un metrolog envie datos primarios de la tablet al servidor
import re
import pyodbc
import pymysql
from datetime import date

## esta funcion valida si el numero de oferta ingresado a la factura cumple con el formato correcto del Cotizador.
def validate_order(val):
    try:
        aux = re.compile(r'^[A-Za-z]{2,3}-(PMP-)?(UIO|GYE)\d{4}-\d{3}$')
        if aux.match(val):
            return True
        else : 
            return False
    except:
        return False

today = date.today()

server1='tcp:192.168.9.6\ESPINOSAPAEZ'
dbname1='V1791359038001_SAFI_4'
user1='sa'
password1='saSql'

logs = ''

# offert = 'ADM-PMP-UIO2023-999'

try:
    SQLServerConnection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL server}; SERVER='+server1+'; DATABASE='+dbname1+'; UID='+user1+'; PWD='+password1)
    print (" ==> CONNECCTION SUCCESS WITH SAFI SQL SERVER")
except:
    logs += "==> error to try connect the database SAFI SQL Server \n" 
    print ('error to try connect the database SQL Server')

try:
    MySQLConnection = pymysql.connect(host="127.0.0.1",user="root",passwd="",database="pruebas" )
    print (" ==> CONNECCTION SUCCESS WITH MYSQL")
except:
    logs += "==> error to try connect the database MySQL \n" 
    print ('error to try connect the database MySQL')


## CUSORES DE CONSULTAS 
cursorsqlsrv = SQLServerConnection.cursor()
cursormysql = MySQLConnection.cursor()

# busca las facturas del año actual
querryFindFactura = """SELECT
    Factura,
	FACHIS.FHComent2 AS OFERTA,
    FechaEmision AS EMISION
FROM
    CXCHISFACTURASCUOTAS FC
    JOIN CXCHIS CH ON FC.CXCHISID = CH.CLHID
    JOIN CXCDIR CD ON CD.codigoid = clhcdid
	JOIN FACHIS ON CD.codigoid = FACHIS.FHClave
    JOIN CXCPTRX CP ON CP.CTID = CH.CLTD
WHERE
	CP.CTCodigo like 'CA'
	AND YEAR(FechaEmision) = YEAR(GETDATE())
GROUP BY 
	Factura,
	FACHIS.FHComent2,
	FechaEmision
ORDER BY
    Factura;"""

try:
    cursorsqlsrv.execute(querryFindFactura)
    facturasInfo = cursorsqlsrv.fetchall()
except Exception as e:
    print(f'ERROR AL EXTRAER LAS FACTURAS: {str(e)}')

print(f"Se encontraron {len(facturasInfo)}")

for fact in facturasInfo:
    # print(f"FACTURA #: {fact[0]}, ===> codigo de oferta: {fact[1]}")
    if fact[0] != None :
        if validate_order(fact[1]):
            print(f"FACTURA #: {fact[0]}, ===> codigo de oferta: {fact[1]}")

# querryFindOrders = """SELECT * FROM orders WHERE est LIKE 'F' and numFact is NULL;"""

# try:
#     cursormysql.execute(querryFindOrders)
#     ordersInfo = cursormysql.fetchall()
# except Exception as e:
#     print(f'ERROR AL EXTRAER LAS FACTURAS: {str(e)}')

# print(f"Se encontraron {len(querryFindOrders)} pedidos")

MySQLConnection.close()
SQLServerConnection.close()

