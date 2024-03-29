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
    MySQLConnection = pymysql.connect(host="127.0.0.1",user="root",passwd="AdminSistemas@",database="metrologia" )
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
	F.FHComent2 AS OFERTA,
	CP.CTCodigo
FROM
    CXCHISFACTURASCUOTAS FC
    JOIN CXCHIS CH ON FC.CXCHISID = CH.CLHID
	JOIN FACHIS F ON FC.Factura = F.FHFactura
    JOIN CXCPTRX CP ON CP.CTID = CH.CLTD
WHERE
	CP.CTCodigo  in ('RR', 'RI', 'CA')
	AND YEAR(FechaEmision) = YEAR(GETDATE())
GROUP BY 
	Factura,
	F.FHComent2,
	CP.CTCodigo
ORDER BY
    Factura;"""

try:
    cursorsqlsrv.execute(querryFindFactura)
    facturasInfo = cursorsqlsrv.fetchall()
except Exception as e:
    print(f'ERROR AL EXTRAER LAS FACTURAS: {str(e)}')

print(f"Se encontraron {len(facturasInfo)}")

for fact in facturasInfo:
    ## Obtener el numero de Factura
    offert = fact[1]
    ## Solo trabajamos con las consultas que tengas número de oferta
    if offert != None :
        ## en caso de encontrar mas contenido del necesario trabajamos solo con el ultimo string
        aux = offert.split()
        if len(aux) > 0:
            aux = aux[-1]
        
            if validate_order(aux):
                ## -- busca la oferta en el SGO
                cursormysql.execute(f"SELECT * FROM orders WHERE N_offert LIKE '{aux}'")
                order = cursormysql.fetchone()
                if order :

                    ## -- Facturas Pagadas
                    if fact[2] == 'CA':
                        ## -- sin numero de factura  y se registra como pagado
                        if order[7] == None :
                            querryOrder = f"UPDATE orders set numFact = '{fact[0]}', fecRegPag = '{today}', fecCom = '{today}', est = 'A', com = 'AUTORIZADO SAFI' WHERE N_offert LIKE '{aux}'"

                        ## -- si coinciden los numeros de oferta y esta en estado F(autorizado)
                        if order[7] == fact[0] and order[11] == 'F':
                            querryOrder = f"UPDATE orders set fecRegPag = '{today}', fecCom = '{today}', est = 'A' WHERE N_offert LIKE '{aux}'"

                    ## -- Retencion registrada
                    else :
                        ## sin pago registrado
                        if order[11] != 'A':
                            querryOrder = f"UPDATE orders set numFact = '{fact[0]}', est = 'F', com = 'AUTORIZADO SAFI' WHERE N_offert LIKE '{aux}' "
                        else :
                            querryOrder = ''

                    try:
                        cursormysql.execute(querryOrder)
                        MySQLConnection.commit()
                        print(f'✔️ Factura N°: {fact[0]}')
                        print('========================================================================')
                    except:
                        print('t', querryOrder)
                        print('❌ no existe la oferta registrada!!')
                        print('========================================================================')
                            
                else :
                    print('⚠️ no existe la oferta registrada!!')
                    print('========================================================================')

MySQLConnection.close()
SQLServerConnection.close()

