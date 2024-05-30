# CONEXION pyw 
## este escript esta creado para comuicar dos bases de datos que se actualizaran cuando un metrolog envie datos primarios de la tablet al servidor
import re
import pyodbc
import pymysql
import pandas as pd
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
querryFindFactura = f"""SELECT  
	FACHIS.FHFactura AS Factura,
	FACHIS.FHComent2 AS OFERTA, 
	CP.CTCodigo,
	CONVERT(char(10), MAX(CLHFEmision), 23) AS fechaEmisionUltimoPago
FROM 
	INVBOD 
	INNER JOIN INVHIS ON INVBOD.IBID = INVHIS.IHBodega 
	INNER JOIN INVMAE ON INVHIS.IHProducto = INVMAE.IMId 
	INNER JOIN INVCAT ON INVMAE.IMGrupo = INVCAT.ICID  
	INNER JOIN FACHIS ON INVHIS.IHFHID = FACHIS.FHID
	INNER JOIN CXCHISFacturasCuotas FC ON FC.Factura = FACHIS.FHFactura
	LEFT OUTER JOIN CXCDIR ON CXCDIR.CodigoID = FACHIS.FHClave
    INNER JOIN CXCHIS CH ON FC.CXCHISID = CH.CLHID
    INNER JOIN CXCPTRX CP ON CP.CTID = CH.CLTD
	
WHERE
	CP.CTCodigo  IN ('RR', 'RI', 'CA', 'FC')
	AND YEAR(CH.CLHFEmision) BETWEEN {today.year - 1} AND {today.year}
GROUP BY 
	FACHIS.FHFactura,
	FACHIS.FHComent2,
	CP.CTCodigo;"""

try:
    # df = pd.read_sql(querryFindFactura, SQLServerConnection)
    # df.to_excel("facturas.xlsx", index = False, engine = 'openpyxl')
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
            for temp_order in aux :
                if validate_order(temp_order):
                    ## -- busca la oferta en el SGO
                    cursormysql.execute(f"SELECT * FROM orders WHERE N_offert LIKE '{temp_order}'")
                    order = cursormysql.fetchone()
                    if order :
                        # print(fact[1])
                        ## -- Facturas Pagadas
                        if fact[2] == 'CA' and order[13] != 'A':
                            ## -- sin numero de factura  y se registra como pagado
                            if 'LM-UIO2024-224' in fact[1] : print(f"{fact[1]} -- {'LM-UIO2024-224' in fact[1]}")
                            if order[7] == None :
                                querryOrder = f"UPDATE orders set numFact = '{fact[0]}', fecRegPag = '{fact[3]}', fecCom = '{today}', autr = 1,est = 'A', com = 'AUTORIZADO SAFI' WHERE N_offert LIKE '{temp_order}'"

                            ## -- si coinciden los numeros de oferta y esta en estado F(autorizado)
                            if order[7] == fact[0] and order[13] == 'F':
                                querryOrder = f"UPDATE orders set fecRegPag = '{fact[3]}', fecCom = '{today}', autr = 1,est = 'A' WHERE N_offert LIKE '{temp_order}'"

                        ## -- Retencion registrada
                        if fact[2] == 'FC' and order[13] == 'P' and order[7] is None:
                            # if 'LM-UIO2024-224' in temp_order: print(fact[2])
                            cursormysql.execute(f"SELECT C.entRpd, O.numFact FROM orders O JOIN plants P ON P.id = O.plant_id JOIN clients C ON P.client_id = C.id WHERE O.N_offert LIKE '{temp_order}'")
                            validate = cursormysql.fetchone()
                            if validate[0] and not validate[1] :
                                querryOrder = f"UPDATE orders set numFact = '{fact[0]}', autr = 1, com = 'AUTORIZADO SAFI LB' WHERE N_offert LIKE '{temp_order}' "
                            else :
                                querryOrder = f"UPDATE orders set numFact = '{fact[0]}' WHERE N_offert LIKE '{temp_order}' "
                        if  (fact[2] == 'RI' or fact[2] == 'RR') and order[13] == 'P':

                            # if 'LM-UIO2024-224' in temp_order: print(f"{fact[2]}--{order[11]}")
                            ## sin pago registrado debe estar en pendiente
                            if order[13] == 'P':
                                querryOrder = f"UPDATE orders set numFact = '{fact[0]}', autr = 1, est = 'F', com = 'AUTORIZADO SAFI' WHERE N_offert LIKE '{temp_order}' "
                            else :
                                querryOrder = ''
                        else :
                            querryOrder = ''

                        try:
                            cursormysql.execute(querryOrder)
                            MySQLConnection.commit()
                            print(f'✔️ Factura N°: {fact[0]}')
                            print('========================================================================')
                        except: 
                            continue
                                
                    else :
                        print('⚠️ no existe la oferta registrada!!')
                        print('========================================================================')

MySQLConnection.close()
SQLServerConnection.close()

