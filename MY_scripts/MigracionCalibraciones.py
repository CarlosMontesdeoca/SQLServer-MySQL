# CONEXION pyw 
## este escript esta creado para comuicar dos bases de datos que se actualizaran cuando un metrolog envie datos primarios de la tablet al servidor
import pymysql
import numpy as np
from datetime import date

today = date.today()

logs = ''

try:
    MySQLConnection = pymysql.connect(host="127.0.0.1",user="root",database="pruebas" )
    print (" ==> CONEXION EXITOSA CON MYSQL")
except:
    print ('error to try connect the database MySQL')


print('=========================================================================')
# CONSULTAS

## CUSORES DE CONSULTAS 
cursormysql = MySQLConnection.cursor()
## ==============================================================

print('LEYENDO TODOS LOS PROYECTOS')
## busca todos los proyectos pendientes de MySQL
cursormysql.execute("SELECT * FROM projects WHERE codPro IS NOT NULL")
data = cursormysql.fetchall()

print(len(data))
i = 1
# print(data[0])
for dt in data:
    aux_id = dt[0]
    aux_est = 'P'
    if ( dt[1] == 'N/A'): 
        offert = 'AUX-NAC20##-' + str(i).zfill(3)
        i += 1
    else:
        offert = dt[1]
    if ( dt[8] == 'F' ):
        aux_est = 'F'
    
    print(f"INGRESANDO PROYECTO DEL PEDIDO ================> {offert}")

    
    cursormysql.execute(f"SELECT * FROM orders WHERE N_offert LIKE '{offert}'")
    order = cursormysql.fetchone()

    # Se compara que el numero de orden no este registrado
    # si esta registrado solo obtendremos el id de orden o lo crearemos para agregar los proyectos
    if order :
        order_id = order[0]
    else :
        print('   CREANDO PEDIDO..')
        query_offert = f"INSERT INTO orders (N_offert, plant_id, contact_id, ases_id, fact, itms, est, com, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        valor_offet = (offert,dt[4],dt[5],dt[11],dt[9],dt[14],aux_est,dt[17],dt[18],dt[19])
        # cursormysql.execute(query_offert, valor_offet)
        # MySQLConnection.commit()
        # order = cursormysql.fetchone()
        # order_id = order[0]
    


    # try: 
    #     cursormysql.execute(f"INSERT INTO balances (tip,marc,modl,ser,cls,maxCap,usCap,div_e,div_d,uni,plant_id) VALUES ('{data_cert[3]}','{data_cert[5]}','{data_cert[6]}','{data_cert[7]}','{clsBl}',{data_cert[8]},{data_cert[9]},{round(data_cert[10],6)},{round(data_cert[11],6)},'kg',{idPlant[0]})")
    #     MySQLConnection.commit()
    # except:
    #     logs += f"==> ERROR TO CREATED NEW BALANCE FOR CERTIFICATE ⚠ \n" 
MySQLConnection.close()