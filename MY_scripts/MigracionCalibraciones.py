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
        offert = 'AUX-NAC20##-' + str(i).zfill(3) + '.'
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
    if dt[11] :
        ases = dt[11]
    else :
        ases = 1
    if order :
        order_id = order[0]
    else :
        print('   CREANDO PEDIDO..')
        query_offert = f"""INSERT INTO orders (N_offert, plant_id, contact_id, ases_id, fact, itms, est, created_at, updated_at) VALUES ('{offert}',{dt[4]},{dt[5]},{ases},'{dt[9]}','{dt[14]}','{aux_est}','{dt[18]}','{dt[19]}')"""
        try:
            cursormysql.execute(query_offert)
            MySQLConnection.commit()
            order_id = cursormysql.lastrowid
            print(f"       PEDIDO CREADO CORRECTAMENTE")
        except pymysql.connect.Error as err:
            print(f"Error de MySQL: {err}")

    # Crear proyecto y asignarlo al numero de orden
    print('   CREANDO PROYECTO..')
    query_calibrate = f"""INSERT INTO calibrates (order_id, tip, codPro, metrologist_id, recPor, fecAsg, path, est, com, created_at, updated_at) VALUES ({order_id},'{dt[2]}','{dt[3]}',{dt[6]},'{dt[7]}','{dt[12]}','{dt[15]}','{dt[8]}','{dt[17]}','{dt[-2]}','{dt[-1]}')"""
    try:
        cursormysql.execute(query_calibrate)
        MySQLConnection.commit()
        project_id = cursormysql.lastrowid
        print(f"       PRYECTO CREADO CORRECTAMENTE")
    except pymysql.connect.Error as err:
        print(f"Error de MySQL: {err}")
    
    print('   INDEXANDO CERTIFICADOS..')
    if dt[2] == 'ICC':
        query_certificates = f"UPDATE certificates SET calibrate_id = {project_id} WHERE project_id LIKE {dt[0]}"
        cursormysql.execute(query_certificates)
        MySQLConnection.commit()
    else :
        print('       SIN CERTIFICADOS PARA ICM')
    
    print('   INDEXANDO EVENTOS..')
    query_event = f"UPDATE calendar SET calibrate_id = {project_id} WHERE project_id LIKE {dt[0]}"
    cursormysql.execute(query_event)
    MySQLConnection.commit()
    # try: 
    #     cursormysql.execute(f"INSERT INTO balances (tip,marc,modl,ser,cls,maxCap,usCap,div_e,div_d,uni,plant_id) VALUES ('{data_cert[3]}','{data_cert[5]}','{data_cert[6]}','{data_cert[7]}','{clsBl}',{data_cert[8]},{data_cert[9]},{round(data_cert[10],6)},{round(data_cert[11],6)},'kg',{idPlant[0]})")
    #     MySQLConnection.commit()
    # except:
    #     logs += f"==> ERROR TO CREATED NEW BALANCE FOR CERTIFICATE ⚠ \n" 
MySQLConnection.close()