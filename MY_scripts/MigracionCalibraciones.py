# CONEXION pyw 
## este escript esta creado para comuicar dos bases de datos que se actualizaran cuando un metrolog envie datos primarios de la tablet al servidor
import pymysql
import numpy as np
from datetime import date

today = date.today()

try:
    MySQLConnection = pymysql.connect(host="127.0.0.1",user="root",database="pruebas" )
    print (" ==> CONNECCTION SUCCESS WITH MYSQL")
except:
    print ('error to try connect the database MySQL')


print('=========================================================================')
# CONSULTAS

## CUSORES DE CONSULTAS 
cursormysql = MySQLConnection.cursor()
## ==============================================================

print('READING PROJECTS DATA....')
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
    
    print(f"CREATING ORDER ================> {offert}")
    query_offert = f"INSERT INTO orders (N_offert, plant_id, contact_id, ases_id, fact, itms, est, com, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    valor_offet = (offert,dt[4],dt[5],dt[11],dt[9],dt[14],aux_est,dt[17],dt[18],dt[19])
    
MySQLConnection.close()