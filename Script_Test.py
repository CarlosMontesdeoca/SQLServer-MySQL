# CONEXION pyw 
import pymysql
import random
from datetime import date

today = date.today()

try:
    MySQLConnection = pymysql.connect(host="127.0.0.1",user="root",passwd="",database="pruebas" )
    print (" ==> CONNECCTION SUCCESS WITH MYSQL")
except:
    print ('error to try connect the database MySQL')


print('=========================================================================')
# CONSULTAS

## CUSORES DE CONSULTAS 
cursormysql = MySQLConnection.cursor()
## ===================================================6===========

print('SEARCHING ITEMS DATA....')
## busca todos los proyectos pendientes de MySQL
cursormysql.execute("SELECT DISTINCT id_camp FROM items WHERE tip LIKE 'M'")
data = cursormysql.fetchall()

# cursormysql.execute("SELECT DISTINCT id FROM items WHERE tip LIKE 'M'")
# data = cursormysql.fetchall()

print('=========================================================================')

print('COMPARING.......')
# Buscaremos todos los certificados si se han recivido datos SQLServer 

keys = {
    '1': 'N1',
    '2': 'N2',
    '2*': 'N2A',
    '5': 'N5',
    '10': 'N10',
    '20': 'N20',
    '20*': 'N20A',
    '50': 'N50',
    '100': 'N100',
    '200': 'N200',
    '200*': 'N200A',
    '500': 'N500',
    '1000': 'N1000',
    '2000': 'N2000',
    '2000*': 'N2000A',
    '5000': 'N5000',
    '10000': 'N10000',
    '20000': 'N20000',
    '200000': 'N200000',
    '500000': 'N500000',
    '1000000': 'N1000000'
}
vals = [
    '1',
    '2',
    '2*',
    '5',
    '10',
    '20',
    '20*',
    '50',
    '100',
    '200',
    '200*',
    '500',
    '1000',
    '2000',
    '2000*',
    '5000',
    '10000',
    '20000',
    '200000',
    '500000',
    '1000000'
]
for item in data:
    if(keys.get(item[0], False)):
        cursormysql.execute(f"UPDATE items SET id_camp = '{keys[item[0]]}' WHERE id_camp LIKE '{item[0]}'")
        MySQLConnection.commit()
    
    # print(random.choice(vals))
    # cursormysql.execute(f"UPDATE items SET id_camp = '{random.choice(vals)}' WHERE id LIKE {item[0]}")
    # MySQLConnection.commit()
    
    # print (f"{item[0]} ==> {keys[item[0]]}" )
    # # print(f"==> Updating item '{item[1]}'")
    # newnom = item[1].replace(" ","")

    # if item[4] == 'kg':
    #     querry = f"UPDATE items SET nom = '{newnom}' uni = 'g', mass_nm = {item[5] * 1000} WHERE id LIKE {item[0]}"
        
    # else :
    #     querry = f"UPDATE items SET nom = '{newnom}' WHERE id LIKE {item[0]}" 

    # if item[1][-2:] == '0*' or item[1][-3:] == '***' or item[1][-1] == 'B' or item[1][-1] == 'D':
    #     index = querry.find('nom')
    #     querry = querry[:index] + f"id_camp = '{item[2]}*', " + querry[index:]

    # try:
    #     cursormysql.execute(querry)
    #     MySQLConnection.commit()
    #     print(f"        Item '{item[1]}' updated success data: id> {item[0]} querry='{querry}'")
    # except:
    #     print(f"the id: {item[0]} item: '{item[1]}' has a problem to updated")
print("DATA MIGRATION COMPLETED SUCCESSFULLY")


MySQLConnection.close()

