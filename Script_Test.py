# CONEXION pyw 
import pymysql
from datetime import date

today = date.today()

try:
    MySQLConnection = pymysql.connect(host="127.0.0.1",user="root",passwd="",database="test" )
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
cursormysql.execute("SELECT id, nom, id_camp, loc, uni, mass_nm FROM items WHERE tip LIKE 'M'")
data1 = cursormysql.fetchall()
# print(data1, len(data1))

print('=========================================================================')

print('COMPARING.......')
# Buscaremos todos los certificados si se han recivido datos SQLServer 
for item in data1:
    # print(f"==> Updating item '{item[1]}'")
    newnom = item[1].replace(" ","")

    if item[4] == 'kg':
        try: 
            querry = f"UPDATE items SET nom = '{newnom}' uni = 'g', mass_nm = {item[5] * 1000} WHERE id LIKE {item[0]}"
            # MySQLConnection.commit()
            # print(f"   ==> Item '{item[1]}' updated success data: id> {item[0]} ")
        except:
            print(f"the id: {item[0]} item: '{item[1]}' has a problem to updated")
    else :
        try: 
            querry = f"UPDATE items SET nom = '{newnom}' WHERE id LIKE {item[0]}" 
            # MySQLConnection.commit()
            # print(f"        Item '{item[1]}' updated success data: id> {item[0]} ")
        except:
            print(f"the id: {item[0]} item: '{item[1]}' has a problem to updated")

    if item[1][-2:] == '0*' or item[1][-3:] == '***' or item[1][-1] == 'B' or item[1][-1] == 'D':
        index = querry.find('nom')
        querry = querry[:index] + f"id_camp = '{item[2]}*' " + querry[index:]

    print(querry)
    # try: 
    #     cursormysql.execute(f"UPDATE items SET uni = 'g', mass_nm = {item[5] * 1000} WHERE id LIKE {item[0]}")
    #     MySQLConnection.commit()
    #     print(f"        Item '{item[1]}' updated success data: id> {item[0]} ")
    # except:
    #     print(f"the id: {item[0]} item: '{item[1]}' has a problem to updated")
print("DATA MIGRATION COMPLETED SUCCESSFULLY")


MySQLConnection.close()

