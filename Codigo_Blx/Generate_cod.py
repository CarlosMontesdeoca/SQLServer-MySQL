## script para generacion de codigo unico en formato Hexadecimal para equipos de clientes

import pymysql

def get_hexadecimal(value):
    numbers = "0123456789ABCDEF"
    return numbers[value]

def generate_cod(number):
    cod = ""
    while(number > 0):
        # print (number)
        index = number % 16
        aux = get_hexadecimal(index)
        cod = aux + cod
        number = int(number / 16)
    return cod
try:
    MySQLConnection = pymysql.connect(host="127.0.0.1",user="root",passwd="",database="pruebas" )
    print (" ==> CONNECCTION SUCCESS WITH MYSQL")
except:
    print ('error to try connect the database MySQL')


print('=========================================================================')
# CONSULTAS

## CUSORES DE CONSULTAS 
cursormysql = MySQLConnection.cursor()

print('GETING BALANCES DATA FROM DATA BASE....')
## busca todos los proyectos pendientes de MySQL
cursormysql.execute("SELECT id FROM balances")
data = cursormysql.fetchall()
print(data, len(data))

for blx in data:
    print (generate_cod(blx[0]))

# num = 4530
# hex = generate_cod(num)

print('=========================================================================')