import openpyxl
from openpyxl.styles import Protection
import pymysql
import pymongo
import sys


year = sys.argv[1]
month = sys.argv[2]

def connectMySQL():
    try:
        MySQLConnection = pymysql.connect(host="127.0.0.1",user="root",passwd="",database="pruebas" )
        print (" ==> CONNECCTION SUCCESS WITH MYSQL")
        return MySQLConnection.cursor()
    
    except:
        print ('error to try connect the database MySQL')
        return False

def connectMongo():
    try:
        client = pymongo.MongoClient("mongodb://192.168.9.221:27017/")
        db = client["operaciones"]
        collection = db["quoters"]
        print (" ==> CONNECCTION SUCCESS WITH MONGODB")
        return collection
    
    except:
        print ('error to try connect the database MONGO DB')
        return False

def getMetrologists(loc):
    try:
        cursormysql.execute(f"SELECT * FROM metrologists WHERE est LIKE 'A' AND coms > 0 AND loc LIKE '{loc}';")
        metrologistsList = cursormysql.fetchall()
        return metrologistsList
    except Exception as e:
        print(f'ERROR AL EXTRAER LAS FACTURAS: {str(e)}')
        return False
    
def getDisc(order_id, met_id):
    cursormysql.execute(f"""SELECT C.dsc, COUNT(CE.id) FROM orders O
	JOIN calibrates C ON O.id = C.order_id
    JOIN certificates CE ON CE.calibrate_id = C.id
    WHERE O.id LIKE {order_id}
    AND C.metrologist_id LIKE {met_id}
    AND CE.est NOT IN ('C','D');""")
    disc_list = cursormysql.fetchall()
    n, d = 0, 0
    for data in disc_list:
        if data[0]:
            n += data[0] * data[1]
            d += data[1]
    if d == 0 :
        return 0
    else:
        return n / d

def insertInToExcel(start, data, sheet, cos_prec):
    cursormongo = connectMongo()
    i_prec = start - 1
    temp = {}
    t_serv, t_cals, t_soft = 0,0,0
    for metrologist in data:
        cals, serv, soft, disc = 0,0,0,0
        print(metrologist[1])
        cursormysql.execute(f"""SELECT O.N_offert, C.part, SA.cant, SA.service_id, SA.order_id, S.met, S.cat   FROM contributions C
            JOIN services_apr SA ON C.service_apr_id = SA.id
            JOIN orders O ON O.id = SA.order_id
            JOIN services S ON S.id = SA.service_id 
            WHERE YEAR(C.created_at) = {year}
            AND MONTH(C.created_at) = {month}
            AND O.est = 'A'
            AND C.metrologist_id = {metrologist[0]}
            AND S.com = true;""")
        comissions = cursormysql.fetchall()
        
        for commision in comissions:
            if commision[0] not in temp:
                quote = cursormongo.find_one({"N_offert": commision[0]})
                temp[commision[0]] = [quote['disc'], quote['services']]

            result = next(item for item in temp[commision[0]][1] if item['service_id'] == commision[3])
            cost = result['cant'] * result['cost'] * ( 100 - temp[commision[0]][0]) / 100
            # print(commision[0], temp[commision[0]])
            # print(cost)
            if commision[5]:
                cals += cost
                t_cals += cost
                disc += getDisc(commision[4], metrologist[0])
            elif commision[6] == 'SOFTWARE':
                soft += cost
                t_soft += cost
            else:
                serv += cost
                t_serv += cost
            # tols += cost * 0.1
        print('==============================================================')
        print(serv,cals,soft)
        print('II ====', start)
        sheet[f"D{start}"] = metrologist[1]
        sheet[f"E{start}"] = f"{metrologist[7]}%"
        sheet[f"F{start}"] = serv * 0.9
        sheet[f"H{start}"] = disc
        sheet[f"I{start}"] = cals * 0.7
        sheet[f"K{start}"] = soft * 0.9
        start += 1
    if cos_prec:
        sheet[f"F{i_prec}"] = t_serv * 0.1
        sheet[f"I{i_prec}"] = t_cals * 0.3
        sheet[f"K{i_prec}"] = t_soft * 0.1
    # print(temp)
cursormysql = connectMySQL()


#     print(metrologist[1])
work_sheet = openpyxl.load_workbook('Contabilidad/plantilla.xlsx')
sheet = work_sheet.active

sheet.protection.password = "P43C1T401"

## Ingresar tecnicos de Quito
metrologistsList = getMetrologists('UIO')
print('UIO', len(metrologistsList))
insertInToExcel(7, metrologistsList, sheet, True)

## Ingresar tecnicos de Guayaquil
metrologistsList = getMetrologists('GYE')
print('GYE', len(metrologistsList))
insertInToExcel(20, metrologistsList, sheet, True)

## Ingresar tecnicos de Manta
metrologistsList = getMetrologists('MTA')
print('MTA', len(metrologistsList))
insertInToExcel(32, metrologistsList, sheet, False)

for row in sheet.iter_rows():
    for cell in row:
        cell.protection = Protection(locked=True)

work_sheet.save(f"Reporte_{year}_{month}.xlsx")
