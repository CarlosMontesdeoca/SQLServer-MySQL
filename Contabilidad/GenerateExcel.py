import openpyxl
from openpyxl.styles import Protection
import pymysql
import pymongo
import sys
import os

year = sys.argv[1]
month = sys.argv[2]

if int(month) == 1 :
    year_filter = int(year) - 1
    month_filter = 12
else :
    year_filter = year
    month_filter = int(month) - 1

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
    cursormysql.execute(f"""SELECT C.dsc, COUNT(CE.id) FROM calibrates C
    JOIN certificates CE ON CE.calibrate_id = C.id
    WHERE C.order_id LIKE {order_id}
    AND C.metrologist_id LIKE {met_id}
    AND CE.est NOT IN ('C','D');""")
    disc_list = cursormysql.fetchall()
    # print(disc_list)
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
            WHERE YEAR(O.fecRegPag) = {year_filter}
            AND MONTH(O.fecRegPag) = {month_filter}
            AND O.est = 'A'
            AND C.metrologist_id = {metrologist[0]}
            AND S.com = true;""")
        comissions = cursormysql.fetchall()
        
        for commision in comissions:
            if commision[0] not in temp:
                quote = cursormongo.find_one({"N_offert": commision[0]})
                temp[commision[0]] = [quote['disc'], quote['services']]
                
            result = next(item for item in temp[commision[0]][1] if item['service_id'] == commision[3])
            cost = result['cant'] * result['cost'] * (( 100 - temp[commision[0]][0]) / 100) * commision[1] / 100

            if commision[5]:
                tip = 'C'
                cals += cost
                t_cals += cost
                disc += getDisc(commision[4], metrologist[0]) * cost * commision[1] / 10000
            elif commision[6] == 'SOFTWARE':
                tip = 'S'
                soft += cost
                t_soft += cost
            else:
                tip = 'O'
                serv += cost
                t_serv += cost
            
            print(f"OFERTA_{tip}: {commision[0]}| Descuento: {temp[commision[0]][0]}| Participacion: {commision[1]}| Cantidad {result['cant']}|Costo {result['cost']}| Multa {disc} == {result['service_id']}")
            # tols += cost * 0.1
        # print(temp)
        print(f'TOTAL: C {cals} || S {soft} || O {serv}')
        print('==============================================================')
        print(serv,cals,soft)
        print('II ====', metrologist[7])
        sheet[f"B{start}"] = metrologist[1]
        sheet[f"C{start}"] = f"{metrologist[7]}%"
        sheet[f"D{start}"] = serv * 0.9
        sheet[f"F{start}"] = disc
        sheet[f"G{start}"] = cals * 0.7
        sheet[f"I{start}"] = soft * 0.9
        start += 1
    if cos_prec:
        sheet[f"D{i_prec}"] = t_serv * 0.1
        sheet[f"G{i_prec}"] = t_cals * 0.3
        sheet[f"I{i_prec}"] = t_soft * 0.1
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

path = f"C:/archivos_contabilidad/Comisiones/{year}/"
doc = f"Reporte_{year}_{month}"
os.makedirs(path, exist_ok=True)

work_sheet.save(f"{path}{doc}.xlsx")
