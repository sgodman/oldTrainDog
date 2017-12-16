import xml.etree.ElementTree as ET
import MySQLdb as MyDB
import glob

# Get the name of the most upto date reference file
fpath = glob.glob('//192.168.1.3/trainDog/reference/*_ref_*.xml')
fpath2 = glob.glob('//192.168.1.3/trainDog/reference/*.xml')
fpath3 = [x for x in fpath2 if x not in fpath]

# setting up the parsing of the xml file
print("File being used is: {}".format(fpath[0]))
# setting up the parsing of the xml file
tree = ET.parse(fpath3[0])
root = tree.getroot()

# setting up database connection on chef
db = MyDB.connect(host="192.168.1.3", user="root", passwd="password", db="traindog")
cursor = db.cursor()

# purging data from database
tables = ['Route', 'Stops']
for table in tables:
    sql = "TRUNCATE TABLE " + table
    cursor.execute(sql)
    db.commit()
print("Database tables purged")

# below blocks read relevant data from the xml into correct tables in db

# late reasons
sql = "insert into Route VALUES "
sql2 = "insert into Stops VALUES "

for child in root:
    if child.tag == '{http://www.thalesgroup.com/rtti/XmlTimetable/v8}Journey':
        attribs = child.attrib
        sql += "('" + attribs.get('rid') + "',"
        sql += "'" + attribs.get('uid') + "',"
        sql += "'" + attribs.get('trainId') + "',"
        sql += "'" + attribs.get('ssd') + "',"
        sql += "'" + attribs.get('toc') + "',"
        if 'trainCat' in attribs:
            sql += "'" + attribs.get('trainCat') + "'),"
        else:
            sql = sql[:-1]
            sql += ",null),"

        for child2 in child:
            attribs2 = child2.attrib
            sql2 += "('" + child2.tag + "',"
            if 'tpl' in attribs2:
                sql2 += "'" + attribs2.get('tpl') + "',"
            else:
                sql2 = sql2[:-1]
                sql2 += ",null),"
            if 'act' in attribs2:
                sql2 += "'" + attribs2.get('act') + "',"
            else:
                sql2 = sql2[:-1]
                sql2 += ",null),"
            if 'plat' in attribs2:
                sql2 += "'" + attribs2.get('plat') + "',"
            else:
                sql2 = sql2[:-1]
                sql2 += ",null),"
            if 'pta' in attribs2:
                sql2 += "'" + attribs2.get('pta') + "',"
            else:
                sql2 = sql2[:-1]
                sql2 += ",null),"
            if 'ptd' in attribs2:
                sql2 += "'" + attribs2.get('ptd') + "',"
            else:
                sql2 = sql2[:-1]
                sql2 += ",null),"
            if 'wta' in attribs2:
                sql2 += "'" + attribs2.get('wta') + "',"
            else:
               sql2 = sql2[:-1]
               sql2 += ",null),"
            if 'wtd' in attribs2:
                sql2 += "'" + attribs2.get('wtd') + "',"
            else:
                sql2 = sql2[:-1]
                sql2 += ",null),"
            sql2 += "'" + attribs.get('rid') + "'),"

sql = sql[:-1]
sql2 = sql[:-1]
number_of_rows = cursor.execute(sql)
db.commit()
print("{} Routes have been added to DB".format(number_of_rows))

db.close()
