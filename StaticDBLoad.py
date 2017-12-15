import xml.etree.ElementTree as ET
import MySQLdb as MyDB
import glob

# Get the name of the most upto date reference file
fpath = glob.glob('//192.168.1.3/trainDog/reference/*_ref_*.xml')
print("File being used is: {}".format(fpath[0]))
# setting up the parsing of the xml file
tree = ET.parse(fpath[0])
root = tree.getroot()

# setting up database connection on chef
db = MyDB.connect(host="192.168.1.3", user="root", passwd="password", db="traindog")
cursor = db.cursor()

# purging data from database
tables = ['location', 'lateReason', 'via', 'cancelledReason', 'CISSource', 'operator']
for table in tables:
    sql = "TRUNCATE TABLE " + table
    cursor.execute(sql)
    db.commit()
print("Database tables purged")
# below blocks read relevant data from the xml into correct tables in db

# location
sql = "insert into location VALUES "
for child in root:
    if child.tag == '{http://www.thalesgroup.com/rtti/XmlRefData/v3}LocationRef':
        attribs = child.attrib
        sql += "('" + attribs.get('tpl') + "',null,null,'"
        sql += attribs.get('locname').replace("'", "''") + "'),"
sql = sql[:-1]
number_of_rows = cursor.execute(sql)
db.commit()
print("{} locations added to the db".format(number_of_rows))

# late reasons
sql = "insert into lateReason VALUES "
for child in root:
    if child.tag == '{http://www.thalesgroup.com/rtti/XmlRefData/v3}LateRunningReasons':
        for child2 in child:
            attribs = child2.attrib
            sql += "('" + attribs.get('code') + "','"
            sql += attribs.get('reasontext').replace("'", "''") + "'),"
sql = sql[:-1]
number_of_rows = cursor.execute(sql)
db.commit()
print("{} late reasions added to the db".format(number_of_rows))

# cancelled reason
sql = "insert into cancelledReason VALUES "
for child in root:
    if child.tag == '{http://www.thalesgroup.com/rtti/XmlRefData/v3}CancellationReasons':
        for child2 in child:
            attribs = child2.attrib
            sql += "('" + attribs.get('code') + "','"
            sql += attribs.get('reasontext').replace("'", "''") + "'),"
sql = sql[:-1]
number_of_rows = cursor.execute(sql)
db.commit()
print("{} cancelled reasons added to the db".format(number_of_rows))

# via
sql = "insert into via VALUES "
for child in root:
    if child.tag == '{http://www.thalesgroup.com/rtti/XmlRefData/v3}Via':
        attribs = child.attrib
        sql += "('" + attribs.get('at') + "','"
        sql += attribs.get('dest') + "','"
        sql += attribs.get('loc1') + "','"
        sql += attribs.get('viatext').replace("'", "''") + "'),"
sql = sql[:-1]
number_of_rows = cursor.execute(sql)
db.commit()
print("{} vias added to the db".format(number_of_rows))

# cis
sql = "insert into CISSource VALUES "
for child in root:
    if child.tag == '{http://www.thalesgroup.com/rtti/XmlRefData/v3}CISSource':
        attribs = child.attrib
        sql += "('" + attribs.get('code') + "','"
        sql += attribs.get('name').replace("'", "''") + "'),"
sql = sql[:-1]
number_of_rows = cursor.execute(sql)
db.commit()
print("{} CIS source added to the db".format(number_of_rows))

# operator
sql = "insert into operator VALUES "
for child in root:
    if child.tag == '{http://www.thalesgroup.com/rtti/XmlRefData/v3}TocRef':
        attribs = child.attrib
        sql += "('" + attribs.get('toc') + "','"
        sql += attribs.get('tocname') + "','"
        sql += attribs.get('url').replace("'", "''") + "'),"
sql = sql[:-1]
number_of_rows = cursor.execute(sql)
db.commit()
print("{} operators added to the db".format(number_of_rows))

db.close()
