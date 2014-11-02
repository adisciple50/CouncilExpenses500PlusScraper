__author__ = 'Jason'

import csv
import pymysql as MySQLdb

"""
mydb = MySQLdb.connect(host='178.62.105.36',
    user='root',
    passwd='acronamy',
    db='CouncilExpenses500Plus')
cursor = mydb.cursor()
"""
#https://data.bathhacked.org/api/views/cimd-yfzu/rows.csv?accessType=DOWNLOAD

csv_data = csv.reader(open('Expenses.csv'))
for row in csv_data:
    print(row)
    """    cursor.execute('INSERT INTO testcsv(names, \
              classes, mark )' \
              'VALUES("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")',
              row)
    #close the connection to the database.
    cursor.close()"""
print("Done")