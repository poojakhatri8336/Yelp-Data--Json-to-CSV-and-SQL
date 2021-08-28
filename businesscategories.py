import json
import csv
import pymysql
from time import time

with open('yelp_academic_dataset_business.json',encoding="utf8") as f:
    data = [json.loads(line) for line in f]

listofcategories = []
totalrecords = len(data)
csv_file = open('json_categories.csv', 'w', newline='')
csv_writer = csv.writer(csv_file)

mydb = pymysql.connect(host="localhost", user="root", password="psspl12345", database="jsondata")
mycursor = mydb.cursor()
mycursor.execute(
    'CREATE TABLE IF NOT EXISTS business_categories(Business_Id VARCHAR(50) ,categories VARCHAR(100),FOREIGN KEY(Business_Id) REFERENCES jsondata.business_master(Business_Id))')

for record in range(totalrecords):
    categories = data[record]['categories']
    if not bool(categories):
        csv_writer.writerow([data[record]['business_id'], "null"])
    else:
        listofcategories= categories.split(",")
        for i in listofcategories:
            csv_writer.writerow([data[record]['business_id'],i])

            query = u'INSERT INTO business_categories(Business_Id,categories) VALUES (%s,%s)'
            values = data[record]['business_id'],i
            mycursor.execute(query, values)
            mydb.commit()

csv_file.close()
mycursor.close()
