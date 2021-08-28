import json
import csv
import pymysql
from time import time

with open('yelp_academic_dataset_business.json',encoding="utf8") as f:
    data = [json.loads(line) for line in f]

    csv_file = open('json_businessmaster.csv', 'w', newline='',encoding="utf-8")
    csv_writer = csv.writer(csv_file)

    mydb = pymysql.connect(host="localhost", user="root", password="psspl12345", database="jsondata")
    mycursor = mydb.cursor()
    mycursor.execute(
        'CREATE TABLE IF NOT EXISTS business_master(Business_Id VARCHAR(50) NOT NULL PRIMARY KEY,Business_Name VARCHAR(250),Address VARCHAR(250),City VARCHAR(50),State VARCHAR(50),Postalcode VARCHAR(50),Latitude DECIMAL(9,2),Longitude DECIMAL(9,2), Stars FLOAT,Review_count INT ,Is_open INT)')



    totalrecords = len(data)
    print(totalrecords)
    #Fetch data hours data and store it in lists
    for record in range(totalrecords):
        csv_writer.writerow([data[record]['business_id'],data[record]['name'],data[record]['address'],data[record]['city'],data[record]['state'],data[record]['postal_code'],data[record]['latitude'],data[record]['longitude'],data[record]['stars'],data[record]['review_count'],data[record]['is_open']])

        query = u'INSERT INTO business_master(Business_Id,Business_Name,Address,City,State,Postalcode,Latitude,Longitude,Stars,Review_count,Is_open) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        values = data[record]['business_id'],data[record]['name'],data[record]['address'],data[record]['city'],data[record]['state'],data[record]['postal_code'],data[record]['latitude'],data[record]['longitude'],data[record]['stars'],data[record]['review_count'],data[record]['is_open']
        mycursor.execute(query, values)
        mydb.commit()

    csv_file.close()
    mycursor.close()