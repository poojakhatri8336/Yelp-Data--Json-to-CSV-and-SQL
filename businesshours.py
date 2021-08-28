import json
import csv
import pymysql

listof_businessid=[]
listof_days=[]
listof_opentime=[]
listof_closetime=[]


with open('yelp_academic_dataset_business.json',encoding="utf8") as f:
    data = [json.loads(line) for line in f]

    totalrecords = len(data)

    #Fetch data hours data and store it in lists
    for record in range(totalrecords):
        has_items = bool(data[record]['hours'])
        if has_items == True:
            for i in data[record]['hours']:
                day = i
                open_time =  data[record]['hours'][i].split("-")[0]
                close_time = data[record]['hours'][i].split("-")[1]
                listof_businessid.append(data[record]['business_id'])
                listof_days.append(day)
                listof_opentime.append(open_time)
                listof_closetime.append(close_time)

        else:
            day= "null"
            open_time= "null"
            close_time= "null"
            listof_businessid.append(data[record]['business_id'])
            listof_days.append(day)
            listof_opentime.append(open_time)
            listof_closetime.append(close_time)





    #open csv

    csv_file = open('json_hours.csv', 'w', newline='')
    csv_writer = csv.writer(csv_file)

    #open connection to write to dbms

    mydb = pymysql.connect(host="localhost", user="root", password="psspl12345", database="jsondata")
    mycursor = mydb.cursor()
    mycursor.execute(
                'CREATE TABLE IF NOT EXISTS business_hours(Business_Id VARCHAR(50) ,Day VARCHAR(50),open_time VARCHAR(50),close_time VARCHAR(50), FOREIGN KEY(Business_Id) REFERENCES jsondata.business_master(Business_Id))')

    print(len(listof_businessid))
    for i in range(len(listof_businessid)):
        csv_writer.writerow([listof_businessid[i],listof_days[i],listof_opentime[i],listof_closetime[i]])

        query = u'INSERT INTO business_hours(Business_Id,Day,open_time,close_time) VALUES (%s,%s,%s,%s)'
        values = listof_businessid[i],listof_days[i],listof_opentime[i],listof_closetime[i]
        mycursor.execute(query, values)
        mydb.commit()


    csv_file.close()
    mycursor.close()
















    #data[0]['attributes']['BusinessParking']