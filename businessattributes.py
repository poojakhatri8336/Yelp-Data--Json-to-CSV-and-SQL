import json
import csv
import pymysql


with open('yelp_academic_dataset_business.json',encoding="utf8") as f:
    data = [json.loads(line) for line in f]



totalrecords = len(data)
csv_file = open('json_attributes.csv', 'w', newline='')
csv_writer = csv.writer(csv_file)

mydb = pymysql.connect(host="localhost", user="root", password="psspl12345", database="jsondata")
mycursor = mydb.cursor()
mycursor.execute(
    'CREATE TABLE IF NOT EXISTS business_attributes(Business_Id VARCHAR(50) ,Attribute_Category VARCHAR(100),Attribute VARCHAR(100) NULL,Attribute_Value VARCHAR(100) NULL,FOREIGN KEY(Business_Id) REFERENCES jsondata.business_master(Business_Id))')

for record in range(totalrecords):
    has_items = bool(data[record]['attributes'])
    if has_items == True:
        for k,v in data[record]['attributes'].items():
            if k == "Ambience":
                ambiencestring = data[record]['attributes']['Ambience']
                ambiencedict = eval(ambiencestring)

                if not bool(ambiencedict):
                    csv_writer.writerow([data[record]['business_id'], 'Ambience', "null"])

                    query = u'INSERT INTO business_attributes(Business_Id,Attribute_Category,Attribute,Attribute_Value) VALUES (%s,%s,%s,%s)'
                    values = data[record]['business_id'], 'Ambience', "null", "null"
                    mycursor.execute(query, values)
                    mydb.commit()

                else:
                    for key, val in ambiencedict.items():
                        csv_writer.writerow([data[record]['business_id'], 'Ambience', key, val])

                        query = u'INSERT INTO business_attributes(Business_Id,Attribute_Category,Attribute,Attribute_Value) VALUES (%s,%s,%s,%s)'
                        values = data[record]['business_id'], 'Ambience', key, val
                        mycursor.execute(query, values)
                        mydb.commit()

            elif k == "GoodForMeal":
                GoodForMealstring = data[record]['attributes']['GoodForMeal']
                GoodForMealdict = eval(GoodForMealstring)
                if not bool(GoodForMealdict):
                    csv_writer.writerow([data[record]['business_id'], 'GoodForMeal',"null"])

                    query = u'INSERT INTO business_attributes(Business_Id,Attribute_Category,Attribute,Attribute_Value) VALUES (%s,%s,%s,%s)'
                    values = data[record]['business_id'], 'GoodForMeal', "null", "null"
                    mycursor.execute(query, values)
                    mydb.commit()
                else:
                    for key, val in GoodForMealdict.items():
                        csv_writer.writerow([data[record]['business_id'], 'GoodForMeal', key, val])

                        query = u'INSERT INTO business_attributes(Business_Id,Attribute_Category,Attribute,Attribute_Value) VALUES (%s,%s,%s,%s)'
                        values = data[record]['business_id'], 'GoodForMeal', key, val
                        mycursor.execute(query, values)
                        mydb.commit()

            elif k == "BusinessParking":
                businessparkingstring = data[record]['attributes']['BusinessParking']
                businessparkingdict = eval(businessparkingstring)
                if not bool(businessparkingdict):
                    csv_writer.writerow([data[record]['business_id'], 'BusinessParking',"null"])

                    query = u'INSERT INTO business_attributes(Business_Id,Attribute_Category,Attribute,Attribute_Value) VALUES (%s,%s,%s,%s)'
                    values = data[record]['business_id'], 'BusinessParking', "null", "null"
                    mycursor.execute(query, values)
                    mydb.commit()
                else:
                    for key, val in businessparkingdict.items():
                        csv_writer.writerow([data[record]['business_id'], 'BusinessParking', key, val])

                        query = u'INSERT INTO business_attributes(Business_Id,Attribute_Category,Attribute,Attribute_Value) VALUES (%s,%s,%s,%s)'
                        values = data[record]['business_id'], 'BusinessParking', key, val
                        mycursor.execute(query, values)
                        mydb.commit()

            elif k == "Music":
                Musicstring = data[record]['attributes']['Music']
                Musicdict = eval(Musicstring)
                if not bool(Musicdict):
                    csv_writer.writerow([data[record]['business_id'], 'Music',"null"])

                    query = u'INSERT INTO business_attributes(Business_Id,Attribute_Category,Attribute,Attribute_Value) VALUES (%s,%s,%s,%s)'
                    values = data[record]['business_id'], 'Music', "null", "null"
                    mycursor.execute(query, values)
                    mydb.commit()
                else:
                    for key, val in Musicdict.items():
                        csv_writer.writerow([data[record]['business_id'], 'Music', key, val])

                        query = u'INSERT INTO business_attributes(Business_Id,Attribute_Category,Attribute,Attribute_Value) VALUES (%s,%s,%s,%s)'
                        values = data[record]['business_id'], 'Music', key, val
                        mycursor.execute(query, values)
                        mydb.commit()

            elif k == "BestNights":
                BestNightsstring = data[record]['attributes']['BestNights']
                BestNightsdict = eval(BestNightsstring)
                if not bool(BestNightsdict):
                    csv_writer.writerow([data[record]['business_id'], 'BestNights',"null"])

                    query = u'INSERT INTO business_attributes(Business_Id,Attribute_Category,Attribute,Attribute_Value) VALUES (%s,%s,%s,%s)'
                    values = data[record]['business_id'], 'BestNights', "null", "null"
                    mycursor.execute(query, values)
                    mydb.commit()
                else:
                    for key, val in BestNightsdict.items():
                        csv_writer.writerow([data[record]['business_id'], 'BestNights', key, val])

                        query = u'INSERT INTO business_attributes(Business_Id,Attribute_Category,Attribute,Attribute_Value) VALUES (%s,%s,%s,%s)'
                        values = data[record]['business_id'], 'BestNights', key, val
                        mycursor.execute(query, values)
                        mydb.commit()

            elif k == "DietaryRestrictions":
                DietaryRestrictionsstring = data[record]['attributes']['DietaryRestrictions']
                DietaryRestrictionsdict = eval(DietaryRestrictionsstring)
                if not bool(DietaryRestrictionsdict):
                    csv_writer.writerow([data[record]['business_id'], 'DietaryRestrictions',"null"])

                    query = u'INSERT INTO business_attributes(Business_Id,Attribute_Category,Attribute,Attribute_Value) VALUES (%s,%s,%s,%s)'
                    values = data[record]['business_id'], 'DietaryRestrictions', "null", "null"
                    mycursor.execute(query, values)
                    mydb.commit()
                else:
                    for key, val in DietaryRestrictionsdict.items():
                        csv_writer.writerow([data[record]['business_id'], 'DietaryRestrictions', key, val])

                        query = u'INSERT INTO business_attributes(Business_Id,Attribute_Category,Attribute,Attribute_Value) VALUES (%s,%s,%s,%s)'
                        values = data[record]['business_id'], 'DietaryRestrictions', key, val
                        mycursor.execute(query, values)
                        mydb.commit()

            elif k == "HairSpecializesIn":
                HairSpecializesInstring = data[record]['attributes']['HairSpecializesIn']
                HairSpecializesIndict = eval(HairSpecializesInstring)
                if not bool(HairSpecializesIndict):
                    csv_writer.writerow([data[record]['business_id'], 'HairSpecializesIn',"null"])

                    query = u'INSERT INTO business_attributes(Business_Id,Attribute_Category,Attribute,Attribute_Value) VALUES (%s,%s,%s,%s)'
                    values = data[record]['business_id'], 'HairSpecializesIn', "null", "null"
                    mycursor.execute(query, values)
                    mydb.commit()
                else:
                    for key, val in HairSpecializesIndict.items():
                        csv_writer.writerow([data[record]['business_id'], 'HairSpecializesIn', key, val])

                        query = u'INSERT INTO business_attributes(Business_Id,Attribute_Category,Attribute,Attribute_Value) VALUES (%s,%s,%s,%s)'
                        values = data[record]['business_id'], 'HairSpecializesIn', key, val
                        mycursor.execute(query, values)
                        mydb.commit()

            else:
                csv_writer.writerow([data[record]['business_id'], "Others" ,k, v])

                query = u'INSERT INTO business_attributes(Business_Id,Attribute_Category,Attribute,Attribute_Value) VALUES (%s,%s,%s,%s)'
                values = data[record]['business_id'], 'Others', k, v
                mycursor.execute(query, values)
                mydb.commit()

    else:
            csv_writer.writerow([data[record]['business_id'], "null"])

            query = u'INSERT INTO business_attributes(Business_Id,Attribute_Category,Attribute,Attribute_Value) VALUES (%s,%s,%s,%s)'
            values = data[record]['business_id'], 'null', 'null','null'
            mycursor.execute(query, values)
            mydb.commit()

csv_file.close()
mycursor.close()







