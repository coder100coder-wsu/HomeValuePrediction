
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

mydb = myclient['housing']

col = mydb['houseprices']

x= col.find({},{'_id':0,'city':1})

for data in x:
 print(data)


