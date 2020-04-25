from pymongo import MongoClient  # this lets us connect to MongoDB
import pprint  # this lets us print our MongoDB documents nicely
# the connection uri to our course cluster
client = MongoClient('mongodb://analytics-student:analytics-password@cluster0-shard-00-00-jxeqq.mongodb.net:27017,cluster0-shard-00-01-jxeqq.mongodb.net:27017,cluster0-shard-00-02-jxeqq.mongodb.net:27017/?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin')
# the trips collection on the citibike database
trips = client.citibike.trips
# find all trips between 5 and 10 minutes in duration that start at station 216
query = {"tripduration": {"$gte": 5000, "$lt": 10000}, "start station id": 216}

# only return the bikeid, tripduration, and _id (displayed by default)
projection = {"bikeid": 1, "tripduration": 1}
# print all of the trips
for doc in trips.find(query, projection):
    pprint.pprint(doc)
