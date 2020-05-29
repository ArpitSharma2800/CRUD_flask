from pymongo import MongoClient
import pprint
# We're just reading data, so we can use the course cluster
client = MongoClient(MONGO_URL)

pipeline = [
    {
        '$limit': 5
    }
]
movies = client.users.movies.aggregate([
    {"$sort": {"year": -1}},
    {
        '$limit': 20
    },
    {
        "$group": {
            "_id": "_id",
            "title": {
                "$push": {
                    "_title": "$title",
                    "_year": "$year"
                }
            }
        }
    }
])
filters = {"$text":
           {"$search": "titanic"}
           }
movie = client.users.movies.find({"$text": {"$search": "titanic"}})
pprint.pprint(list(movie))
# sorte = movies.sort('year')
# pprint.pprint(list(movie))
