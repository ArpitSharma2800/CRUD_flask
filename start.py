from pymongo import MongoClient
from IPython.display import clear_output
import pprint
client = MongoClient(
    "mongodb+srv://arpit:iluvmuma@arpit-xcm5n.gcp.mongodb.net/test?retryWrites=true&w=majority")

pipeline = [
    {
        '$sortByCount': '$language'
    },
    {
        '$facet': {
            'most used language': [{'$limit': 10}],
            'top5': [{'$bucketAuto': {
                'groupBy': '$count',
                'buckets': 5,
                'output': {
                    'language combination': {'$sum': 1},
                    "titles": {'$push': "$year"}
                }
            }}]
        }
    }
]
clear_output()
pprint.pprint(list(client.users.movies_initial.aggregate(pipeline)))
# db = client.users
# print(db)
