from pymongo import MongoClient
from IPython.display import clear_output
import pprint
client = MongoClient(
    "mongodb+srv://arpit:iluvmuma@arpit-xcm5n.gcp.mongodb.net/test?retryWrites=true&w=majority")

pipeline = [
    {
        '$limit': 20
    },
    {
        '$addFields': {
            'lastupdated': {
                '$arrayElemAt': [
                    {'$split': ["$lastupdated", "."]},
                    0
                ]
            }
        }
    },
    # {
    #     '$facet': {
    #         'most used language': [{'$limit': 10}],
    #         'top5': [{'$bucketAuto': {
    #             'groupBy': '$count',
    #             'buckets': 5,
    #             'output': {
    #                 'language combination': {'$sum': 1},
    #                 "titles": {'$push': "$year"}
    #             }
    #         }}]
    #     }
    # }
    {
        '$project': {
            '_id': 1,
            'title': 1,
            'year': 1,
            'released': {
                '$cond': {
                    'if': {'$ne': ["$released", ""]},
                    'then':{
                        '$dateFromString': {
                            'dateString': "$released"
                        }
                    },
                    'else': ""
                }
            },
            'runtime': 1,
            'rated': "$rating",
            'country': 1,
            'directors': {'$split': ["$director", ","]},
            'genres': {'$split': ["$genre", ","]},
            'casts': {'$split': ["$cast", ","]},
            'languages': {'$split': ["$language", ","]},
            'plot':1,
            'fullPlot':"$fullplot",
            'metacritic':1,
            'imdb':{
                'id': "$imdbID",
                'rating': "$imdbRating",
                'votes': "$imdbVotes"
            },
            'lastUpdated': {
                '$cond': {
                    'if': {'$ne': ["$lastupdated", ""]},
                    'then':{
                        '$dateFromString': {
                            'dateString': "$lastupdated",
                            'timezone': "America/New_York"
                        }
                    },
                    'else': ""
                }
            }
        }
    },
    {
        '$out': "movies_scratch"
    }
]
clear_output()
pprint.pprint(list(client.users.movies_initial.aggregate(pipeline)))
# db = client.users
# print(db)
