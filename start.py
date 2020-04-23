from pymongo import MongoClient
from IPython.display import clear_output
import pprint
client = MongoClient(
    "mongodb+srv://arpit:iluvmuma@arpit-xcm5n.gcp.mongodb.net/test?retryWrites=true&w=majority")

pipeline = [
    {
        '$group': {
            '_id': {"language": "$language"}
        }
    }
]
clear_output()
pprint.pprint(list(client.users.movies_initial.aggregate(pipeline)))
# db = client.users
# print(db)
