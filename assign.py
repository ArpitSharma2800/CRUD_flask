from pymongo import MongoClient
import pprint
# We're just reading data, so we can use the course cluster
client = MongoClient('mongodb://analytics-student:analytics-password@cluster0-shard-00-00-jxeqq.mongodb.net:27017,cluster0-shard-00-01-jxeqq.mongodb.net:27017,cluster0-shard-00-02-jxeqq.mongodb.net:27017/?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin')
# We'll be using two different collections this time around
# client = MongoClient(
# "mongodb+srv://arpit:iluvmuma@arpit-xcm5n.gcp.mongodb.net/peopleraw?retryWrites=true&w=majority")
movies = client.users.movies
surveys = client.results.surveys
pprint.pprint(surveys)

# filteres = {
#     "results":
#     {
#         "$elemMatch":
#         {
#             "score": 6
#         }
#     }
# }
# # projection = {"results": 1}
filteres = {"results.product": "abc"}
survey_filter_doc = surveys.find(
    filteres)
pprint.pprint(survey_filter_doc)
