import pymongo
import pprint
free_tier_client = pymongo.MongoClient(MONGO_URL)
people = free_tier_client.cleansing["people-raw"]


def distilled_explain(explain_output):
    return {
        'executionTimeMillis': explain_output['executionStats']['executionTimeMillis'],
        'totalDocsExamined': explain_output['executionStats']['totalDocsExamined'],
        'nReturned': explain_output['executionStats']['nReturned']
    }


query_1_stats = people.find({
    "address.state": "Nebraska",
    "last_name": "Miller",
}).explain()

query_2_stats = people.find({
    "first_name": "Harry",
    "last_name": "Reed"
}).explain()

print(distilled_explain(query_1_stats))
print(distilled_explain(query_2_stats))
