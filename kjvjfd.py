import dateparser
from pymongo import MongoClient, UpdateOne

USER = 'arpit'
PASSWORD = 'iluvmuma'
DB = 'users'
BATCH_SIZE = 100  # Batch size for batch updating with bulk_write()
count = 0


client = MongoClient(MONGO_URL)

print(client.peopleraw.people.count_documents(
    {'birthday': {'$type': 'string'}}))

people_raw = client.peopleraw.people.find(
    {'birthday': {'$type': 'string'}})

batch_updates = []

for person in people_raw:

    update = {'$set': {'birthday': dateparser.parse(person['birthday'])}}
    batch_updates.append(UpdateOne({'_id': person['_id']}, update=update))
    count += 1

    if count == BATCH_SIZE:

        client.peopleraw.people.bulk_write(batch_updates)
        print(f'Finished updating a batch of {BATCH_SIZE} documents')
        batch_updates = []

if batch_updates:

    client.peopleraw.people.bulk_write(batch_updates)
    print(f'Finished updating a last batch of {len(batch_updates)} documents')

print('Finshed all the updates.')
