import pymongo
from pymongo import MongoClient, UpdateOne
from IPython.display import clear_output
import pprint
import re

client = MongoClient(
    "mongodb+srv://arpit:iluvmuma@arpit-xcm5n.gcp.mongodb.net/test?retryWrites=true&w=majority")

batch = 100
updates = []
count = 0

for movies in client.users.movies.find({}):

    fields_to_set = {}
    fields_to_unset = {}

    for k, v in movies.copy().items():
        if v == "" or v == [""]:
            del movies[k]  # deleting if not exist
            fields_to_unset[k] = ""  # unset the values

    if 'director' in movies:
        fields_to_unset['director'] = ""
        fields_to_set['directors'] = movies['director'].split(", ")
    if 'cast' in movies:
        fields_to_set['cast'] = movies['cast'].split(", ")
    if 'writer' in movies:
        fields_to_unset['writer'] = ""
        fields_to_set['writers'] = movies['writer'].split(", ")
    if 'genre' in movies:
        fields_to_unset['genre'] = ""
        fields_to_set['genres'] = movies['genre'].split(", ")
    if 'country' in movies:
        fields_to_unset['country'] = ""
        fields_to_set['countries'] = movies['country'].split(", ")
    if 'language' in movies:
        fields_to_unset['language'] = ""
        fields_to_set['languages'] = movies['language'].split(", ")
    if 'fullplot' in movies:
        fields_to_unset['fullplot'] = ""
        fields_to_set['fullPlot'] = movies['fullplot']
    if 'rating' in movies:
        fields_to_unset['rating'] = ""
        fields_to_set['rated'] = movies['rating']

    imdb = {}
    if 'imdbID' in movies:
        fields_to_unset['imdbID'] = ""
        imdb['id'] = movies['imdbID']
    if 'imdbRating' in movies:
        fields_to_unset['imdbRating'] = ""
        imdb['rating'] = movies['imdbRating']
    if 'imdbVotes' in movies:
        fields_to_unset['imdbVotes'] = ""
        imdb['votes'] = movies['imdbVotes']
    if imdb:
        fields_to_set['imdb'] = imdb

    update_docs = {}
    if fields_to_set:
        update_docs['$set'] = fields_to_set
    if fields_to_unset:
        update_docs['$unset'] = fields_to_unset
    # pprint.pprint(update_docs)

    updates.append(UpdateOne({'_id': movies['_id']}, update_docs))
    count += 1
    print(count)
    if count == batch:
        client.users.movies.bulk_write(updates)
        updates = []
        count = 0
if updates:
    client.users.movies.bulk_write(updates)
