from pymongo import MongoClient
import matplotlib.pyplot as plt
from IPython.display import clear_output
import pprint
client = MongoClient(MONGO_URL)

moviees = client.users.movies_initial.aggregate([
    {"$limit": 20}
])
query = {

    "imdbRating": {"$exists": True},
    "imdbVotes": {"$exists": True}
}
projection = {
    "_id": 0,
    "imdbRating": 1,
    "imdbVotes": 1
}
rm = list(moviees.find(query, projection))
pprint.pprint(rm[0])

imdbRatings = [moviees['imdbRating'] for moviees in rm]
# print(imdbRatings)
imdbVote = [moviees['imdbVotes'] for moviees in rm]
# print(imdbRatings)
plt.clf()
# fig, ax = plt.subplots()
# ax.scatter(imdbRatings, imdbVote, alpha=0.5)
plt.plot(imdbRatings, imdbVote)
plt.title("imdbR vs iimdV")

plt.show()
