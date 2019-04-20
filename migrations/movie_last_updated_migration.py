import configparser
import os

import dateutil.parser as parser
from bson import ObjectId
from pymongo import MongoClient, UpdateOne
from pymongo.errors import InvalidOperation

"""
Update all the documents in the `movies` collection, such that the "lastupdated"
field is stored as an ISODate() rather than a string.
"""
config = configparser.ConfigParser()
# meh...
config.read(os.path.abspath(os.path.join(os.path.dirname( __file__ ), '../.ini')))

host = config['PROD']['MFLIX_DB_URI']
mflix = MongoClient(host)["mflix"]

predicate = {"lastupdated": {"$type": "string", "$exists": "true"}}
projection = {"lastupdated": 1}

cursor = mflix.movies.find(predicate, projection)

# this will transform the "lastupdated" field to an ISODate() from a string
movies_to_migrate = []
for doc in cursor:
    doc_id = doc.get('_id')
    lastupdated = doc.get('lastupdated', None)
    movies_to_migrate.append(
        {
            "doc_id": ObjectId(doc_id),
            "lastupdated": parser.parse(lastupdated)
        }
    )

print(f"{len(movies_to_migrate)} documents to migrate")

try:
    bulk_updates = [UpdateOne(
        {
            "_id": movie.get("doc_id")
        },
        {
            "$set": {
                "lastupdated": movie.get("lastupdated")
            }
        }
    ) for movie in movies_to_migrate]

    # here's where the bulk operation is sent to MongoDB
    bulk_results = mflix.movies.bulk_write(bulk_updates)
    print(f"{bulk_results.modified_count} documents updated")

except InvalidOperation:
    print("no updates necessary")
except Exception as e:
    print(str(e))
