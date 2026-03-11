#app.py
from flask import Flask, request, jsonify
from pymongo import MongoClient, WriteConcern, ReadPreference

app = Flask(__name__)
#connect to the mongo cluster
mongo_uri = "mongodb+srv://lab6user:user1234@cs498-hw3.0yt0qmw.mongodb.net/?appName=cs498-hw3"
client = MongoClient(mongo_uri)
#database being used and collection
database = client["ev_db"]
collection = database["vehicles"]

#1. fast but unsafe write
#endpoing it POST/insert-fast
#primary node only
@app.route("/insert-fast", methods=["POST"])
def insert_fast():
  #take the request from the json
  data_req = request.json
  #is fast so w=1 with primary node
  w_1_collect = collection.with_options(write_concern=WriteConcern(w=1))
  #insert into database
  final = w_1_collect.insert_one(data_req)
  return jsonify({"inserted_id": str(final.inserted_id)})


#2. highly durable write
#POST / insert-safe
#ensure data is written to the majoirity
@app.route("/insert-safe", methods=["POST"])
def insert_safe():
  data_req = request.json
  w_maj_collection = collection.with_options(write_concern=WriteConcern(w="majority"))
  final = w_maj_collection.insert_one(data_req)
  return jsonify({"inserted_id": str(final.inserted_id)})

#3. strongly consistent read
#GET / count-tesla-primary
#read is primary
@app.route("/count-tesla-primary", methods=["GET"])
def read_primary():
  read_primary_collection = collection.with_options(read_preference=ReadPreference.PRIMARY)
  counts_found = read_primary_collection.count_documents({"Make": "TESLA"})
  return jsonify({"count": counts_found})

#4. Eventually consistent analytical read
#GET / count-bmw-secondary
#read prefencer to secondary node
@app.route("/count-bmw-secondary", methods=["GET"])
def read_secondary():
  read_sec_collection = collection.with_options(read_preference=ReadPreference.SECONDARY)
  counts_found = read_sec_collection.count_documents({"Make": "BMW"})
  return jsonify({"count": counts_found})

if __name__ == "__main__":
  app.run(host="0.0.0.0", port=8080)

