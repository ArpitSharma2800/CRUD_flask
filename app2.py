from flask import Flask
from flask_pymongo import PyMongo
from bson.json_util import dumps
from bson.objectid import ObjectId
from flask import jsonify, request
from werkzeug.security import generate_password_hash, check_password_hash
import os

app = Flask(__name__)
app.sceret_key = os.environ['SECERT']
app.config['MONGO_URI'] = os.environ['DB']
mongo = PyMongo(app)


@app.route('/add', methods=['POST'])
def add_user():
    _json = request.json
    _name = _json['name']
    _email = _json['email']
    _gender = _json['gender']
    _password = _json['pwd']
    _category = _json['category']

    if _name and _email and _password and _gender and _category and request.method == 'POST':
        users = mongo.db.user.find_one({'email': _email})
        if(users):
            response = jsonify("email already exist")
            response.status_code = 200
            return response

        _hashed_password = generate_password_hash(_password)
        id = mongo.db.user.insert(
            {'name': _name, "email": _email, 'gender': _gender, 'category': _category, "pwd": _hashed_password})
        response = jsonify("User added successfully")
        response.status_code = 200
        return response
    else:
        return notfound()


@app.route('/update/<id>', methods=['PUT'])
def update_user(id):
    _id = id
    _json = request.json
    _name = _json['name']
    _email = _json['email']
    _password = _json['pwd']

    if _name and _email and _password and _id and request.method == 'PUT':
        _hashed_password = generate_password_hash(_password)
        mongo.db.user.update_one({'_id': ObjectId(_id['$oid']) if '$oid' in _id else ObjectId(
            _id)}, {'$set':  {'name': _name, "email": _email, "pwd": _hashed_password}})
        response = jsonify("User updated successfully")
        response.status_code = 200
        return response
    else:
        return notfound()


@app.route('/users')
def users():
    users = mongo.db.user.find()
    resp = dumps(users)
    return resp


@app.route('/users/<id>')
def user(id):
    users = mongo.db.user.find_one({'_id': ObjectId(id)})
    resp = dumps(users)
    return resp


@app.route('/delete/<id>', methods=['DELETE'])
def delete_user(id):
    mongo.db.user.delete_one({'_id': ObjectId(id)})
    resp = jsonify("user deleted successfully")
    return resp


@app.errorhandler(404)
def notfound(error=None):
    message = {
        "status": 404,
        "message": 'Not found' + request.url
    }
    resp = jsonify(message)
    resp.status_code = 404
    return resp


if __name__ == "__main__":
    app.run(debug=True)
