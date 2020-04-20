from flask_json_schema import JsonSchema, JsonValidationError  # adding JSON schema
from flask import Flask, jsonify, request  # adding flask to project

app = Flask(__name__)  # initializing flask
schema = JsonSchema(app)  # initialing jsonSchema

# example JSON schema
todo_schema = {
    'required': ['list'],
    'properties': {
        'list': {
            'todo': {'type': 'string'},
            'priority': {'type': 'integer'},
        }

    }
}

todos = []

# validating Schema
@app.errorhandler(JsonValidationError)
def validation_error(e):
    return jsonify({'error': e.message, 'errors': [validation_error.message for validation_error in e.errors]})


@app.route('/todo', methods=['GET', 'POST'])  # routes
@schema.validate(todo_schema)
def create_message():
    if request.method == 'POST':
        todos.append(request.get_json())
        print(todos)
        return jsonify({'success': True, 'message': 'Created todo'})

    return jsonify(todos)


if __name__ == "__main__":
    app.run(debug=True)
