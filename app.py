from flask import Flask, request
from flask_cors import CORS
from jsonschema import validate, ValidationError
from service import file_ingestion, insert_update
from schema import create_db_schema

# Running flask app
app = Flask(__name__)
# Adding cors to flask
CORS(app)


# Ingest File controller
@app.route("/ingest_file", methods=['GET'])
def ingest_file():
    try:
        file_ingest = file_ingestion.LargeFileIngestion()
        result = file_ingest.ingest_all_data()
        return {'Exception': None, 'IsSuccessfullyIngested': result[0], 'TimeTaken': result[1]}
    except Exception as exp:
        return {'Exception': str(exp), 'IsSuccessfullyIngested': False}
    

# Insert Update Schema
iu_schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "data": {
        "type": "array",
        "items": { "$ref": "#/$defs/contain" }
        }
    },
    "$defs": {
        "contain": {
            "type": "object",
            "required": ["name", "sku", "description"],
            "properties": {
                "name": {
                    "type": "string",
                    "description": "name column value"
                },
                "sku": {
                    "type": "string",
                    "description": "sku column value"
                },
                "description": {
                    "type": "string",
                    "description": "description column value"
                }
            }
        }
    }
}
# Insert Update controller
@app.route("/insert_update", methods=['POST'])
def insert_update_func():
    try:
        new_data = request.get_json()
        validate(instance=new_data, schema=iu_schema)
        if 'data' not in new_data:
            raise ValidationError("'data' key not present")
        new_data = new_data['data']
        insert_update_obj = insert_update.InsertUpdateDetails()
        result = insert_update_obj.insert_update_data(new_data)
        result['Exception'] = None
        result['IsSuccessfull'] = True
        return result
    except ValidationError as exp:
        return {'ValidationError': 'Request body should be in the format : {"data": [{"name": "value", "sku": "value", "description": "value"}]} . ' + str(exp.message)}
    except Exception as exp:
        return {'Exception': str(exp), 'IsSuccessfull': False}


# Start the flask api
if __name__ == '__main__':
    # creates db and collections on first run
    create_db_schema.create_db_with_schemas()
    # start
    app.run()
