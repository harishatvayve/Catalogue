from flask import Flask, jsonify, request
from Catalogue import Catalogue  # Ensure this import path is correct
from marshmallow import Schema, fields, ValidationError,validate
import csv,pandas as pd
import tempfile,os

app = Flask(__name__)
app.json.sort_keys = False
DATABASE = 'Catalogue.db'

class UniversalSearchQuerySchema(Schema):
    material = fields.Str()
    min_quantity = fields.Integer()
    max_quantity = fields.Integer()
    min_length = fields.Float()
    max_length = fields.Float()
    sort_by = fields.String(validate=lambda s: s.strip() if isinstance(s, str) else s, required=False)
    sort_order = fields.Str(validate=validate.OneOf(['asc', 'desc']), required=False)
    def process_filters(self, filters):
        if 'sort_by' in filters:
            filters['sort_by'], filters['sort_order'] = self.extract_sort_details(filters['sort_by'])
        return filters

    def extract_sort_details(self, sort_by):
        if not sort_by:
            return None, None
        sort_by = sort_by.strip()
        if sort_by.lower().endswith(' desc'):
            column_name = sort_by[:-5].strip()
            order = 'DESC'
        elif sort_by.lower().endswith(' asc'):
            column_name = sort_by[:-4].strip()
            order = 'ASC'
        else:
            column_name = sort_by.strip()
            order = 'ASC'
        return column_name, order

class SearchQuerySchema(Schema):
    input = fields.Str(required=True, validate=lambda s: s.isalnum() or '_' in s,
                       error_messages={'validator_failed': 'Query can only contain alphanumeric characters and underscore.'})
    

@app.route('/api/parts', methods=['GET'])
def get_all_parts():
    db = Catalogue(DATABASE)
    parts_dict = db.view_parts()
    db.close()
    return jsonify(parts_dict)

@app.route('/api/search', methods=['GET'])
def search_parts():
    db = Catalogue(DATABASE)
    try:
        parameters= SearchQuerySchema().load({'input': request.args.get('query', '')})
    except ValidationError as err:
        return jsonify({'error': err.messages}), 400
    print(parameters)
    search_dict = db.search_part(parameters['input'])
    db.close()
    return jsonify(search_dict)

@app.route('/api/universal_search', methods=['GET'])
def universal_search_route():
    db = Catalogue(DATABASE)
    try:
        search_params = UniversalSearchQuerySchema().load(request.args)
        search_params = UniversalSearchQuerySchema().process_filters(search_params)

        
    except ValidationError as err:
        return jsonify({'error': err.messages}), 400
    
    search_results= db.universal_search(search_params)
    db.close()
    return jsonify(search_results)



@app.route('/upload', methods=['POST'])
def upload_csv():
    db = Catalogue(DATABASE)
    
    if 'csv_file' not in request.files:
        return jsonify({'error': 'No CSV file provided'}), 400

    csv_file = request.files['csv_file']
    if csv_file.filename == '':
        return jsonify({'error': 'No CSV file selected'}), 400

    if csv_file and csv_file.filename.endswith('.csv'):
        # Save the file to a temporary location
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, csv_file.filename)
        csv_file.save(temp_path)

        try:
            issues = db.upsert_parts(temp_path)
        finally:
            os.remove(temp_path)

        if 'missing_rows' in issues or 'conflicting_rows' in issues:
            return jsonify(issues), 400
        
        return jsonify({'message': 'All rows inserted successfully'}), 200

    else:
        return jsonify({'error': 'Invalid CSV file'}), 400

    db.close()
if __name__ == '__main__':
    app.run(debug=True)
