from marshmallow import Schema, fields, ValidationError

class SearchQuerySchema(Schema):
    query = fields.Str(required=True)

    def validate_query(self, value):
        if not value.isalnum() and '_' not in value:
            raise ValidationError("Invalid characters in search query. Only letters, numbers, and underscores are allowed.")
