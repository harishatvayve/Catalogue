import sqlite3
from collections import OrderedDict
import pandas as pd
import csv,xlsxwriter
import datetime

class Catalogue:
    def __init__(self, db_file=None):
        self.conn = None
        if db_file:
            self.connect(db_file)

    def connect(self, db_file):
        self.conn = sqlite3.connect(db_file)
        self.conn.execute("PRAGMA foreign_keys = ON")

    def close(self):
        if self.conn:
            self.conn.close()

    def view_parts(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM CatalogueParts")
        all_parts = cursor.fetchall()
        columns = [column[0] for column in cursor.description]

        parts_dict = {}
        for part_record in all_parts:
            part_details = dict(zip(columns, part_record))
            part_name = part_details.pop('part_name')  # Use part_name as the key
            parts_dict[part_name] = part_details

        return parts_dict
    
    def drop(self):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM CatalogueParts")

    def fetching_as_dict(self,cursor):
        results=cursor.fetchall()
        
        final_dict = OrderedDict()
        columns = [column[0] for column in cursor.description]
        for part_record in results:
            part_details = dict(zip(columns, part_record))
            part_name = part_details.pop('Part_name')  # Use part_name as the key
            final_dict[part_name] = part_details
        
        
        return final_dict

    def search_part(self, parameter):
        cursor = self.conn.cursor()
        query="""SELECT * FROM CatalogueParts WHERE Part_Name LIKE '%"""
        query+=str(parameter) + "%' "
        cursor.execute(query)
        result=self.fetching_as_dict(cursor)
        return result
        

    def universal_search(self, filters):
        cursor = self.conn.cursor()

        # Base query
        query = "SELECT * FROM CatalogueParts WHERE 1=1"
        parameters = []
        if 'material' in filters:
            query += " AND material = ?"
            parameters.append(filters['material'])
        if 'min_quantity' in filters:
            query += " AND quantity >= ?"
            parameters.append(filters['min_quantity'])

        if 'max_quantity' in filters:
            query += " AND quantity <= ?"
            parameters.append(filters['max_quantity'])

        if 'min_length' in filters:
            query += " AND length >= ?"
            parameters.append(filters['min_length'])

        if 'max_length' in filters:
            query += " AND length <= ?"
            parameters.append(filters['max_length'])

        if 'sort_by' in filters and filters['sort_by']:
            sort_by=filters['sort_by']
            order=filters['sort_order']
            query += f" ORDER BY {sort_by} {order}"

        cursor.execute(query, parameters)
        result=self.fetching_as_dict(cursor)        
        return result                  

    
    def csv_header(self,csv_file):
        with open(csv_file,'r') as file:    
            csv_reader=csv.DictReader(file)
            csv_header=csv_reader.fieldnames
            return csv_header
    Schema_Mapping={
        "Part Name":"Part_name",
        "Quantity": "Quantity",
        "Part Description": "Part_desc",
        "Width": "Width",
        "Length": "Length",
        "Height": "Height",
        "Volume": "Volume",
        "Area": "Area",
        "Mass": "Mass",
        "Density": "Density",
        "Material": "Material"
        
              
    }

    def validate_headers(self, csv_header):
        mapped_headers = []
        missing_headers = set(self.Schema_Mapping.keys()) - set(csv_header)

        if missing_headers:
            missing_headers_str = ", ".join(missing_headers)
            raise ValueError(f"Missing headers: {missing_headers_str}")

        for head in csv_header:
            if head in self.Schema_Mapping:
                mapped_headers.append(self.Schema_Mapping[head])
            else:
                raise ValueError(f"Unknown header '{head}' found in CSV.")

        return mapped_headers
    SCHEMA_TYPES = {
        "Part Name": str,
        "Quantity": int,
        "Part Description": str,
        "Width": float,
        "Length": float,
        "Height": float,
        "Volume": float,
        "Area": float,
        "Mass": float,
        "Density": float,
        "Material": str
        
        
    }

    def convert_data_types_with_column_headers(self,csv_file):
        ###Convertes data types which are required by the db and also converts the column names to db column names
        csv_header=self.csv_header(csv_file)
        df=pd.read_csv(csv_file,dtype=self.SCHEMA_TYPES)
        df.columns=self.validate_headers(csv_header)
        return df
    
    def check_missing_values(self, df):
        missing_values_rows = df[df.isnull().any(axis=1)]
        return missing_values_rows.to_dict('records') if not missing_values_rows.empty else None
    
    def find_conflicts(self, df):
        cursor = self.conn.cursor()
        conflicts = []

        for row in df.itertuples(index=False, name='Row'):
            cursor.execute("SELECT 1 FROM CatalogueParts WHERE Part_name = ?", (row.Part_name,))
            if cursor.fetchone():
                conflicts.append(row._asdict())

        return conflicts
    
    def check_conflict_details(self, conflicts):
        cursor = self.conn.cursor()
        different_conflicts = []


        for row in conflicts:
            cursor.execute("SELECT * FROM CatalogueParts WHERE Part_name = ?", (row['Part_name'],))
            existing_part = cursor.fetchone()
            if existing_part:
                existing_part_dict = {desc[0]: existing_part[i] for i, desc in enumerate(cursor.description) if desc[0] != "Revision"}
                sorted_existing_part_dict= dict(sorted(existing_part_dict.items()))
                sorted_row_dict = dict(sorted(row.items()))
                sorted_row_dict.pop('Revision',None)
                if sorted_existing_part_dict != sorted_row_dict:
                    different_conflicts.append(row)

        return different_conflicts
    
    def upsert_data(self, df):
        try:
            with self.conn:
                cursor = self.conn.cursor()

                for row in df.itertuples(index=False, name='Row'):
                    cursor.execute("""
                        INSERT INTO CatalogueParts (Part_name, Quantity, Part_desc, Width, Length, Height, Volume, Area, Mass, Density, Material)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(Part_name) DO NOTHING
                    """, (row.Part_name, row.Quantity, row.Part_desc, row.Width, row.Length, row.Height, row.Volume, row.Area, row.Mass, row.Density, row.Material))

            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            raise e

    

    def upsert_parts(self, csv_file):
        df = self.convert_data_types_with_column_headers(csv_file)
        missing = self.check_missing_values(df)
        conflicts = self.find_conflicts(df)
        different_conflicts=self.check_conflict_details(conflicts)

        issues = {
            'missing_rows': missing,
            'conflicting_rows': different_conflicts
        }

        if missing or different_conflicts:

            return issues 

        try:
            with self.conn:
                self.upsert_data(df)
            return "All rows inserted successfully"
        except sqlite3.Error as e:
            self.conn.rollback()
            return f"Error occurred: {e}"
        
    def generate_conflict_report(self, csv_file):
        df = self.convert_data_types_with_column_headers(csv_file)
        conflicts = self.find_conflicts(df)
        different_conflicts = self.check_conflict_details(conflicts)
        
        report_rows = []

        # Convert different_conflicts to a set of part names for quick lookup
        different_conflict_part_names = {conflict['Part_name'] for conflict in different_conflicts}

        for index, row in df.iterrows():
            row_dict = row.to_dict()
            part_name = row_dict['Part_name']
            
            if part_name in different_conflict_part_names:
                row_dict['Issue'] = 'Conflict Found'
                row_dict['Action'] = ''
            elif part_name in [conflict['Part_name'] for conflict in conflicts]:
                row_dict['Issue'] = 'Present'
                row_dict['Action'] = 'Ignore'
            else:
                row_dict['Issue'] = 'No error'
                row_dict['Action'] = 'No Action'

            report_rows.append(row_dict)

        report_df = pd.DataFrame(report_rows)
        row_count = len(report_df)
        workbook = xlsxwriter.Workbook('conflict_report.xlsx')
        worksheet = workbook.add_worksheet()
        
        for col_num, col_name in enumerate(report_df.columns):
            worksheet.write(0, col_num, col_name)
        
        for row_num, row in enumerate(report_df.itertuples(index=False)):
            for col_num, value in enumerate(row):
                worksheet.write(row_num + 1, col_num, value)
        
        worksheet.data_validation(f'M2:M{row_count + 1}', {
            'validate': 'list',
            'source': ['Overwrite', 'Update', 'No Action', 'Ignore'],
            'input_title': 'Select an Action',
            'input_message': 'Please Choose one Option'
        })
        
        workbook.close()
        return 'conflict_report.xlsx'
        

    
    def convert_excel_to_csv(self,excel_file, csv_file):
        df = pd.read_excel(excel_file)
        df.to_csv(csv_file, index=False)
        return csv_file
    

    def revised_or_new(self,csv_file):
        df=pd.read_csv(csv_file)
        if 'Action' in df.columns:
            return 'Revised'
        else:
            return 'New'


    def process_actions(self, excel_file):
        csv_file=self.convert_excel_to_csv(excel_file)
        df = pd.read_csv(csv_file)
        
        try:
            cursor = self.conn.cursor()
            self.conn.execute("BEGIN")  # Start a transaction

            for index, row in df.iterrows():
                action = row['Action']
                part_name = row['Part_name']

                if action == 'Update':
                    self.update_part(part_name, row, cursor)
                elif action == 'Overwrite':
                    self.overwrite_part(part_name, row, cursor)
                elif action == 'No Action':
                    self.insert_part(row, cursor)
                elif action == 'Ignore':
                    pass 

            self.conn.commit()  # Commit the transaction if all operations succeed
            return "All rows inserted successfully"
        except Exception as e:
            self.conn.rollback()  # Roll back the transaction if any operation fails
            return f"Error occurred: {e}"
    

    def update_part(self, part_name, row, cursor):
        cursor.execute("""
            UPDATE CatalogueParts
            SET Quantity = ?, Part_desc = ?, Width = ?, Length = ?, Height = ?, Volume = ?, Area = ?, Mass = ?, Density = ?, Material = ?, Revision = Revision + 1
            WHERE Part_name = ?
        """, (
            row['Quantity'], row['Part_desc'], row['Width'], row['Length'], row['Height'], row['Volume'], row['Area'], row['Mass'], row['Density'], row['Material'], part_name
        ))
        self.conn.commit()

    
    def overwrite_part(self,part_name, row, cursor):
        
        cursor.execute("""
            UPDATE CatalogueParts
            SET Quantity = ?, Part_desc = ?, Width = ?, Length = ?, Height = ?, Volume = ?, Area = ?, Mass = ?, Density = ?, Material = ?
            WHERE Part_name = ?
        """, (
            row['Quantity'], row['Part_desc'], row['Width'], row['Length'], row['Height'], row['Volume'], row['Area'], row['Mass'], row['Density'], row['Material'], part_name
        ))
        self.conn.commit()


    def insert_part(self, row,cursor):
        
        cursor.execute("""
            INSERT INTO CatalogueParts (Part_name, Quantity, Part_desc, Width, Length, Height, Volume, Area, Mass, Density, Material)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row['Part_name'], row['Quantity'], row['Part_desc'], row['Width'], row['Length'], row['Height'], row['Volume'], row['Area'], row['Mass'], row['Density'], row['Material']
        ))
        self.conn.commit()


db=Catalogue('Catalogue.db')
print(db.process_actions('conflict_report.csv'))

