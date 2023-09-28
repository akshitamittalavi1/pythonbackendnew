import os
import re
import pyodbc
import pandas as pd
import openai
from io import StringIO
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient
from azure_cosmos.cosmos_db import cosmos_pipeline
from azure_blob import blob_storage
from azure_sql import sql_prompt 
from config import BLOB_CONTAINER1, DRIVER


class DataPipeline:
    """
    A class for building a data pipeline for managing data from Azure Blob Storage, SQL Database, and Cosmos DB.
    """

    def __init__(self):
        """
        Initializes the DataPipeline instance.

        Args:
            connection_string_sql (str): The connection string for the SQL Database.
            cosmos_endpoint (str): The endpoint for the Cosmos DB.
            cosmos_key (str): The key for accessing the Cosmos DB.
        """
        self.connection_string_sql = f"""DRIVER={DRIVER};SERVER={os.getenv("dewasql-server")};DATABASE={os.getenv("dewasql-database")};UID={os.getenv("dewasql-username")};PWD={os.getenv("dewasql-password")}"""
        self.cosmos_endpoint = os.getenv("dewacosmosdb-endpoint")
        self.cosmos_key = os.getenv("dewacosmosdb-conn-str")
        self.cosmos_client = CosmosClient(self.cosmos_endpoint, self.cosmos_key)
        self.BLOB_CONN_STRING = os.getenv("dewablob-connection-string")

    def get_data(self, filename):
        """
        Loads the tabular data from Azure Blob Storage for processing.

        Args:
            filename (str): The name of the data file to retrieve.

        Returns:
            bool: True if the data is successfully loaded, False otherwise.
        """
        try:
            blob_service_client = BlobServiceClient.from_connection_string(self.BLOB_CONN_STRING)
            container_client = blob_service_client.get_container_client(BLOB_CONTAINER1)
            blob_client = container_client.get_blob_client(filename)

            with open(filename, "wb") as data:
                data.write(blob_client.download_blob().readall())

            return True
        except Exception as e:
            print(f"Error loading data from Azure Blob Storage: {str(e)}")
            return False

    def insert_data_to_sql(self, file_path, excel_data):
        """
        Inserts data from a file into a SQL Database table.

        Args:
            file_path (str): The path to the file containing the data.

        Returns:
            bool: True if the data is successfully inserted, False otherwise.
        """
        try:
            conn = pyodbc.connect(self.connection_string_sql)
            cursor = conn.cursor()
            sql_table_name, extension = file_path.split(".")

            pattern = r"[^A-Za-z0-9_=+-]"
            sql_table_name = re.sub(pattern, '_', sql_table_name)
            sql_table_name=sql_table_name.lower()

            excel_data.columns = excel_data.columns.str.lower()
            column_names_tuple = tuple(excel_data.columns.tolist())
            column_names = ", ".join([f"{re.sub(pattern, '_', column)}" for column in excel_data.columns])

            columns_type = {}
            for column_name, dtype in excel_data.dtypes.items():
                pattern = r"[^A-Za-z0-9_=+-]"
                column_name = re.sub(pattern, '_', column_name)
                column_name=column_name.lower()
                if dtype == 'int64':
                    columns_type[column_name] = 'INTEGER'
                elif dtype == 'float64':
                    columns_type[column_name] = 'FLOAT'
                elif dtype == 'bool':
                    columns_type[column_name] = 'BOOLEAN'
                elif dtype == 'datetime64[ns]':
                    columns_type[column_name] = 'DATETIME'
                else:
                    columns_type[column_name] = 'VARCHAR(MAX)'

            col_name_type = ', '.join([f"{re.sub(pattern, '_', column_name)} {dtype}" for column_name, dtype in columns_type.items()])

            # Print the table name for debugging

            # Create the SQL table
            create_table_sql = f'''CREATE TABLE {sql_table_name} ({col_name_type});'''
            cursor.execute(create_table_sql)

            for _, row in excel_data.iterrows():
                placeholders = ", ".join(["?" for _ in excel_data.columns])
                sql_insert = f"INSERT INTO {sql_table_name} ({column_names}) VALUES ({placeholders})"
                # values = tuple(row)
                values = []
                for column_name, dtype in excel_data.dtypes.items():
                    value = row[column_name]

                    if dtype == 'int64':
                        values.append(int(value) if not pd.isna(value) else int('nan'))
                    elif dtype == 'float64':
                        values.append(float(value) if not pd.isna(value) else 5.0)
                    elif dtype == 'bool':
                        values.append(bool(value) if not pd.isna(value) else bool('nan'))
                    elif dtype == 'datetime64[ns]':
                        values.append(pd.to_datetime(value) if not pd.isna(value) else None)
                    else:
                        values.append(str(value) if not pd.isna(value) else None)
                cursor.execute(sql_insert, tuple(values))
                conn.commit()
            print('table updated')
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            print(f"Error inserting data into SQL Database: {str(e)}")
            return False

    def answer_from_table(self, query):
        """
        Executes an SQL query on the SQL Database and returns the results.

        Args:
            query (str): The SQL query to execute.

        Returns:
            list: A list of rows as query results.
        """
        try:
            conn = pyodbc.connect(self.connection_string_sql)
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Exception as e:
            print(f"Error executing SQL query: {str(e)}")
            return []

    def drop_table(self, table_name):
        """
        Drops a table from the SQL Database.

        Args:
            table_name (str): The name of the table to drop.

        Returns:
            bool: True if the table is successfully dropped, False otherwise.
        """
        try:
            conn = pyodbc.connect(self.connection_string_sql)
            cursor = conn.cursor()
            delete_table_query = f'DROP TABLE IF EXISTS {table_name};'
            cursor.execute(delete_table_query)
            conn.commit()
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            print(f"Error dropping table from SQL Database: {str(e)}")
            return False

    def update_table_schema(self, user_id, data):
        """
        Updates the schema of a table in Cosmos DB.

        Args:
            table_name (str): The name of the table.
            columns_type (dict): A dictionary of column names and their data types.

        Returns:
            bool: True if the schema is successfully updated, False otherwise.
        """
        try:
            # Insert data into the container
            query = f"SELECT * FROM c WHERE c.id = '{user_id}'"
            # Execute the query to retrieve the document(s) with the matching key
            items = list(self.container.query_items(query, enable_cross_partition_query=True))
            if items:
                # Assuming there's only one matching document (key is unique)
                item = items[0]
                # Extract the associated value
                current_schema = item.get("value")
                for key, value in data.items():
                    current_schema.append(value[0])
                self.container.upsert_item({"id": user_id, "value": current_schema})
                return value            
            else:
                for key, value in data.items():
                    self.container.upsert_item({"id": key, "value": value})
            print('cosmos updated')
            return "successfully updated data on cosmos"
        except Exception as e:
            print(f"Error updating table schema in Cosmos DB: {str(e)}")
            return False

    def get_table_schema_from_cosmos(self, table_names):
        """
        Retrieves the schema(s) of table(s) from Cosmos DB.

        Args:
            table_names (str or list): The name of the table or a list of table names.

        Returns:
            dict or list: A dictionary containing the table schema if a single table name is provided,
                          or a list of dictionaries containing the schemas if multiple table names are provided.
        """
        try:
            if isinstance(table_names, str):
                # If a single table name is provided, convert it to a list for consistency
                table_names = [table_names]

            schemas = []
            for table_name in table_names:
                database = self.cosmos_client.get_database_client('your_database_name')
                container = database.get_container_client('your_container_name')
                item = container.read_item(table_name, table_name)
                schemas.append(item['schema'])

            return schemas
        except Exception as e:
            print(f"Error retrieving table schema(s) from Cosmos DB: {str(e)}")
            return []


    def delete_cosmos_schema(self, user_id, file_name):
        """
        Fetches data from the container based on a user ID.

        Args:
            user_id (str): The user ID to retrieve data for.

        Returns:
            Any: The value associated with the user ID.
        """
        # Define a query to retrieve a specific document by its key
        query = f"SELECT * FROM c WHERE c.id = '{user_id}'"

        # Execute the query to retrieve the document(s) with the matching key
        items = list(self.container.query_items(query, enable_cross_partition_query=True))
        # Check if any items were found
        file_name=file_name.lower().replace(' ', '_')
        file_name=file_name.split('/')[-1]
        if items:
            # Assuming there's only one matching document (key is unique)
            item = items[0]
            # Extract the associated value
            value = item.get("value")
            updated_values=[]
            for i in value:
                for key, value in i.items():
                    if key==file_name:
                        print(f'table {file_name} deleted from cosmos')
                        pass
                    else:
                        updated_values.append(i)
            schema_dict={user_id:updated_values}
            for key, value in schema_dict.items():
                    self.container.upsert_item({"id": key, "value": value})
        else:
            print(f"Key '{user_id}' not found in the container.")

    def delete_excel(self, user_id, file_name):
        try:
            message, blob_data=self.blob_pipeline.blob_deleting(user_id,  file_name)
            if file_name.endswith(".csv"):
                table_name=file_name.split('/')[-1].split('.')[0]
                data=pd.read_csv(StringIO(blob_data.decode('utf-8')))
                self.delete_cosmos_schema(user_id, file_name)
                self.drop_table(table_name)
            else:
                excel_file = pd.ExcelFile(blob_data)
                sheet_names=excel_file.sheet_names
                self.delete_cosmos_schema(user_id, file_name)
                for i in sheet_names:
                    table_name=file_name.replace(' ','_').split('.')[0]+'_'+i.replace(' ','_')
                    table_name=table_name.lower().split('/')[-1]
                    self.drop_table(table_name)
        except Exception as e:
            print(f'error in deleting the table : {e}')
        
    def process_excel_file(self, user_id, file_name, file_contents):
        """
        Processes an Excel file, inserts data into SQL, and updates the schema in Cosmos DB.
        Handles multiple sheets in Excel files.

        Args:
            file_path (str): The path to the Excel file.

        Returns:
            dict: A dictionary containing the schemas for all created tables.
        """
        try:
            if file_name.endswith(".csv"):
                # For CSV files, insert data into SQL and update Cosmos for one table
                schema_data=[]
                table_name=file_name
                data=pd.read_csv(StringIO(file_contents.decode('utf-8')))
                if len(data)>0:
                    if self.insert_data_to_sql(table_name, data):
                        columns_type = self.extract_columns_type(data)
                        schema_data.append({table_name.split('.')[0]:columns_type})
                if len(schema_data)>0:
                    schema_dict={user_id:[{file_name.lower():schema_data}]}
                    self.update_table_schema(user_id, schema_dict)
            else:
                # For Excel files, process each sheet separately
                excel_file = pd.ExcelFile(file_contents)
                sheet_names=self.get_sheet_names(excel_file)
                schema_data=[]
                for i in sheet_names:
                    table_name=file_name.split('.')[0]+'_'+i.replace(' ','_')+'.'+file_name.split('.')[1]
                    table_name=table_name.lower()
                    data = excel_file.parse(sheet_name=i)
                    if len(data)>0:
                        if self.insert_data_to_sql(table_name, data):
                            columns_type = self.extract_columns_type(data)
                            schema_data.append({table_name.split('.')[0]:columns_type})
                if len(schema_data)>0:
                    schema_dict={user_id:[{file_name.lower():schema_data}]}
                    self.update_table_schema(user_id, schema_dict)
            return schema_data
        except Exception as e:
            print(f"Error processing Excel file: {str(e)}")
            return {}
        

    def extract_columns_type(self, data):
        columns_type = {}
        for column_name, dtype in data.dtypes.items():
            pattern = r"[^A-Za-z0-9_=+-]"
            column_name = re.sub(pattern, '_', column_name)
            column_name=column_name.lower()
            if dtype == 'int64':
                columns_type[column_name] = 'int'
            elif dtype == 'float64':
                columns_type[column_name] = 'float'
            elif dtype == 'bool':
                columns_type[column_name] = 'boolean'
            elif dtype == 'datetime64[ns]':
                columns_type[column_name] = 'datetime'
            else:
                columns_type[column_name] = 'VARCHAR(MAX)'
        return columns_type

    def get_sheet_names(self, excel_file):
        """
        Gets the names of all sheets associated with a table.

        Args:
            table_name (str): The name of the table.

        Returns:
            list: A list of sheet names.
        """
        sheet_names=excel_file.sheet_names
        return sheet_names


    def delete_schema_from_cosmos(self, table_name):
        """
        Deletes the schema document from Cosmos DB.

        Args:
            table_name (str): The name of the table.
        """
        try:
            database = self.cosmos_client.get_database_client(self.cosmos_database_name)
            container = database.get_container_client(self.cosmos_container_name)
            container.delete_item(table_name, table_name)
        except Exception as e:
            print(f"Error deleting schema from Cosmos DB: {str(e)}")
            
            
    def get_all_table_schemas(self, user_id):
        """
        Retrieve schemas for multiple tables from Cosmos DB and return a formatted string.

        Args:
            pipeline (DataPipeline): The DataPipeline instance.
            table_names (list): A list of table names to retrieve schemas for.

        Returns:
            str: A formatted string containing the schemas of the specified tables.
        """
        schema_string = "The schema of the tables are as follows:\n"

        # Define a query to retrieve a specific document by its key
        query = f"SELECT * FROM c WHERE c.id = '{user_id}'"

        # Execute the query to retrieve the document(s) with the matching key
        items = list(self.container.query_items(query, enable_cross_partition_query=True))

        # Check if any items were found
        if items:
            # Assuming there's only one matching document (key is unique)
            item = items[0]
            schema=''

            # Extract the associated value
            value = item.get("value")

            schema=''
            for p in value:
                for i, j in p.items():
                    for k in j:
                        for t, s in k.items():
                            schema=schema+'Table Name :'+t+'\n'+'Schema:'+str(s)+'\n\n'



            return schema

        return schema_string
    
    def generate_sql_query(self, user_id, query):
        """
        Generates an SQL query from a natural language query based on the schema.

        Args:
            query (str): The natural language query.
            table_schema (dict): The schema of the table as a dictionary.

        Returns:
            str: The generated SQL query.
        """

        schema=self.get_all_table_schemas(user_id)
        connection_string_sql = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}"
        conn = pyodbc.connect(connection_string_sql)
        cursor = conn.cursor()

        prompt_sql=sql_prompt.prompt_sql.format(schema=schema)
        # Create a ChatCompletion request to generate SQL query
        sql_query = openai.ChatCompletion.create(
            engine="qna-deployment-model",
            messages=[
                {'role': 'system', 'content': prompt_sql},
                {'role': 'user', 'content': query}
            ]
        )["choices"][0]["message"]["content"]
        # Execute the provided SQL query
        result=self.answer_from_table(sql_query)

        print(result)

        return result
