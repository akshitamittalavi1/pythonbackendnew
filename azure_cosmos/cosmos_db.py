import os 
from azure.cosmos import CosmosClient, PartitionKey

class cosmos_pipeline():
    """
    This class provides methods for working with Azure Cosmos DB, including creating a database, container, uploading data, and fetching data.
    """

    def __init__(self, database_name, container_name):
        """
        Initializes an instance of the cosmos_pipeline class.

        Args:
            key (str): The primary key for accessing the Azure Cosmos DB.
            endpoint (str): The Cosmos DB endpoint URL.
            database_name (str): The name of the database.
            container_name (str): The name of the container (table) within the database.
        """
        self.key = os.getenv("dewacosmosdb-conn-str")
        self.endpoint = os.getenv("dewacosmosdb-endpoint")
        print('*'*50)
        print(self.key)
        print(self.endpoint)
        self.client = CosmosClient(self.endpoint, self.key)
        self.database = self.client.get_database_client(database_name)
        self.container = self.database.get_container_client(container_name)

    def create_database(self, database_name):
        """
        Creates a database in Azure Cosmos DB if it doesn't exist.

        Args:
            database_name (str): The name of the database to create.
        """
        # Create a database if it doesn't exist
        database = self.client.create_database_if_not_exists(id=database_name)

    def create_container(self, database_name, container_name):
        """
        Creates a container (table) within a database in Azure Cosmos DB if it doesn't exist.

        Args:
            database_name (str): The name of the database.
            container_name (str): The name of the container to create.
        """
        # Create a database if it doesn't exist
        database = self.client.create_database_if_not_exists(id=database_name)

        # Create a container (table) within the database if it doesn't exist
        container = database.create_container_if_not_exists(
            id=container_name,
            partition_key=PartitionKey(path="/partition_key_path")
        )

        print(f"Container '{container_id}' created successfully.")

    def upload_data(self, data):
        """
        Inserts data into the container.

        Args:
            data (dict): A dictionary of data to insert into the container.
        """
        # Insert data into the container
        for key, value in data.items():
            self.container.upsert_item({"id": key, "value": value})

    def fetch_data(self, user_id):
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
        if items:
            # Assuming there's only one matching document (key is unique)
            item = items[0]

            # Extract the associated value
            value = item.get("value")

            return value
        else:
            print(f"Key '{user_id}' not found in the container.")
            
            
    def fetch_all_documents(self):
        try:

            # Query all documents in the collection
            query = "SELECT * FROM c"
            items = list(self.container.query_items(query, enable_cross_partition_query=True))

            return items
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            return None
        
   # Define a function to fetch user details by user_id
    def get_user_details(self, user_id):
        try:
            # Define the query to fetch user details by user_id
            query = "SELECT * FROM c WHERE c.id = @user_id"
            parameters = [{"name": "@user_id", "value": user_id}]

            # Enable cross-partition query
            query_options = {
                "enableCrossPartitionQuery": True
            }

            # Execute the query
            result = list(self.container.query_items(query, parameters=parameters, enable_cross_partition_query=True))

            if result:
                # Assuming user_id is unique, return the first result
                return result[0]
            else:
                # User not found
                return None
        except Exception as e:
            # Handle exceptions as needed (e.g., log the error)
            print(f"An error occurred: {str(e)}")
            return None
#         query = f"SELECT * FROM c WHERE c.id = @user_id"
#         user = next(self.container.query_items(query, parameters=[{"name": "@user_id", "value": user_id}]), None)
#         return user

#     def fetch_specific_data(self, user_id, column_name):
#         """
#         Fetches a specific column value for a given user ID from the container.

#         Args:
#             user_id (str): The user ID to retrieve data for.
#             column_name (str): The name of the column to retrieve.

#         Returns:
#             Any: The value of the specified column associated with the user ID.
#         """
#         # Define a query to retrieve a specific document by its key
#         query = f"SELECT * FROM c WHERE c.id = '{user_id}'"

#         # Execute the query to retrieve the document(s) with the matching key
#         items = list(self.container.query_items(query, enable_cross_partition_query=True))

#         # Check if any items were found
#         if items:
#             # Assuming there's only one matching document (key is unique)
#             item = items[0]

#             # Extract the associated value
#             value = item.get("value")

#             value = value[column_name]

#             return value
        
    def fetch_specific_data(self, user_id, column_names):
        """
        Fetches a specific column value for a given user ID from the container.

        Args:
            user_id (str): The user ID to retrieve data for.
            column_name (str): The name of the column to retrieve.

        Returns:
            Any: The value of the specified column associated with the user ID.
        """
        # Define a query to retrieve a specific document by its key
        query = f"SELECT * FROM c WHERE c.id = '{user_id}'"

        # Execute the query to retrieve the document(s) with the matching key
        items = list(self.container.query_items(query, enable_cross_partition_query=True))

        # Check if any items were found
        if items:
            # Assuming there's only one matching document (key is unique)
            item = items[0]

            # Extract the associated value
            values = item.get("value")
            if isinstance(column_names, list):
                values = [values[column_name] for column_name in column_names]
            if isinstance(column_names,str):
                values = [values[column_names]]
            return values

    def delete_database(self, endpoint, key, database_name):
        """
        Deletes a database in Azure Cosmos DB.

        Args:
            endpoint (str): The Cosmos DB endpoint URL.
            key (str): The primary key for accessing the Azure Cosmos DB.
            database_name (str): The name of the database to delete.

        Returns:
            bool: True if the database is successfully deleted, False otherwise.
        """
        try:
            client = CosmosClient(endpoint, key)
            database = client.get_database_client(database_name)
            database.delete_database()
            print(f"Database '{database_name}' deleted successfully.")
            return True
        except Exception as e:
            print(f"Error deleting database '{database_name}': {str(e)}")
            return False
    
    def delete_collection(self, collection_name):
        """
        Deletes a collection (container) within a database in Azure Cosmos DB.

        Args:
            endpoint (str): The Cosmos DB endpoint URL.
            key (str): The primary key for accessing the Azure Cosmos DB.
            database_name (str): The name of the database containing the collection.
            collection_name (str): The name of the collection (container) to delete.

        Returns:
            bool: True if the collection is successfully deleted, False otherwise.
        """
        try:
            self.database.delete_container(collection_name)
            print(f"Collection '{collection_name}' in database deleted successfully.")
            return True
        except Exception as e:
            print(f"Error deleting collection '{collection_name}' in database: {str(e)}")
            return False
    
