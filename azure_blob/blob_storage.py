import logging
import pickle
import json
import os
from azure.storage.blob import BlobServiceClient,BlobClient, ContainerClient, ContentSettings
from project_logs import MyLogger
from config import BLOB_CONTAINER1, BLOB_CONTAINER3
from azure_cognitive_search.cognitive_search_1 import SearchPipeline
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta

logger = MyLogger(filename="./projectlogs/blob_log.log")


class AzureBlob():
    """
    This class has a set of functions defined for uploading files, initializing Azure blob storage object, downloading, and getting the size of a file on blob containers.
    """
    def __init__(self):
        self.search_pipeline=SearchPipeline()
        self.blob_connection_string = os.getenv("dewablob-connection-string")
        self.blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)
    
    @logger.log_execution(logger)
    def initialize_blob_client(self, file_name, blob_container_name):
        """
        This method establishes a connection to the blob storage and returns the container object.

        Args:
            file_name (str): The name of the file in string format, e.g., "oil-and-gas-handbook.pdf".
            blob_container_name (str): The name of the Azure blob container in string format, e.g., "ctminerv3".
            blob_connection_string (str): The connection string for accessing the blob container.

        Returns:
            blob_client: The blob_client object for performing upload, download, read operations on the blob container if the connection is successfully established, or else returns False.
        """
        try:
            blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)
            blob_client = blob_service_client.get_blob_client(container=blob_container_name, blob=file_name)
            logger.log(message=f"Container: {blob_container_name} Created!", level=logging.INFO)
            print(f"Created Container named as {blob_container_name}")
            return blob_client
        except Exception as e:
            print("Azure Blob error", e)
            return False

    @logger.log_execution(logger)
    def upload_blob(self, filepath, file_name, blob_container_name):
        """
        This method uploads the file to the Azure blob container.

        Args:
            filepath (str): The path to the file in string format.
            file_name (str): The name of the file in string format.
            blob_container_name (str): The name of the Azure blob container.
            blob_connection_string (str): The connection string for the storage account to establish a connection to the blob container.

        Returns:
            bool: True if the file is successfully uploaded to the Azure blob container, False otherwise.
        """
        # Call the method for initializing the object for the blob container
        blob_client = self.initialize_blob_client(file_name, blob_container_name, self.blob_connection_string)

        # Check if the connection is successful
        if blob_client:
            try:
                with open(filepath, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                    logger.log(f"File: {file_name} uploaded Successfully!", level=logging.INFO)
                    print(f"File Uploaded Successfully")
                    return True
            except Exception as e:
                print("Azure Blob error", e)
                return False
        else:
            return False
       
    @logger.log_execution(logger)
    def upload_blob_to_path(self, container_name, path, file_content, content_type, metadata=None):
        try:
            print('in blob try')
            # Remove leading and trailing slashes from the path
            path = path.strip('/')

            # Create a BlobServiceClient using the connection string
            blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)

            # Get a BlobClient for the specified container and path
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=path)

            print('blob before upload')

            # Upload the file content and set content type
            blob_client.upload_blob(file_content, overwrite=True, content_settings=ContentSettings(content_type=content_type))
            print('blob after upload')
            # Set metadata for the blob
            if metadata:
                print('in metadata')
                blob_properties = blob_client.get_blob_properties()
                blob_properties.metadata = metadata
                blob_client.set_blob_metadata(metadata=blob_properties.metadata)
                print('last metadata')

            # Get the URL of the uploaded blob
            blob_url = blob_client.url
            print('blob url {blob_url}')

            return blob_url

        except Exception as e:
            print(f"Error: {e}")
            return None
    
     
    @logger.log_execution(logger)
    def list_folder_files(self, user_id, user_dept, user_div, folder_name, BLOB_CONTAINER_NAME):
        print('before try inside')
        try:
            blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)
            container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
            

            # Initialize a list to store file paths and URLs
            file_paths_urls = []
            path=f'dewa/{user_div}/{user_dept}/{user_id}/{folder_name}/'
            for blob in container_client.list_blobs(name_starts_with=path):
                file_name=blob.name.split('/')[-1]
                individual_access='Individual'
                file_date=blob.last_modified.strftime("%d/%m/%Y")
                blob_url = self.generate_blob_sas_url(blob.name)
                file_paths_urls.append({"file_name": file_name, "access": individual_access, "file_date": file_date, "blob_url": blob_url})

            return file_paths_urls

        except Exception as e:
            print(f"Error: {e}")
            return []


        
        
    @logger.log_execution(logger)
    def list_files_by_user_id_metadata(self, user_id, user_dept, user_div, BLOB_CONTAINER_NAME):
        try:
            blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)
            container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
            

            # Initialize a list to store file paths and URLs
            file_paths_urls = []

            # Define the list of access values to check
            access_values = [user_id, user_dept, user_div]

            # List all blobs in the container
            blobs = container_client.list_blobs(name_starts_with=None)

            for blob in blobs:
                # Check if the blob has the expected metadata
                blob_client = container_client.get_blob_client(blob.name)
                metadata = blob_client.get_blob_properties().metadata
                file_access=blob.name.split('/')
                if metadata:
                    print(blob.name)
                    # Check if the required metadata key "access" exists in the blob
                    if "access" in metadata:
                        # Check if the metadata "access" value matches any of the access values
                        if metadata["access"] in access_values:

                            # Construct the file path
                            file_date=blob.last_modified.strftime("%d/%m/%Y")
                            file_path = blob.name
                            file_name=blob.name.split('/')[-1]

                            # Get the URL of the blob
                            blob_url = self.generate_blob_sas_url(blob.name)
#                             blob_url = container_client.get_blob_client(blob).url

                            
                            if len(file_access)==5:
                                individual_access='Individual'
                            elif len(file_access)==4:
                                individual_access='Department'
                            elif len(file_access)==3:
                                individual_access='Division'
                            elif len(file_access)==2:
                                individual_access='Organization'
                            else:
                                individual_access='Individual'
                                file_name=blob.name.split('/')[-2]
                                blob_url='NaN'
                            

                            # Add file path and URL to the list
                            file_paths_urls.append({"file_name": file_name, "access": individual_access, "file_date": file_date, "blob_url": blob_url})

                    elif len(file_access)>=5:
                        print(blob.name)
                        individual_access = 'Individual'
                        file_name = blob.name.split('/')[-1]
                        blob_url = 'NaN'
                        file_date = blob.last_modified.strftime("%d/%m/%Y")
                        print(file_date)
                        file_paths_urls.append({"file_name": file_name, "access": individual_access, "file_date": file_date, "blob_url": blob_url})

            return file_paths_urls

        except Exception as e:
            print(f"Error: {e}")
            return []
        
    @logger.log_execution(logger)    
    def generate_blob_sas_url(self, blob_name, expiration_hours=24):
        # Create a BlobServiceClient using the account name and account key

        sas_expiry = datetime.utcnow() + timedelta(hours=1)  # Expires in 1 hour

        sas_permissions = BlobSasPermissions(read=True, list=True)  # Adjust as needed

        # Generate the SAS token for the blob
        sas_token = generate_blob_sas(
            self.blob_service_client.account_name,
            BLOB_CONTAINER3,
            blob_name,
            account_key=self.blob_service_client.credential.account_key,  # Use account key for authentication
            permission=sas_permissions,
            expiry=sas_expiry
            )
        
        sas_url = f"https://{self.blob_service_client.account_name}.blob.core.windows.net/{BLOB_CONTAINER3}/{blob_name}?{sas_token}"

        return sas_url   
    
    @logger.log_execution(logger)    
    def blob_deleting(self, user_id, file_name):
        try:
            # Create a BlobServiceClient using the connection string
            blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)
            # Get a container client
            container_client = blob_service_client.get_container_client(BLOB_CONTAINER3)

            print('blob name', file_name)
            # Get a blob client for the blob you want to delete
            blob_client = container_client.get_blob_client(file_name)

            print('blob client')
            print(file_name)

            if file_name.endswith(tuple(['.xlsx','.csv'])):
                print('in excel download')
                # Download the blob to a stream object
                download_stream = blob_client.download_blob()

                # Read the blob data from the stream
                blob_data = download_stream.read()
            else:
                blob_data=None

            metadata = blob_client.get_blob_properties().metadata

            if metadata['owner']==user_id:
                print('in metadata')
                # Delete the blob
                blob_client.delete_blob()
                document_name=file_name.split('/')[-1]
                print("hii")
                message= self.search_pipeline.delete_docs_in_indexer(document_name)

            else:
                return {'message':'Your are not eligible to delete file'}, None

            return {'message':'The file is deleted successfully'}, blob_data
        except Exception as e:
            print(f"An error occurred: {e}")
            return 'An error occured when deleting the file'

    
    
    
    @logger.log_execution(logger)
    def upload_blob_metadata(self, blob_container_name, blob_name, data):
        """
        This method uploads data to the Azure blob container and sets metadata.

        Args:
            blob_container_name (str): The name of the Azure blob container.
            blob_name (str): The name of the blob.
            data (bytes): The data to upload.
            blob_connection_string (str): The connection string for the storage account.

        Returns:
            str: The URL of the uploaded blob.
        """
#         blob_client = self.initialize_blob_client(blob_name, blob_container_name, blob_connection_string)
        try:
            # Remove leading and trailing slashes from blob_name
            blob_name = blob_name.strip('/')

            # Create the BlobServiceClient using the connection string
            blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)

            # Get the blob client for the specified container and blob name
            blob_client = blob_service_client.get_blob_client(container=blob_container_name, blob=blob_name)

            # Upload the data
            blob_client.upload_blob(data, overwrite=True, content_settings=ContentSettings(content_type='application/pdf'))

            # Set metadata for the blob
            blob_properties = blob_client.get_blob_properties()
            blob_properties.metadata = metadata
            blob_client.set_blob_metadata(metadata=blob_properties.metadata)

            print(f"File '{blob_name}' uploaded to container '{blob_container_name}' successfully!")
            logger.log(f"File: {blob_name} uploaded to container {blob_container_name}' successfully!", level=logging.INFO)
            print(f"File Uploaded Successfully")

            return blob_client.url
        except Exception as e:
            print(f"Error: {e}")
            return None

    @logger.log_execution(logger)
    def set_metadata(self, blob_container_name, blob_name, more_blob_metadata):
        """
        This method sets metadata for a blob in the Azure blob container.

        Args:
            blob_container_name (str): The name of the Azure blob container.
            blob_name (str): The name of the blob.
            more_blob_metadata (dict): Additional metadata to set.
            blob_connection_string (str): The connection string for the storage account.

        Returns:
            bool: True if metadata is successfully set, False otherwise.
        """
        blob_client = self.initialize_blob_client(blob_name, blob_container_name, self.blob_connection_string)
        try:
            # Retrieve existing metadata, if desired
            blob_metadata = blob_client.get_blob_properties().metadata
            blob_metadata.update(more_blob_metadata)
            # Set metadata on the blob
            blob_client.set_blob_metadata(metadata=blob_metadata)
            print("Meta-data updated")
            return True
        except Exception as e:
            print(f"Error: {e}")
            return False

    @logger.log_execution(logger)
    def download_blob(self, blob_container_name, blob_name, output_file_path):
        """
        This method downloads a blob from the Azure blob container.

        Args:
            blob_container_name (str): The name of the Azure blob container.
            blob_name (str): The name of the blob to download.
            output_file_path (str): The path to save the downloaded file.
            blob_connection_string (str): The connection string for the storage account.

        Returns:
            str: The URL of the downloaded blob.
        """
        blob_client = self.initialize_blob_client(blob_name, blob_container_name, self.blob_connection_string)
        try:
            with open(output_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            print(f"File '{blob_name}' downloaded to '{output_file_path}'")
            return blob_client.url
        except Exception as e:
            print(f"Error: {e}")
            return None

    @logger.log_execution(logger)
    def delete_blob(self, blob_container_name, blob_name):
        """
        This method deletes a blob from the Azure blob container.

        Args:
            blob_container_name (str): The name of the Azure blob container.
            blob_name (str): The name of the blob to delete.
            blob_connection_string (str): The connection string for the storage account.

        Returns:
            bool: True if the blob is successfully deleted, False otherwise.
        """
        blob_client = self.initialize_blob_client(blob_name, blob_container_name, self.blob_connection_string)
        try:
            blob_client.delete_blob()
            print(f"Blob '{blob_name}' deleted from container '{blob_container_name}'")
            logger.log(f"Blob '{blob_name}' deleted from container '{blob_container_name}'", level=logging.INFO)
            return True
        except Exception as e:
            print(f'Error: {e}')
            return False

    @logger.log_execution(logger)
    def create_container(self, blob_container_name):
        """
        Creates a container in Azure Blob Storage.

        Args:
            blob_container_name (str): Name of the container.
            blob_connection_string (str): The connection string for the storage account.

        Returns:
            ContainerClient: A client to interact with the created container.
        """
        # Create the container
        blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)
        container_client = blob_service_client.create_container(blob_container_name)
        return container_client

    @logger.log_execution(logger)
    def delete_container(self, blob_container_name):
        """
        Deletes a container from the blob storage.

        Args:
            blob_container_name (str): Name of the container to delete.
            blob_connection_string (str): The connection string for the storage account.

        Returns:
            bool: True if the container is successfully deleted, False otherwise.
        """
        # Get a reference to a container or create one if it does not exist
        blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)
        container_client = blob_service_client.get_container_client(blob_container_name)
        # Delete the container
        try:
            container_client.delete_container()
            print(f"Container '{blob_container_name}' deleted")
            return True
        except Exception as e:
            print(f"Container '{blob_container_name}' does not exist")
            print(f"Error: {e}")
            return False

    @logger.log_execution(logger)
    def get_container_list(self):
        """
        Gets a list of containers in the storage account.

        Args:
            blob_connection_string (str): The connection string for the storage account.

        Returns:
            list: List of container names.
        """
        # Get an iterator of containers in the storage account
        blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)
        container_list = blob_service_client.list_containers()
        try:
            return [container.name for container in container_list]
        except Exception as e:
            print(f"Error: {e}")
            return None

    @logger.log_execution(logger)
    def direct_upload_blob(self, data, folder_name, filetype, blob_container_name):
        """
        Uploads data to the Azure blob container.

        Args:
            data: The data to upload.
            folder_name (str): The name of the folder on the blob container in string format.
            filetype (str): The type of file, e.g., 'pickle', 'json', 'pdf'.
            blob_container_name (str): The name of the Azure blob container.
            blob_connection_string (str): The connection string for the storage account.

        Returns:
            bool: True if the data is successfully uploaded to the Azure blob container, False otherwise.
        """
        blob_client = self.initialize_blob_client(folder_name, blob_container_name, self.blob_connection_string)

        if blob_client:
            try:
                if filetype == 'pickle':
                    blob_client.upload_blob(pickle.dumps(data), overwrite=True)
                elif filetype == 'json':
                    blob_client.upload_blob(json.dumps(data), overwrite=True)
                elif filetype == 'pdf':
                    blob_client.upload_blob(data, overwrite=True)
                logger.log(f"Folder {folder_name} uploaded to Azure Blob Storage {blob_container_name}' successfully!", level=logging.INFO)
                print(f"File Uploaded Successfully")
                return True
            except Exception as e:
                print("Azure Blob error", e)
                return False
        else:
            return False

    @logger.log_execution(logger)
    def get_files_blob(self, file_name, blob_container_name):
        """
        Reads data from the Azure blob container.

        Args:
            file_name (str): The name of the file to read.
            blob_container_name (str): The name of the Azure blob container.
            blob_connection_string (str): The connection string for the storage account.

        Returns:
            bytes: Data read from the blob.
        """
        try:
            blob_client = BlobClient.from_connection_string(self.blob_connection_string, blob_container_name, file_name)
            downloader = blob_client.download_blob(0)
            data = downloader.readall()
            blob_client.close()
            logger.log(f"Reading from Azure files Done successfully!", level=logging.INFO)
            return data
        except Exception as e:
            print(e)

    @logger.log_execution(logger)
    def get_blob_url(self, blob_container_name, file_name):
        """
        Gets the URL of a blob in the Azure blob container.

        Args:
            blob_container_name (str): The name of the Azure blob container.
            file_name (str): The name of the blob.
            blob_connection_string (str): The connection string for the storage account.

        Returns:
            str: The URL of the blob.
        """
        try:
            blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)
            container_client = blob_service_client.get_container_client(blob_container_name)

            blob_client = container_client.get_blob_client(file_name)
            blob_url = blob_client.url
            logger.log(f"Returning URLs Successfully!", level=logging.INFO)
            return blob_url
        except Exception as e:
            print(e)

    @logger.log_execution(logger)
    def get_metadata(self,blob_container_name, blob_name):
        """
        This method gets metadata from a blob in the Azure blob container.

        Args:
            blob_container_name (str): The name of the Azure blob container.
            blob_name (str): The name of the blob.
            blob_connection_string (str): The connection string for the storage account.

        Returns:
            bool: Metadata of blob.
        """
        blob_client = self.initialize_blob_client(blob_name, blob_container_name, self.blob_connection_string)
        try:
            # Retrieve existing metadata, if desired
            blob_metadata = blob_client.get_blob_properties().metadata
            return blob_metadata
        except Exception as e:
            print(f"Error: {e}")
            return False
        
    @logger.log_execution(logger)
    def list_all_blobs_in_container(self,blob_container_name, folder_path = None):
            """
            Lists all blobs in specific folder in the Azure blob container.

            Args:
                blob_container_name (str): The name of the Azure blob container.
                folder_path (str): Path of particular folder on Azure blob container.
                blob_connection_string (str): The connection string for the storage account.

            Returns:
                list: List of  blob names present in particular folder if folder_path is given otherwise it will return list of all blobs  present in container.
            """
            try:
                blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)
                container_client = blob_service_client.get_container_client(blob_container_name)
                blobs_list = container_client.list_blobs(name_starts_with = folder_path)
                listf = [(i['name'], i['last_modified'].strftime("%d/%m/%Y")) for i in blobs_list]
                blob_service_client.close()
                logger.log(f"Returning all the list of containers from {blob_container_name} Successfully!", level=logging.INFO)
                return listf
            except Exception as e:
                print(e)
                
      # Define a function to list files recursively with metadata filter
    @logger.log_execution(logger)
    def list_files_with_metadata(self, container_name,file_name,embedding, prefix="", metadata_filter=None ):
        blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        files = []

        blob_list = container_client.walk_blobs(name_starts_with=prefix)
        for blob in blob_list:
            blob_metadata = blob.metadata
            if all(blob_metadata.get(key) == value for key, value in metadata_filter.items()):
                files.append(blob.name)

        return files


    @logger.log_execution(logger)
    def delete_folder(self, path, container_name):
        try:
            # Create a BlobServiceClient using the connection string
            blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)
            print(path)
            # Get a container client
            container_client = blob_service_client.get_container_client(container_name)

            # List all blobs with a prefix matching the folder path
            blobs_to_delete = [blob for blob in container_client.list_blobs(name_starts_with=path+'/')]

            print(blobs_to_delete)
            print('*'*50)
            # Delete each blob in the list
            for blob in blobs_to_delete:
                container_client.delete_blob(blob.name)
            print('*'*80)
            print(path)
            container_client.delete_blob(path)
            print('*'*30)
            # Note: Azure Blob Storage doesn't have actual folders, so we don't need to delete the "folder."

            return True

        except Exception as e:
            print(f"An error occurred: {e}")
            return False

    @logger.log_execution(logger)
    def create_folder(self, user, folder_name, container_name):
        if not os.path.exists('temp.txt'):
            try:
                with open("temp.txt", "w") as file:
                    print('*'*30)
                    # You can optionally write something to the file here if needed.
                    pass
            except Exception as e:
                print(f"An error occurred: {e}")
        else:
            print(f"File 'temp.txt' already exists.")

        # Define 'path' before using it
        user_dept = user['value']['department']
        user_div = user['value']['division']
        user_id = user.get('id', '')
        path = f"dewa/{user_div}/{user_dept}/{user_id}/{folder_name}/temp.txt"
        print('*'*30)
        print(path)

        # Create a BlobServiceClient using the connection string
        blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)

        # Get a BlobClient for the specified container and path
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=path)
        print('*'*30)

        # Upload the file content and set content type
        # Use the correct blob path (path) here, not the container name
        blob_client.upload_blob(open("temp.txt", "rb"), overwrite=True)
        blob_client.delete_blob()  # No need to specify the blob name; it's already set in blob_client

