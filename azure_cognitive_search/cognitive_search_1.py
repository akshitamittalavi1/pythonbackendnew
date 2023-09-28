import json
import openai
import time
import json
import os
import sys
import re
import uuid
import openai
import logging 
import pandas as pd
import numpy as np
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
)
from azure.core.exceptions import AzureError
from azure.core.credentials import AzureKeyCredential  
from azure.search.documents import SearchClient  
from azure.search.documents.indexes import SearchIndexClient, SearchIndexerClient
from azure.search.documents.models import Vector  
from IPython.display import Image, display 
from azure_logging.project_logs import MyLogger
from azure.search.documents.indexes.models import (  
    VectorSearchAlgorithmConfiguration,
    SearchIndex,  
    SearchField,
    SearchFieldDataType,  
    SimpleField,
    FieldMapping,
    SearchableField,  
    SearchIndex,  
    VectorSearch,   
    SearchIndexerDataContainer,  
    SearchIndexer,  
    SemanticConfiguration, 
    SemanticSettings,
    PrioritizedFields, 
    SemanticField,  
    SearchField,  
    SemanticSettings,  
    VectorSearch,  
    VectorSearchAlgorithmConfiguration,  
    SearchIndexerDataSourceConnection,  
    InputFieldMappingEntry,  
    OutputFieldMappingEntry,  
    SearchIndexerSkillset,
    CorsOptions,
    IndexingParameters,
    IndexerStatus,
    SearchIndexerDataContainer, SearchIndex, SearchIndexer, SimpleField, SearchFieldDataType,
    EntityRecognitionSkill, InputFieldMappingEntry, OutputFieldMappingEntry, SearchIndexerSkillset,
    CorsOptions, IndexingSchedule, SearchableField, IndexingParameters, SearchIndexerDataSourceConnection, WebApiSkill
)
from config import INDEX_NAME
from azure_openai.dewa_prompt import check_followup_prompt, followup_query_prompt, final_prompt
 
logger = MyLogger(filename="./projectlogs/search_pipeine_log.log")

class SearchPipeline:
    """
    This class provides methods for creating embeddings and managing a search index using Azure Cognitive Search and OpenAI.
    """

    def __init__(self):
        """
        Initializes an instance of the SearchPipeline class.

        Args:
            index_name (str): The name of the search index.
        """
        openai.api_key = os.getenv("dewaopenai-azure-api-key")
        openai.api_type = os.getenv("dewaopenai-azure-api-type")
        openai.api_base = os.getenv("dewaopenai-azure-api-base")
        openai.api_version = os.getenv("dewaopenai-azure-api-version")
        self.service_endpoint = os.getenv("dewacogsearch-endpoint")
        self.cog_key = os.getenv("dewacogsearch-access-key")
        self.index_name = INDEX_NAME
        self.credential = AzureKeyCredential(self.cog_key)
        self.index_client = SearchIndexClient(endpoint=self.service_endpoint, credential=self.credential)
        self.search_client = SearchClient(endpoint=self.service_endpoint, index_name=self.index_name,
                                         credential=AzureKeyCredential(self.cog_key))

    def create_embedding(self, data,userid,filename,access):
        """
        Creates document embeddings for JSON data and returns a list of documents.

        Args:
            path (str): The path to the JSON file.
            userid (str): The user ID.
            filename (str): The name of the file.
            access (str): The access information.

        Returns:
            list: A list of documents with embeddings.
        """
        try:
            documents = []
            for i, j in data.items():
                pattern = r"[^A-Za-z0-9_=+-]"
                file_name = re.sub(pattern, '_', filename)
                document_embedding = openai.Embedding.create(input = j, engine='text-embedding-ada-002')['data'][0]['embedding']
                page_document={'id':f"{userid}_{file_name}_{i}", 'content':j, 'contentVector':document_embedding,"access" : access,"page_no":f"{str(i)}", "document_name":filename}
                documents.append(page_document)

            return documents
        except Exception as e:
            print(f'Exception occured with detail : {e}')
            return []

    def create_index(self):
        """
        Creates a search index with defined fields and settings.
        """
        try:
            fields = [
                SimpleField(name="id", type=SearchFieldDataType.String, key=True),
                SearchableField(name="content", type=SearchFieldDataType.String, retrievable=True),
                SimpleField(name="access", type=SearchFieldDataType.String, retrievable=True, filterable=True),
                SimpleField(name="document_name", type=SearchFieldDataType.String, retrievable=True, filterable=True),
                SimpleField(name="page_no", type=SearchFieldDataType.String, retrievable=True, filterable=True),
                SearchField(name="contentVector", type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                            searchable=True, vector_search_dimensions=1536, vector_search_configuration="my-vector-config")
            ]

            vector_search = VectorSearch(
                algorithm_configurations=[
                    VectorSearchAlgorithmConfiguration(
                        name="my-vector-config",
                        kind="hnsw",
                        hnsw_parameters={
                            "m": 4,
                            "efConstruction": 400,
                            "efSearch": 500,
                            "metric": "euclidean"
                        }
                    )
                ]
            )

            index = SearchIndex(name=self.index_name, fields=fields, vector_search=vector_search)
            result = self.index_client.create_or_update_index(index)
            print(f'{result.name} created')
        except Exception as e:
            print(f'Exception occured with detail : {e}')

    def update_index(self, path, userid, filename, access):
        """
        Updates the search index with document embeddings.

        Args:
            path (str): The path to the JSON file.
            userid (str): The user ID.
            filename (str): The name of the file.
            access (str): The access information.
        """
        try:
            documents = self.create_embedding(path, userid, filename, access)
            for idx, i in enumerate(documents):
                result = self.search_client.upload_documents(i)
            print('Index updated succesfully for this document')
        except Exception as e:
            print(f'Exception occured with detail : {e}')
            
            

    def cog_results(self, query, user_id, division, department):
        query_vector = openai.Embedding.create(input=[query],engine="text-embedding-ada-002")["data"][0]["embedding"]
        search_client = SearchClient(endpoint=self.service_endpoint,
                                     index_name=self.index_name, 
                                     credential=AzureKeyCredential(self.cog_key)) 

        results = search_client.search( 
            search_text=None,  
            vector=query_vector,
            top_k=5,  
            vector_fields="contentVector",
            select=["content","document_name", "page_no"],
            filter= f"access eq '{user_id}' or access eq '{division}' or access eq '{department}'",
        )

        
        context=''
        for i, result in enumerate(results):
            if i >= 5:
                break
#         for result in results:
            r=f'document_name: {result["document_name"]} \n\n'+ f'page_no : {result["page_no"]}\n\n' +  f'section : {result["content"]}\n-------------------------------------------------------------------------------------\n\n'
            context=context+r     
        return context
    
    
    def delete_docs_in_indexer(self, file_name):
        # Specify the filename you want to search for
        filename_to_find = "Natural Language Processing with Transformers, Revised Edition (2).pdf"

        # Define the search query
        search_filter = f"document_name eq '{file_name}'"

        # Execute the search query
        results = list(self.search_client.search(search_text="*", filter=search_filter))
        for i in results:
            id=i['id']
            # Delete the document
            self.search_client.delete_documents([{"@search.action": "delete", "id": id}])

        print('File deleted successful from index')

        return 'File deleted from Index'
            