import os 
from azure_document_intelligence.form_recognizer import FormRecognizer
from azure_transalation.translator import Translator
from azure_cognitive_search.cognitive_search_1 import SearchPipeline
from azure_cosmos import cosmos_db
from azure.storage.blob import BlobServiceClient
from config import BLOB_CONTAINER3, COSMOS_DATABASE_NAME, COSMOS_CONTAINER_NAME, INDEX_NAME
import re


class preprocesing:
    def __init__(self):
  
        self.blob_service_client = BlobServiceClient.from_connection_string(os.getenv("dewablob-connection-string"))
        self.form_recognizer=FormRecognizer()
        self.translator=Translator()
        self.cognitive_index=SearchPipeline()
        self.cd = cosmos_db.cosmos_pipeline(COSMOS_DATABASE_NAME, COSMOS_CONTAINER_NAME)


    def create_blob_path(self, user, access_level, folder_name, file_name):
        user_dept = user['value']['department']
        user_div = user['value']['division']
        user_id = user.get('id', '')
        if access_level == "Department":
            return f"dewa/{user_div}/{user_dept}/{file_name}"
        elif access_level == "Division":
            return f"dewa/{user_div}/{file_name}"
        elif access_level == "Organisation":
            return f"dewa/{file_name}"  # Use "rbactest" as the blob container
        elif access_level == "Individual":
            return f"dewa/{user_div}/{user_dept}/{user_id}/{folder_name}/{file_name}"
        elif access_level == "Self":
            return f"dewa/{user_div}/{user_dept}/{user_id}/{file_name}"
   
    def preprocess_pdf(self,  user_id, access_level, folder_name, file_name):
        user = self.cd.get_user_details(user_id)
        print('pre blob')
        pattern = r"[^A-Za-z0-9_=+.-]"
        file_name = re.sub(pattern, '_', file_name)
        path = self.create_blob_path(user, access_level, folder_name, file_name)
        blob_client = self.blob_service_client.get_blob_client(container=BLOB_CONTAINER3, blob=path)
        # Read the blob content as bytes
        blob_content = blob_client.download_blob()
        extracted_data = self.form_recognizer.extract_from_document(blob_content)
        text = self.form_recognizer.extract_text_and_tables(extracted_data.to_dict(), "markdown")
        text = self.form_recognizer.text_formating(text)
        # Detect and translate text if it's in Arabic
    
        if access_level == "Organisation":
            access = "dewa"
        elif access_level == "Division":
            access = user['value']['division']
        elif access_level == "Department":
            access = user['value']['department']
        elif access_level == "Individual" or access_level == "Self":
            access = user.get('id', '')
        
        detected_language = self.translator.detect_language(text[1])
        if detected_language == 'ar':
            text = {page: self.translator.translate(content) for page, content in text.items()}

        self.cognitive_index.update_index(text, user_id, file_name, access)

        print('Your File is Updated Succesfully')
