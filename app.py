import io
import os
import sys
import pandas as pd
import json
import openai
import logging
import fitz
import re
from datetime import datetime
from typing import List, Optional
from azure.storage.blob import BlobServiceClient, ContentSettings
from fastapi import FastAPI, HTTPException, Form, File, UploadFile
from pydantic import BaseModel
from config import BLOB_CONTAINER1, BLOB_CONTAINER2, BLOB_CONTAINER3, COSMOS_DATABASE_NAME, COSMOS_CONTAINER_NAME
from azure_transalation import translator
from azure_document_intelligence import form_recognizer
from azure_openai import openai_pipeline
from dotenv import load_dotenv
from azure_blob import blob_storage
from azure_bing import bing_search
from azure_cosmos import cosmos_db
from azure_openai import preprocessing_pipeline
from fastapi.middleware.cors import CORSMiddleware
from azure_security import azure_keyvault
from azure_sql.SQL_Pipeline_2 import DataPipeline

load_dotenv()

keyvault_obj = azure_keyvault.AzureKeyVault()
keyvault_obj.set_environment_from_key_vault()
translator = translator.Translator()
app = FastAPI()
Blob = blob_storage.AzureBlob()
sql_pipeline=DataPipeline()
search_pipeline = openai_pipeline.OpenAIPipeline()
bing = bing_search.Bing()
cd = cosmos_db.cosmos_pipeline(COSMOS_DATABASE_NAME, COSMOS_CONTAINER_NAME)
pre_process = preprocessing_pipeline.preprocesing()
cosmos = cosmos_db.cosmos_pipeline(COSMOS_DATABASE_NAME, "conversations")


# initialize openai API credentials and details
openai.api_key = os.getenv("dewaopenai-azure-api-key")
openai.api_type = os.getenv("dewaopenai-azure-api-type")
openai.api_base = os.getenv("dewaopenai-azure-api-base")
openai.api_version = os.getenv("dewaopenai-azure-api-version")
BLOB_CONN_STRING = os.getenv("dewablob-connection-string")


from project_logs import MyLogger
logger = MyLogger(filename="./projectlogs/app_logs.log")

# CORS settings
origins = [
    "*"
]

 

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Data(BaseModel):
    query: str
    flag: bool
        
class PDFFileList(BaseModel):
    filenames: List[str]

@app.get("/")
async def read_root():
    logger.log(message="Hello World printed Successfully!", level=logging.INFO)
    return {"Hello": "World"}

def create_blob_path(user, access_level, folder_name, file_name):
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
    
@app.post("/upload")
async def upload_json_and_pdf(
    user_id: str = Form(...),
    access_level: str = Form(...),
    folder_name: str = Form(...),
    # file_name: str = Form(...),
    pdf_files: List[UploadFile] = File(...)
):
    try:
        # Check for PDF file...
        for pdf_file in pdf_files:
            file_name = pdf_file.filename
            pattern = r"[^A-Za-z0-9_=+.-]"
            file_name = re.sub(pattern, '_', file_name)
            print(f'File name: {file_name}')
            file_extensions = [".xlsx", ".xls", ".csv",".pptx",".ppt",".pdf"]
            if not file_name.endswith(tuple(file_extensions)):
                return {"message": "Only PDF,Excel and PPT files are allowed."}
            
            user = cd.get_user_details(user_id)
            if access_level == "Organisation":
                system = "dewa"
            elif access_level == "Division":
                system = user['value']['division']
            elif access_level == "Department":
                system = user['value']['department']
            elif access_level == "Individual" or access_level == "Self":
                system = user.get('id', '')
            
            if user:
                # Create the Blob Storage path using user details
                path = create_blob_path(user, access_level, folder_name, file_name)
                print('after path', path)
                metadata = {
                    "access": system,
                    "owner": user_id
                }
            else:
                print('no user fount')
                return "no user"
            if file_name.endswith(tuple('.pdf')):
                # Read the PDF file content
                content_type='application/pdf'
                print('before content')
                pdf_content = pdf_file.file.read()
                # Fetch user details from Cosmos DB
                blob_url = Blob.upload_blob_to_path(BLOB_CONTAINER3, path, pdf_content, content_type, metadata)
                print('blob url')
                if blob_url:
                    print({"message": f"File {file_name} uploaded to Blob Storage. URL: {blob_url}"})
                else:
                    return {"message": f"File {file_name} upload failed"}
            elif file_name.endswith(tuple([".xlsx", ".xls", ".csv"])):
                # Read the uploaded file into memory
                content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                file_contents = await pdf_file.read()

                print(f'Path uploading {path}')

                blob_url = Blob.upload_blob_to_path(BLOB_CONTAINER3, path, file_contents, content_type, metadata)

                print('uploaded in blob')

                t=sql_pipeline.process_excel_file(user_id, file_name, file_contents)

            elif file_name.endswith(tuple([".pptx",".ppt",])):
                pass
            else:
                return {"message": f"User {user_id} not found in Cosmos DB"}
        return "Files uploaded Successfully"
    except Exception as e:
        return {"message": str(e)} 
    
    
@app.post("/preprocess")
async def preprocess(
    user_id: str = Form(...),
    access_level: str = Form(...),
    folder_name: str = Form(...),
    pdf_files: str = Form(...)
):
    try:
        # Process the PDF filenames and check for valid extensions in a single loop
        pdf_files = pdf_files.split(',')
        for file_name in pdf_files:
            pattern = r"[^A-Za-z0-9_=+.-]"
            print(file_name)
            file_extensions = [".pdf"]
            if not file_name.endswith(tuple(file_extensions)):
                print({"message": "Only PDF files are allowed."})
                continue
                return {"message": "Only PDF files are allowed."}
            else:
                pre_process.preprocess_pdf(user_id, access_level, folder_name, file_name)

        return "Successfully completed the preprocessing"
    except Exception as e:
        return {"message": str(e)}

@app.post("/delete_file")
async def delete_file_api(
    user_id: str = Form(...),
    file_url: str = Form(...),
):
    try:
        blob_url=file_url.split('?')[0]
        file_name=blob_url.split(BLOB_CONTAINER3+'/')[-1]
        pattern = r"[^A-Za-z0-9_=+/.-]"
        file_name = re.sub(pattern, '_', file_name)
        if file_name.endswith('.pdf'):
            # Function to delete file
            message, data=Blob.blob_deleting(user_id,  file_name)
            return message
        else:
            sql_pipeline.delete_excel(user_id, file_name)
    except Exception as e:
        return {"message": str(e)}
      

@app.post("/create_folder")
async def create_individual_folder(
    user_id: str = Form(...),
    folder_name: str = Form(...),
):
    try:
        # Fetch user details from Cosmos DB
        user = cd.get_user_details(user_id)
        if user:
            # Call the create_folder function to create the folder and handle the result
            Blob.create_folder(user, folder_name, BLOB_CONTAINER3)
            print('*'*30)
            return {"message": f"Folder {folder_name} created successfully."}
        else:
            return {"message": f"User {user_id} not found in Cosmos DB"}
    except Exception as e:
        return {"message": str(e)}


@app.post("/delete_folder")
async def delete_folder_api(
    user_id: str = Form(...),
    folder_name: str = Form(...),
):
    try:
        # Get user details from Cosmos DB (Replace this with your actual code)
        user = cd.get_user_details(user_id)
        if user:
            # Define the path of the folder to delete
            user_dept = user['value']['department']
            user_div = user['value']['division']
            user_id = user.get('id', '')
            folder_path = f"dewa/{user_div}/{user_dept}/{user_id}/{folder_name}"
            # Call the delete_folder function to delete the folder and its contents
            success = Blob.delete_folder(folder_path, BLOB_CONTAINER3)
            print('*'*30)
            if success:
                return {"message": f"Folder {folder_name} and its contents deleted successfully."}
            else:
                return {"message": f"Folder {folder_name} deletion failed."}
        else:
            return {"message": f"User {user_id} not found in Cosmos DB"}
    except Exception as e:
        return {"message": str(e)}

    
    

    
@app.post("/list_files")
async def list_files(
    user_id: str = Form(...),
):
    user = cd.get_user_details(user_id)
    if not user:
        return {"message": f"User {user_id} not found in Cosmos DB."}
    # Extract user_dept and user_div from the user's data
    user_dept = user['value']['department']
    user_div = user['value']['division']
    print(user_div, user_dept)
    # List files based on the provided parameters
    print('before blob')
    file_paths_urls = Blob.list_files_by_user_id_metadata(user_id, user_dept, user_div, BLOB_CONTAINER3)
    if file_paths_urls:
        # Create a set to store unique file paths
        unique_file_paths = set()

        # Create a new list for storing unique elements
        unique_data = []

        # Iterate through the data
        for item in file_paths_urls:
            file_path = item["file_name"]
            
            # Check if the file path is not in the set of unique paths
            if file_path not in unique_file_paths:
                unique_file_paths.add(file_path)

                # Add the item to the unique_data list
                unique_data.append(item)
        return {"files": unique_data}
    else:
        return {"message": "No files found for the provided criteria."}
  
    
@app.post("/list_folder_files")
async def list_folder_files(
    user_id: str = Form(...),
    folder_name: str = Form(...),
):
    user = cd.get_user_details(user_id)
    if not user:
        return {"message": f"User {user_id} not found in Cosmos DB."}
    # Extract user_dept and user_div from the user's data
    user_dept = user['value']['department']
    user_div = user['value']['division']
    print(user_div, user_dept)
    # List files based on the provided parameters
    print('before blob')
    file_paths_urls = Blob.list_folder_files(user_id, user_dept, user_div, folder_name, BLOB_CONTAINER3)
    if file_paths_urls:
        return {"files": file_paths_urls}
    else:
        return {"message": "No files found for the provided criteria."}

    
def get_list(user_id):
    user = cd.get_user_details(user_id)
    # print(user)
    if not user:
        return {"message": f"User {user_id} not found in Cosmos DB."}
    # Extract user_dept and user_div from the user's data
    user_dept = user['value']['department']
    user_div = user['value']['division']

    file_paths_urls = Blob.list_files_by_user_id_metadata(user_id, user_dept, user_div, BLOB_CONTAINER3)
    if file_paths_urls:
        # Create a set to store unique file paths
        unique_file_paths = set()

        # Create a new list for storing unique elements
        unique_data = []

        # Iterate through the data
        for item in file_paths_urls:
            file_path = item["file_name"]
            
            # Check if the file path is not in the set of unique paths
            if file_path not in unique_file_paths:
                unique_file_paths.add(file_path)

                # Add the item to the unique_data list
                unique_data.append(item)
        
        folder_list = []
        for ls in unique_data:
            if ls["blob_url"] == "NaN":
                folder_list.append(ls['file_name'])
        print('folder list:', folder_list)
        for folder_name in folder_list:
            file_list = Blob.list_folder_files(user_id, user_dept, user_div, folder_name, BLOB_CONTAINER3)
            unique_data.extend(file_list)

        return {"files": unique_data}
    else:
        return None

def highlight_pdf_page(input_pdf, output_pdf, page_index_to_highlight):
    try:
        doc = fitz.open(input_pdf)
        if int(page_index_to_highlight) < 0 or int(page_index_to_highlight) >= len(doc):
            print(f"Invalid page index {page_index_to_highlight}. The PDF has {len(doc)} pages.")
            return
        page_data = doc[page_index_to_highlight]
        highlight_rectangle = page_data.rect
        highlight = page_data.add_highlight_annot(highlight_rectangle)
        # output_ = f"test_{input_pdf}"
        doc.save(output_pdf)
        doc.close()

        print(f"Page {page_index_to_highlight} highlighted and saved to {output_pdf}")
        return output_pdf
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

# Check if the file exists before attempting to delete it
def delete_file(file_to_delete):
    if os.path.exists(file_to_delete):
        try:
            # Delete the file
            os.remove(file_to_delete)
            print(f"{file_to_delete} has been successfully deleted.")
        except OSError as e:
            print(f"Error deleting {file_to_delete}: {e}")
    else:
        print(f"{file_to_delete} does not exist.")

@app.post("/highlight/")
async def highlight(
    user_id: str = Form(...),
    doc_name: str = Form(...),
    page_no: str = Form(...),
):
    try: 
        print("Getting file list ...")
        pattern = r"[^A-Za-z0-9_=+.-]"
        doc_name = re.sub(pattern, '_', doc_name)
        
        file_list = get_list(user_id)
        # print(file_list)
        for file in file_list['files']:
            # print(file['file_name'])
            if file['file_name'] == doc_name:
                # print('*',*30)
                url = file['blob_url']
                path = url.split('?')[0].split(BLOB_CONTAINER3)[-1]
                blob_client = Blob.blob_service_client.get_blob_client(container=BLOB_CONTAINER3, blob=path)
                # Read the blob content as bytes
                input_file = f"test_{doc_name}"
                with open(input_file, "wb") as local_file:
                    blob_data = blob_client.download_blob()
                    local_file.write(blob_data.readall())

                output_path = highlight_pdf_page(input_file, doc_name, int(page_no) - 1)
                if not output_path:
                    return "Something went wrong while highlighting document."
                delete_file(input_file)

                # Upload the local file to the blob
                blob_client = Blob.blob_service_client.get_blob_client(container=BLOB_CONTAINER3, blob= f"temp/{output_path}")
                cnt_settings = ContentSettings(content_type="application/pdf")
                with open(output_path, "rb") as data:
                    # blob_client = container_client.get_blob_client(doc_name)
                    blob_client.upload_blob(data,overwrite = True,content_settings=cnt_settings)
                    print("Blob uploaded successfully...")

                delete_file(doc_name)
                url = Blob.generate_blob_sas_url(f"temp/{output_path}")
                print(url)
                return {"url" : url}
        print("No such file found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        print("No such file found")
        
    

def try_parse_json(input_string, default_value):
    try:
        # Attempt to parse the input string as JSON
        json_data = json.loads(input_string)
        return json_data
    except ValueError:
        # If parsing fails, return the default value
        return default_value  

@app.post("/get_all_conversations")
#returns all the conversation of a particular user
async def get_all_conversations(
    user_id: str= Form(...)):
    try:
        data = cosmos.get_user_details(user_id)
        convo_list = []
        for key,value in data['value'].items():
            convo_list.append({key:value['convo_name']})
        return convo_list
    
    except Exception as e:
        print("Error:",str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/get_conversation")
# Returns only a single conversation corresponding to a particular user and conversation id
async def get_conversation(
    user_id: str = Form(...),
    conversation_id: str = Form(...)):
    try:
        data = cosmos.get_user_details(user_id)
        print(data)
        conversation_list = []
        for query_details in data['value'][conversation_id]['history']:

            req_id,req_details = list(query_details.items())[0]
            query_ans = {"recommendedQuestions":req_details["similar_ques"],"languageType":req_details["lang"],"queryType":req_details["query_type"]}
            if req_details["lang"] == 'ar':
                query_ans["query"] = req_details["ar_query"]
                query_ans["answer"] = req_details["ar_response"]
            else:
                query_ans["query"] = req_details["en_query"]
                query_ans["answer"] = req_details["en_response"]
            
            if query_ans["queryType"] == "bing":
                query_ans["docLink"] = req_details["bing_doc_link"]
                query_ans["pageNum"] = "NaN"
            else:
                query_ans["docLink"] = list(req_details["doc_name_page"].items())[0][0]
                query_ans["pageNum"] = list(req_details["doc_name_page"].items())[0][1]

            conversation_list.append(query_ans)
        return conversation_list
    
    except Exception as e:
        print("Error:", str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/rename_conversation")
# This function takes user id,Conversation id and Conversation_name(which willbe the new name) as a input
async def rename_conversation(user_id: str = Form(...),
                              conversation_id: str = Form(...),
                              convo_name: str = Form(...)):
    try:
        old_data = cosmos.get_user_details(user_id)
        data = {old_data['id']:old_data['value']}
        data[user_id][conversation_id]['convo_name'] = convo_name
        cosmos.upload_data(data)
        print("Conversation renamed successfully")
        return {f"Conversation {conversation_id} successfully renamed as {convo_name}"}
    
    except Exception as e:
        print("Error:",str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/delete_conversation")
#delete_conversation takes user_id and conversation_id as an input and deletes the conversation corresponding to that
async def delete_conversation(
                    user_id: str = Form(...),
                    conversation_id: str = Form(...)):
    
    try:
        value = cosmos.get_user_details(user_id)['value']
        del value[conversation_id]
        data = {user_id:value}
        print(data)
        cosmos.upload_data(data)
        return {"message": f"Conversation with ID '{conversation_id}' deleted successfully"}
    except Exception as e:
        print("Error while deleting conversation:",str(e))
        raise HTTPException(status_code=500, detail=str(e))


def update_convo(user_id,conversation_id,query_details,updated_date,new_convo):
    if new_convo:
        try:
            print("___"*30)
            old_data = cosmos.get_user_details(user_id)
            old_data['value'][conversation_id] =  {"convo_name":f"New conversation-{conversation_id}","last_date":str(datetime.now()),"history":[query_details]}
            data = {old_data['id']:old_data['value']}
        except:
            data = {user_id : {conversation_id : {"convo_name":f"New conversation-{conversation_id}","last_date":str(datetime.now()),"history":[query_details]}}}
        print("New conversation created.")
        cosmos.upload_data(data)
        return conversation_id,f"New conversation-{conversation_id}"
    old_data = cosmos.get_user_details(user_id)
    if not old_data:
        print(f"{user_id} conversation not found")
    data = {old_data['id']:old_data['value']}
    data[user_id][conversation_id]['last_date'] = updated_date
    data[user_id][conversation_id]['history'].append(query_details)
    print("Running update query  function")
    cosmos.upload_data(data)
    print("Query details updated successfully.")
    return conversation_id,f"New conversation-{conversation_id}"


@app.post("/process_query/")
async def process_query(
    query: str = Form(...),
    lang_type: str = Form(...),
    user_id: str = Form(...),
    query_type: str = Form(...),
    conversation_id: str = Form(...),
    request_id: str = Form(...),
    new_convo: bool = Form(...)
):
    query_details = {"lang":lang_type, "en_query":query, "en_response":"NaN","ar_query":"Nan","ar_response":"NaN","query_type":query_type, "cog_section":"NaN", "sql_query":"NaN","sql_response" : "NaN", "bing_doc_link": "NaN", "similar_ques":"NaN", "document_name":"NaN", "datetime":"NaN"}

    lang_type = translator.detect_language(query)
    if lang_type == "ar":
        query =translator.translate(query, lang_type)
        query_details['en_query'] = "NaN"
        query_details["ar_query"] = query
    else:
        query_details['en_query'] = query
        query_details["ar_query"] = "NaN"
        query_details['ar_response'] = "NaN"

    
    query_details["query_type"] = query_type

    try:
        # Rest of your processing logic here...
        if query_type == "bing":
            result = bing.single_answer(query) if bing else "Bing Search not available"

            query_details["en_response"] = result[0]
            query_details["bing_doc_link"] = result[1]

            response = {
            "answer": result[0],
            "recommendedQuestions": None,
            "docLink": result[1],
            "pageNum": None,
            "languageType": lang_type,
            "queryType": query_type
            }

        else:
            query_details["bing_doc_link"] = "NaN"

            user = cd.get_user_details(user_id)
            user_dept = user['value']['department']
            user_div = user['value']['division']

            result,context = search_pipeline.generate_answer(query, user_id, user_div, user_dept)

            query_details["cog_section"] = context

            print('*'*40)
            print(result)
            default_value = "Sorry, this information is out of my uploaded knowledge base, Please ask queries from Uploaded Documents."
#             data = json.loads(result)
#             data = text_to_json(result)
            data = try_parse_json(result, default_value)
            print('*'*30)
            print(data)
            print(type(data))
            if isinstance(data, dict):
                # Extract values from the parsed JSON
#                 data = convert_dict_keys_to_lowercase(data)
                data_list = list(data.keys())
                try:
                    answer = data.get(data_list[0], "")
                except KeyError:
                    answer = None
                try:
                    document_name = data.get(data_list[1], "")
                except KeyError:
                    document_name = None
                try:
                    page_no = data.get(data_list[2], "")
                except KeyError:
                    page_no = None
                try:
                    similar_queries = data.get(data_list[3], [])
                except KeyError:
                    similar_queries = None

                print('-'*60)
                print(answer)
                print(document_name)
                print(page_no)
                print(similar_queries)
                                
                # match = re.search(r'_(\d{4})$', page_no)
                # last_four_digits = match.group(1)
                # page_num = last_four_digits.lstrip('0')
                # print(page_num)
                # doc_name = attach_pdf_extension(document_name)
                
                if document_name =="null" or document_name =="NA" or page_no =="null" or page_no =="NA" or page_no =="NA" or document_name =="None" or page_no ==None or document_name ==None:
                    response = {
                                "answer": default_value,
                                "recommendedQuestions": None,
                                "docLink": None,
                                "pageNum": None,
                                "languageType": lang_type,
                                "queryType": query_type
                                }
                else:
                    response = {
                                "answer": answer,
                                "recommendedQuestions": similar_queries,
                                "docLink": document_name,
                                "pageNum": page_no,
                                "languageType": lang_type,
                                "queryType": query_type
                                }
            else:
                response = {
                                "answer": data,
                                "recommendedQuestions": None,
                                "docLink": None,
                                "pageNum": None,
                                "languageType": lang_type,
                                "queryType": query_type
                                }
#             response = result
        if lang_type == "ar":
            ar_response = translator.translate(response["answer"], "en",lang_type)
            query_details["ar_response"] = ar_response
            query_details["en_response"] = "NaN"

        print(response["answer"])
        query_details["en_response"] = response["answer"]
        query_details["sql_query"] = "NaN"
        query_details["sql_response"] = "NaN"
        query_details["datetime"] = str(datetime.now())
        query_details["similar_ques"] = response["recommendedQuestions"]


        if query_type=="bing":
            query_details["doc_name_page"] = "NaN"
        else:
            query_details["doc_name_page"] = {response["docLink"]:response["pageNum"]}
        print("Running")
        convo_details = {request_id : query_details}
        print("Running 3")
        convo_id,convo_name = update_convo(user_id,conversation_id,convo_details,str(datetime.now()),new_convo)
        print("Running2")
        logger.log(message="Query Processed processed successfully", level=logging.INFO)
        if new_convo:
            response["conversationId"] = convo_id
            response["conversationName"] = convo_name
            return {"result": response}
        print("Running 3")
        return {"result": response}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


# @app.post("/share/")
# async def shzre(
#     user_id: str = Form(...),
#     source_blob_url: str = Form(...),
#     access_level: str = Form(...),
#     shared_list: str = Form(...)
# ):
#     source_blob_url = source_blob_url.split("?")[0]
#     filename = source_blob_url.split("/")[-1]
#     print(source_blob_url,filename,sep="\n")
#     if access_level == "Individual":
#         for share_to in shared_list.split(","):
#             user = cd.get_user_details(share_to)
#             folder_name = "Shared"
#             user_dept = user['value']['department']
#             user_div = user['value']['division']
#             user_id = user.get('id', '')
#             path = f"dewa/{user_div}/{user_dept}/{user_id}/{folder_name}/{filename}"
#             # path = create_blob_path(user,access_level,folder_name,filename)
#             print(path)
    

        
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, timeout_keep_alive = 1500)

    
    
    
    
    
       
    
    
    
    
