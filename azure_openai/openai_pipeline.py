import os
import pandas as pd
import openai
import re
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
from azure_openai.dewa_prompt import check_followup_prompt, followup_query_prompt, final_prompt
from azure_cognitive_search import cognitive_search_1
from config import INDEX_NAME

class OpenAIPipeline:
    """
    A class for generating SQL queries from natural language queries using Azure Cognitive Search and OpenAI.
    """

    def __init__(self):
        """
        Initializes the DataPipeline instance.

        Args:
            service_name (str): The service name for Azure Cognitive Search.
            admin_key (str): The admin key for Azure Cognitive Search.
            index_name (str): The name of the search index.
            api_base (str): The base URL for the OpenAI API.
            api_key (str): The API key for the OpenAI API.
            api_type (str): The API type for the OpenAI API.
            api_version (str): The API version for the OpenAI API.
        """
        openai.api_key = os.getenv("dewaopenai-azure-api-key")
        openai.api_type = os.getenv("dewaopenai-azure-api-type")
        openai.api_base = os.getenv("dewaopenai-azure-api-base")
        openai.api_version = os.getenv("dewaopenai-azure-api-version")
        self.service_endpoint = os.getenv("dewacogsearch-endpoint")
        self.cog_key = os.getenv("dewacogsearch-access-key")
        self.index_name = INDEX_NAME
        self.search_pl = cognitive_search_1.SearchPipeline()



    def generate_answer(self, query, user_id, division, department, last_history=None):
        followup = self.check_followup(query, history)
        if followup == "True":
            final_query = self.followup_query(query, history)
        else:
            final_query = query
        context = self.search_pl.cog_results(final_query, user_id, division, department)
        prompt = final_prompt.format(context=context)
        ans = openai.ChatCompletion.create(engine="gpt4-32k",
                                            messages=[
                                                    {'role': 'system','content':" You are a QnA Company Customer Care Bot, who returns answer in JSON Format with 4 keys answer, document_name, page_no, and similar_queries."},
                                                    {'role': 'assistant','content': prompt},    
                                                    {'role': 'user', 'content':final_query+"Json Response:"}]
                                          )
        return ans["choices"][0]["message"]["content"], context
    
    def check_followup(self, query, history=None):
        """
        Checks if a query is a follow-up query based on context.

        Args:
            query (str): The natural language query.
            last_history (str, optional): The last history or context (default is None).

        Returns:
            bool: True if the query is a follow-up, False otherwise.
        """
        followup_prompt = check_followup_prompt.format(history=history, query = query)
        ans = openai.ChatCompletion.create(
            engine="qna-deployment-model",
            messages=[
                {'role': 'system', 'content': "Your task is to check whether the query is a follow up query or not and return True/False based on that"},
                {'role': 'assistant', 'content': followup_prompt},
                {'role': 'user', 'content': query}
            ]
        )
        return ans["choices"][0]["message"]["content"]

    def followup_query(self, query, history=None):
        """
        Generates a follow-up query based on context.

        Args:
            query (str): The natural language query.
            last_history (str, optional): The last history or context (default is None).

        Returns:
            str: The generated follow-up query.
        """
        follow_query_prompt = followup_query_prompt.format(history=history, query = query )
        ans = openai.ChatCompletion.create(
            engine="qna-deployment-model",
            messages=[
                {'role': 'system', 'content': follow_query_prompt},
                {'role': 'user', 'content': query}
            ]
        )
        return ans["choices"][0]["message"]["content"]
