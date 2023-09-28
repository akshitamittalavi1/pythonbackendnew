import requests
import logging
import os
from azure_logging.project_logs import MyLogger

logger = MyLogger(filename="./projectlogs/bing_search_log.log")

class Bing:
    """
    This class provides methods for querying the Bing Search API to retrieve single or multiple answers.
    """

    def __init__(self):
        """
        Initializes an instance of the Bing class.

        Args:
            api_key (str): The API key for accessing the Bing Search API.
            endpoint (str): The endpoint URL for the Bing Search API.
        """
        self.bing_search_api_key = os.getenv("dewabing-api-key")
        self.bing_search_endpoint = os.getenv("dewabing-endpoint") + "v7.0/search"

    @logger.log_execution(logger)
    def single_answer(self, query):
        """
        Retrieves a single answer from the Bing Search API based on the query.

        Args:
            query (str): The query to search for.

        Returns:
            list: A list containing the content and URL of the top search result.
        """
        params = {'q': query, 'answerCount': 1}
        headers = {'Ocp-Apim-Subscription-Key': self.bing_search_api_key}

        # Call the API
        try:
            response = requests.get(self.bing_search_endpoint,
                                    headers=headers, params=params)
            response.raise_for_status()
            json = response.json()
            results = json["webPages"]["value"]
            data = [
                f"{results[0]['snippet']}", f"{results[0]['url']}"
            ]
            logger.log(message="Response received from Bing single_answer", level=logging.INFO)
            print(f"Response received: {response}")
            return data

        except Exception as e:
            print(e)

    @logger.log_execution(logger)
    def multiple_answer(self, query):
        """
        Retrieves multiple answers from the Bing Search API based on the query.

        Args:
            query (str): The query to search for.

        Returns:
            list: A list of strings containing content and URL for each search result.
        """
        params = {'q': query, 'answerCount': 10}
        headers = {'Ocp-Apim-Subscription-Key': self.bing_search_api_key}

        # Call the API
        try:
            response = requests.get(self.bing_search_endpoint,
                                    headers=headers, params=params)
            response.raise_for_status()
            json = response.json()
            results = json["webPages"]["value"]
            data = [
                f"Content: {result['snippet']}, URL: {result['url']}" for result in results
            ]
            logger.log(message="Response received from Bing multiple_answer", level=logging.INFO)
            print(f"Response received: {response}")
            return data

        except Exception as e:
            print(e)
