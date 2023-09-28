import os
import requests
import uuid
import logging
import json
from azure_logging.project_logs import MyLogger

logger = MyLogger(filename="./projectlogs/Translator_log.log")


class Translator:
    """
    This class provides methods for language detection and translation using the Azure Translator service.
    """

    def __init__(self):
        """
        Initializes an instance of the Translator class.
        """
        self.key = os.getenv("dewatranslation-key")
        self.endpoint = os.getenv("dewatranslation-endpoint")
        self.location = os.getenv("dewatranslation-location")

    @logger.log_execution(logger)
    def detect_language(self, text):
        """
        Detects the language of the given text using the Azure Translator service.

        Args:
            text (str): The text for language detection.

        Returns:
            str: The detected language or an empty string in case of an error.
        """
        try:
            path = '/detect'
            constructed_url = self.endpoint + path

            params = {
                'api-version': '3.0'
            }

            headers = {
                'Ocp-Apim-Subscription-Key': self.key,
                'Ocp-Apim-Subscription-Region': self.location,
                'Content-type': 'application/json',
                'X-ClientTraceId': str(uuid.uuid4())
            }

            body = [{
                'text': text
            }]

            request = requests.post(
                constructed_url, params=params, headers=headers, json=body)
            response = request.json()
            print(response)
            logger.log(message=f"Language detection Done Successfully, ", level=logging.INFO)
            return response[0]['language']
        except Exception as e:
            print(e)
            return ''

    @logger.log_execution(logger)
    def translate(self, text, source_language='ar', target_language='en'):
        """
        Translates the given text from the source language to the target language using the Azure Translator service.

        Args:
            text (str): The text to be translated.
            source_language (str): The source language (default is 'ar' for Arabic).
            target_language (str): The target language (default is 'en' for English).

        Returns:
            str: The translated text or an empty string in case of an error.
        """
        try:
            path = '/translate'
            constructed_url = self.endpoint + path

            params = {
                'api-version': '3.0',
                'from': source_language,
                'to': target_language
            }

            headers = {
                'Ocp-Apim-Subscription-Key': self.key,
                'Ocp-Apim-Subscription-Region': self.location,
                'Content-type': 'application/json',
                'X-ClientTraceId': str(uuid.uuid4())
            }

            body = [{
                'text': text
            }]

            request = requests.post(
                constructed_url, params=params, headers=headers, json=body)
            response = request.json()
            logger.log(message=f"Translation Done Successfully, ", level=logging.INFO)
            return response[0]['translations'][0]['text']
        except Exception as e:
            print(e)
            return ''
