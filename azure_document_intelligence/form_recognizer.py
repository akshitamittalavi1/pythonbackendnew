import os 
import json
import pandas as pd
from collections import defaultdict, Counter
from operator import itemgetter
from tabulate import tabulate
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential



class FormRecognizer:
    """
    This class provides methods for extracting information and tables from documents using the Azure Form Recognizer service.
    """

    def __init__(self) -> None:
        """
        Initializes an instance of the FormRecognizer class.
        """
        self.endpoint = os.getenv("dewaform-endpoint")
        self.key = os.getenv("dewaform-key")
        # Create a Form Recognizer client
        self.document_analysis_client = DocumentAnalysisClient(
            endpoint=self.endpoint, credential=AzureKeyCredential(self.key)
        )

    def extract_from_document(self,data):
        """
        Extracts information from a document using the Azure Form Recognizer service.

        Args:
            data (str): The document data.

        Returns:
            dict: Extracted information or an empty dictionary in case of an error.
        """
        extracted_text = ""
        try:
            poller = self.document_analysis_client.begin_analyze_document(
                "prebuilt-layout", document=data
            )
            result = poller.result()
            return result
        except Exception as e:
            print(e)
            return {}

    def fill_table_by_row_column_span(self,row_count, column_count, cells):
        """
        Arranges the cells(extracted from form recognizer) for a table in a list of lists object.
        This function positions everything on the basis of column index, row index, row span and column span only.
        Headers are not treated differently here.

        Args:
            row_count (int): An integer specifying number of rows in the table
            column_count (int): An integer specifying number of columns in the table
            cells (dict): A dictionary containing cells data of the table.


        Returns:
            table: The extracted table as a list of lists. The headers list is also included.
            table_content_list: The list containing all the japanese text of all the cells of the table
        """
        # Initialize an empty table
        table = [["" for _ in range(column_count)] for _ in range(row_count)]
        table_content_list = []
        # Fill the cell content in the table, considering row_span and column_span
        for cell in cells:
            row_index = cell["row_index"]
            column_index = cell["column_index"]
            row_span = cell["row_span"]
            column_span = cell["column_span"]
            content = cell["content"].split("\n:unselected:")[0]
            content = content.split("\n:selected:")[0]
            content = content.split(":unselected:")[0]
            content = content.split(":selected:")[0]

            if content not in ["", ":unselected:"]:
                table_content_list.append(content)
            for row_span_index in range(row_span):
                for column_span_index in range(column_span):
                    table[row_index + row_span_index][
                        column_index + column_span_index
                    ] = content
        return table, table_content_list


    def make_unique_columns(self,df_columns):
        """
        Created unique column names of a dataframe.
        If multiple columns have same name, their name will be differentiated by adding " " at the end of the name.

        Args:
            df_columns (list): DataFrame columns

        Returns:
            new_col_names: The list containing the updated unique column names

        """
        name_counts = defaultdict(int)
        new_col_names = []

        for name in df_columns:
            new_count = name_counts[name] + 1
            new_col_names.append(str(name) + " " * new_count)
            name_counts[name] = new_count

        return new_col_names


    def extract_single_table_as_dataframe(self,data, kind="paragraph"):
        """
        Extracts a table from the dictionary produced by form recognizer for one table and returns it as a string with a table in markdown format.

        Args:
            data (dict): A dictionary containing table data.

        Returns:
            table markdown: The extracted table in markdown format as a string.
            table_content_list: The list containing all the Japanese text of all the cells of the table.

        """
        row_count = data["row_count"]
        column_count = data["column_count"]
        cells = data["cells"]
        # Find the number of header rows
        try:
            header_rows = max(
                cell["row_index"] + cell["row_span"]
                for cell in cells
                if cell["kind"] == "columnHeader"
            )
        except:
            header_rows = 0

        table, table_content_list = self.fill_table_by_row_column_span(
            row_count, column_count, cells
        )
        # Filter out empty rows
        filtered_table = [row for row in table if any(cell != "" for cell in row)]
        # Join header items with colons
        updated_header_table = [
            " : ".join(tuple(dict.fromkeys(items)))
            for items in zip(*filtered_table[:header_rows])
        ]
        # if header rows is not 0, headers are present in table otherwise headers are not present
        if header_rows != 0:
            # Create DataFrame, skipping the header rows
            df = pd.DataFrame(
                filtered_table[header_rows:], columns=updated_header_table)
        else:
            # Create DataFrame by not specifying column headers
            # since headers not present in table
            df = pd.DataFrame(filtered_table[header_rows:])
        df.drop_duplicates(inplace=True)
        df.columns = list(self.make_unique_columns(df.columns))
        if kind == "markdown":
            return (
                tabulate(df.to_records(index=False),
                        headers=df.columns, tablefmt="github"),

                table_content_list,
            )
        elif kind == "json":
            return (
                json.dumps(
                    {"fields": list(df.columns), "data": df.values.tolist()},
                    ensure_ascii=False,
                ),
                table_content_list,
            )
        else:
            # if kind is paragraph or any other
            return df, table_content_list


    def table_extraction(self,tables_dictionary, kind="paragraph"):
        """
        Extracts tables from the provided dictionary of tables produced by form recognizer
        and returns them as a dictionary containing page_number(key) and list of extracted tables from the page_number

        Args:
            tables_dictionary (dict): A dictionary containing table data.

        Returns:
            tables_content: Dictionary containing page number as keys and tables data(list of dictionaries) as values.
            Each dictionary in the list corresponds to a table and has the following keys:
            1. "table_content": dictionary containing the following:
                1. "role": "table"
                2. "content": table(in japanese) as string in markdown format
            2. "table_list": list of japanese text contained in the orginal table

        """
        tables_content = {}
        for table_dict in tables_dictionary:
            page_number = int(table_dict["bounding_regions"][0]["page_number"])
            table, table_list = self.extract_single_table_as_dataframe(
                table_dict, kind
            )

            single_table_content = {
                "table_content": {
                    "role": "table",
                    "content": table,
                    #                 "translated_content": translated_table,
                },
                "table_list": table_list,
            }
            #             single_table_content=translated_table_markdown
            if page_number in tables_content.keys():
                tables_content[page_number].append(single_table_content)
            else:
                tables_content[page_number] = [single_table_content]
        return tables_content


    def smallest_subsequence(self,stream, search):
        """
        Extracts the smallest sublist of the stream list that contains all the elements of search list

        Args:
            stream (list): A main list
            search (list): A list to be searched in main list

        Returns:
            minimal_subsequence: smallest sublist of the stream list that contains all the elements of search list
        """

        if not search:
            return []  # the shortest subsequence containing nothing is nothing

        stream_counts = Counter(stream)
        search_counts = Counter(search)

        minimal_subsequence = None

        start = 0
        end = 0
        subsequence_counts = Counter()

        while True:
            # while subsequence_counts doesn't have enough elements to cancel out every
            # element in search_counts, take the next element from search
            while search_counts - subsequence_counts:
                if end == len(stream):  # if we've reached the end of the list, we're done
                    return minimal_subsequence
                subsequence_counts[stream[end]] += 1
                end += 1

            # while subsequence_counts has enough elements to cover search_counts, keep
            # removing from the start of the sequence
            while not search_counts - subsequence_counts:
                if minimal_subsequence is None or (end - start) < len(minimal_subsequence):
                    minimal_subsequence = stream[start:end]
                subsequence_counts[stream[start]] -= 1
                start += 1


    def find_sub_list(self,sublist, main_list):
        """
        Extracts the start and end index of sublist from main_list

        Args:
            sublist (list): A list
            main_list (list): A list

        Returns:
            start_index: integer representing the start index of sublist in main_list
            end_index: integer representing the end index of sublist in main_list
        """
        results = []
        sublist_modified = self.smallest_subsequence(main_list, sublist)

        if sublist_modified is not None:
            sublist_length = len(sublist_modified)

            for ind in (i for i, e in enumerate(main_list) if e == sublist_modified[0]):
                if main_list[ind: ind + sublist_length] == sublist_modified:
                    results.append((ind, ind + sublist_length - 1))

        return results[0] if results else None


    def paragraph_kind_page_construction(self,page_content, tables_content, number_of_pages):
        """
        Constructs the unstructured content pagewise by adding the paragraph content after ##Paragraph## and
        table content after ##Table##.

        Args:
            page_content (dict): A dictionary containing structured pagewise content of the document.
            tables_content (dict): Dictionary containing page number as keys and tables data(list of dictionaries) as values.
            number_of_pages (int): Number of pages in the document.

        Returns:
            page_content: A Dict containing the structured and unstructured pagewise content extracted from the documents.
        """
        # adding tables into the page_content at the correct position
        for page_number in range(1, number_of_pages + 1):
            paragraphs = list(
                map(itemgetter("content"),
                    page_content[page_number]["structured_content"])
            )
            if page_number in tables_content.keys():
                prev_index = 0
                count = 0
                text_string = ""
                for table in tables_content[page_number]:
                    start_index, end_index = find_sub_list(
                        table["table_list"], paragraphs)
                    if start_index == None:
                        is_error = True
                        problems.append(
                            {
                                "page_number": page_number,
                                "paragraphs": paragraphs,
                                "table_list": table["table_list"],
                            }
                        )
                        continue
                    if count == 0 and start_index == 0:
                        text_string = "##Table##\n" + "\n".join(
                            paragraphs[start_index: end_index + 1]
                        )
                    elif count == 0:
                        text_string = "##Paragraph##\n" + "\n".join(
                            paragraphs[0:start_index]
                        )
                        text_string += "\n##Table##\n" + "\n".join(
                            paragraphs[start_index: end_index + 1]
                        )
                    elif count != 0 and prev_index == start_index:
                        text_string += "\n##Table##\n" + "\n".join(
                            paragraphs[start_index: end_index + 1]
                        )
                    elif count != 0 and prev_index < start_index:
                        text_string += "\n##Paragraph##\n" + "\n".join(
                            paragraphs[prev_index:start_index]
                        )
                        text_string += "\n##Table##\n" + "\n".join(
                            paragraphs[start_index: end_index + 1]
                        )
                    prev_index = end_index

                    count += 1
                if end_index + 1 < len(paragraphs):
                    text_string += "\n##Paragraph##\n" + "\n".join(
                        paragraphs[end_index + 1:]
                    )
                page_content[page_number]["unstructured_content"] = text_string
            else:
                page_content[page_number][
                    "unstructured_content"
                ] = "##Paragraph##\n" + "\n".join(paragraphs)
            return page_content


    def extract_text_and_tables(self,analyze_result_dict, kind="markdown"):
        """
        Extracts both text and tables from the specified file using the Azure Form Recognizer API.

        Args:
            analyze_result_dict (dict): The form recognizer dictionary object of the file.
            kind (string):  paragraph: if tables need to be added as text in the text of the page
                            markdown: if tables need to be added in markdown format in the text of the page
                            json: if tables need to be added in json format in the text of the page

        Returns:
            Dict: A Dict containing the pagewise content extracted from the documents.
        """
        is_error = False
        number_of_pages = len(analyze_result_dict["pages"])
        page_content = {
            page_number: {"unstructured_content": "", "structured_content": []}
            for page_number in range(1, number_of_pages + 1)
        }
        for paragraph in analyze_result_dict["paragraphs"]:
            page_number = int(paragraph["bounding_regions"][0]["page_number"])
            paragraph["content"] = paragraph["content"].replace(
                "\n:unselected:", "")
            paragraph["content"] = paragraph["content"].replace("\n:selected:", "")
            paragraph["content"] = paragraph["content"].replace(":unselected:", "")
            paragraph["content"] = paragraph["content"].replace(":selected:", "")
            page_content[page_number]["structured_content"].append(
                {
                    "role": paragraph["role"] if paragraph["role"] is not None else "paragraph",
                    "content": paragraph["content"],
                }
            )
        tables_content = self.table_extraction(analyze_result_dict["tables"], kind)
        if kind in ["markdown", "json"]:
            for page_number in range(1, number_of_pages + 1):
                paragraphs = list(
                    map(
                        itemgetter("content"),
                        page_content[page_number]["structured_content"],
                    )
                )
                if page_number in tables_content.keys():
                    for table in tables_content[page_number]:
                        table_list = table["table_list"]
                        if table_list:
                            start_indices = [
                                i
                                for i, para in enumerate(paragraphs)
                                if any(item in para for item in table_list)
                            ]
                            if start_indices:
                                start_index = start_indices[0]
                                end_index = start_indices[-1]
                                page_content[page_number]["structured_content"][
                                    start_index: end_index + 1
                                ] = [table["table_content"]]
                page_content[page_number]["unstructured_content"] = "\n".join(
                    list(
                        map(
                            itemgetter("content"),
                            page_content[page_number]["structured_content"],
                        )
                    )
                )
        else:
            page_content = self.paragraph_kind_page_construction(
                page_content, tables_content, number_of_pages
            )
        return page_content
    
    def text_formating(self,extracted_text):
        """
        Formats the extracted text content.

        Args:
            extracted_text (dict): The extracted text content.

        Returns:
            dict: Formatted text content.
        """
        formatted_text = {}
        for page_index, page in extracted_text.items():
            page_data = ""
            for idx, dictionary in enumerate(page["structured_content"]):
                if dictionary["role"] == "table":
                    page_data += "##TABLE##\n"
                    page_data += dictionary["content"] + "\n"
                else:
                    if idx == 0:
                        page_data += "##PARAGRAPH##\n"
                    if idx > 0:
                        if page['structured_content'][idx - 1] != 'table':
                            pass
                        else:
                            page_data += "##PARAGRAPH##\n"
                    page_data += dictionary["content"] + "\n"
            formatted_text[page_index] = page_data
        return formatted_text
        