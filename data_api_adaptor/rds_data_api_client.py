"""
RDS Data API Client module, tailored to be backwards compatible with PyMySQL methods
"""

__version__ = '1.2.0'
__author__ = 'Dersu Ozan ERCAN'
__last_updated__ = '2021/05/07 13:43'

import os
import re
import boto3

from typing import Union, Tuple


class RdsDataApiClient:
    def __init__(self) -> None:
        super().__init__()
        self.AWS_ACCESS_KEY_ID = os.getenv('ACCESS_KEY_ID')
        self.AWS_SECRET_ACCESS_KEY = os.getenv('SECRET_ACCESS_KEY')
        self.REGION_NAME = os.getenv('RDS_REGION')

        self.RESOURCE_ARN = os.getenv('RDS_RESOURCE_ARN')
        self.SECRET_ARN = os.getenv('RDS_SECRET_ARN')
        self.DATABASE_NAME = os.getenv('RDS_DATABASE_NAME')
        self.rds_client = boto3.client('rds-data', aws_access_key_id=self.AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY,
                                       region_name=self.REGION_NAME)
        self.data_api_response = None
        self.number_of_affected_rows = None
        self.returning_ids = []
        self.lastrowid = None

    ### Making adaptors for backwards compatibility with common MySql connectors ###
    def execute(self, query:str, query_args: Union[dict, list, tuple, None] = None) -> int:
        """
        Mimics PyMySQL's cursor.execute() call

        Args:
            query (str): SQL query(either parameterized or plain)
            query_args (dict, list, tuple, None): Arguments to fill placeholders in parameterized queries

        Returns:
            int: numberOfRecordsUpdated from data_api_response

        Raises:
            ClientError: When there is an error on AWS's side.
        """
        if query_args:
            new_query, new_query_args = self.convert_query_and_query_args_for_data_api(query, query_args)
            self.data_api_response = self.execute_query(new_query, new_query_args)
        else:
            self.data_api_response = self.execute_query(query)
    
        # Set lastrowid if it's an INSERT query
        if 'INSERT' in query.upper():
            # Check if INSERT operation generated any field(and assuming it will generate primary key field)
            if len(self.data_api_response['generatedFields']) > 0:
                self.lastrowid = self.data_api_response['generatedFields'][0]['longValue']

        # Set affected_rows and return
        self.number_of_affected_rows = self.data_api_response['numberOfRecordsUpdated']
        return self.number_of_affected_rows

    def executemany(self, query: str, query_args_list: list) -> int:
        """
        Mimics PyMySQL's cursor.executemany() call

        Args:
            query (str): SQL query(either parameterized or plain)
            query_args_list (list): List of parameter lists to fill placeholders in queries

        Returns:
            int: Number of results updated

        Raises:
            ClientError: When there is an error on AWS's side.
        """
        new_query_args_list = []
        new_query = query
        for query_args in query_args_list:
            new_query, new_query_args = self.convert_query_and_query_args_for_data_api(query, query_args)
            new_query_args_list.append(new_query_args)

        self.data_api_response = self.execute_query(new_query, new_query_args_list, True)

        # Set number of affected_rows, set returning_ids and return
        # NOTE: Boto client can return generated fields = [0, 0, 0, 0].
        # That statement below prevents that. Also gonna check that out in more detail
        for result in self.data_api_response['updateResults']:
            if len(result['generatedFields']) != 0 and result['generatedFields'][0].get('longValue'):
                self.returning_ids.append(result['generatedFields'][0]['longValue'])
        self.number_of_affected_rows = len(self.data_api_response['updateResults'])
        return self.number_of_affected_rows

    def fetchone(self) -> Union[dict, None]:
        """
        Mimics PyMySQL's cursor.fetchone() method
        """
        return self.data_api_response_to_pymysql_response(self.data_api_response, 'one')

    def fetchall(self) -> list:
        """
        Mimics PyMySQL's cursor.fetchall() method
        """
        return self.data_api_response_to_pymysql_response(self.data_api_response, 'all')

    def commit(self) -> None:
        """
        Here to ensure BaseSqlModel.conn.commit() calls won't brake
        NOTE:
            data_api_client's execute_statement and batch_execute_statement calls don't need
            explicit commit when they run without transactionId parameter.
        """
        pass

    def affected_rows(self) -> int:
        """
        Mimics PyMySQL's connection.affected_rows() method
        """
        return self.number_of_affected_rows

    ### Public helper wrappers ###
    def get_returned_ids(self) -> list:
        """
        Returns inserted/updated ids from executemany statements
        Note: This info comes from batch_execute_statement response dict, updateResults field
        """
        return self.returning_ids

    def begin_transaction(self, schema: Union[str, None] = None) -> str:
        """Starts a transaction and returns transactionId which will be used to commit that transaction"""
        response = self.rds_client.begin_transaction(
            database=self.DATABASE_NAME,
            resourceArn=self.RESOURCE_ARN,
            schema=f"{schema if schema else self.DATABASE_NAME}",
            secretArn=self.SECRET_ARN)
        
        return response['transactionId']

    def commit_transaction(self, transaction_id: str) -> bool:
        """Commits the transaction"""
        response = self.rds_client.commit_transaction(
            transactionId=transaction_id,
            resourceArn=self.RESOURCE_ARN,
            secretArn=self.SECRET_ARN)
        
        return bool(response)

    def query_adaptor(self, query: str, query_args: Union[dict, list, tuple, None],
                      fetch_mode: str) -> Union[dict, list, None]:
        """
        Wrapper for issuing SELECT queries written for PyMySQL(or other libraries that shares the same convention)
        """
        if query_args:
            new_query, new_query_args = self.convert_query_and_query_args_for_data_api(query, query_args)
            data_api_response = self.execute_query(new_query, new_query_args)
            return self.data_api_response_to_pymysql_response(data_api_response, fetch_mode)
        else:
            data_api_response = self.execute_query(query)
            return self.data_api_response_to_pymysql_response(data_api_response, fetch_mode)

    def execute_query(self, query: str, query_parameters: list = [], execute_many: bool = False) -> dict:
        """
        Wrapper function for issuing sql queries to RDS Data API

        Args:
            query (str): Sql query
            query_parameters (list): Parameter list or list of parameter lists
                                     to fill placeholders in query
            execute_many (bool): Mimics PyMySQL's executemany cursor call,
                                 runs batch_execute_statement when True,
                                 runs execute_statement when False.

        Returns:
            dict: Dictionary containing records along with Response and Column metadata.

        Raises:
            ClientError: When there is an error on AWS's side.
        """
        try:
            if execute_many:
                response = self.rds_client.batch_execute_statement(
                    secretArn=self.SECRET_ARN,
                    database=self.DATABASE_NAME,
                    resourceArn=self.RESOURCE_ARN,
                    sql=query,
                    parameterSets=query_parameters)
            else:
                response = self.rds_client.execute_statement(
                    secretArn=self.SECRET_ARN,
                    database=self.DATABASE_NAME,
                    resourceArn=self.RESOURCE_ARN,
                    sql=query,
                    parameters=query_parameters,
                    includeResultMetadata=True)
        except (self.rds_client.exceptions.BadRequestException,
                self.rds_client.exceptions.ForbiddenException) as error:
            # These exceptions should not happen, printing out to diagnose it later(development stage)
            print("From rds_client.execute_statement this error occured: ", error)
            print("Statement:", query)
            raise error

        return response

    def execute_query_in_transaction(self, query: str, transaction_id: str, query_parameters: list = [],
                                     execute_many: bool = False) -> dict:
        """
        Wrapper function for issuing sql queries to RDS Data API

        Args:
            query (str): Sql query
            transaction_id (str): Transaction id. This will be used to commit transaction by caller.
            query_parameters (list): Parameter list or list of parameter lists
                                     to fill placeholders in query
            execute_many (bool): Mimics PyMySQL's executemany cursor call,
                                 runs batch_execute_statement when True,
                                 runs execute_statement when False.

        Returns:
            dict: Dictionary containing records along with Response and Column metadata.

        Raises:
            ClientError: When there is an error on AWS's side.
        """
        try:
            if execute_many:
                response = self.rds_client.batch_execute_statement(
                    secretArn=self.SECRET_ARN,
                    database=self.DATABASE_NAME,
                    resourceArn=self.RESOURCE_ARN,
                    sql=query,
                    parameterSets=query_parameters,
                    transactionId=transaction_id)
            else:
                response = self.rds_client.execute_statement(
                    secretArn=self.SECRET_ARN,
                    database=self.DATABASE_NAME,
                    resourceArn=self.RESOURCE_ARN,
                    sql=query,
                    parameters=query_parameters,
                    transactionId=transaction_id,
                    includeResultMetadata=True)
        except (self.rds_client.exceptions.BadRequestException,
                self.rds_client.exceptions.ForbiddenException) as error:
            # These exceptions should not happen, printing out to diagnose it later(development stage)
            print("From rds_client.execute_statement this error occured: ", error)
            raise error

        return response

    def data_api_response_to_pymysql_response(self, data_api_response: dict,
                                              fetch_mode: str) -> Union[dict, list, None]:
        """
        Converts response from Data API to response structure of PyMySQL DictCursor

        Args:
            data_api_response (dict): Dictionary from execute_statement call of
                                      rds_client(includes ResultMetadata)
            fetch_mode (str): Mimics PyMySQL's fetchone and fetchall cursor methods.
                              Can be 'one' or 'all'

        Returns:
            dict: Formatted dictionary of first record when fetch_mode = 'one'
            list: List of formatted dictionaries when fetch_mode = 'all', empty list when there are no records
            None: When there are no records(with fetch_mode one) or fetch_mode is undefined or executed query was not SELECT
        """
        # If no columns are returned(e.g non-SELECT queries) and request was successful, return None
        if (not data_api_response.get('columnMetadata') and
            data_api_response['ResponseMetadata']['HTTPStatusCode'] == 200):
            return
        
        # Get column labels from metadata
        column_labels = [column['label'] for column in data_api_response['columnMetadata']]
        
        if len(data_api_response['records']) == 0:
            if fetch_mode == 'one':
                return
            elif fetch_mode == 'all':
                return []

        if fetch_mode == 'one':
            # Fetch the first record in records list and construct a dict from it with column labels
            row = data_api_response['records'].pop(0)
            row_values = [value if key != 'isNull' else None for type_value_pair in row for key, value in type_value_pair.items()]
            formatted_row = dict(zip(column_labels, row_values))

            return formatted_row
        elif fetch_mode == 'all':
            # Fetch all the records and construct a list of dicts from it with column labels
            formatted_rows = []
            for row in data_api_response['records']:
                row_values = [value if key != 'isNull' else None for type_value_pair in row for key, value in type_value_pair.items()]
                formatted_row = dict(zip(column_labels, row_values))
                formatted_rows.append(formatted_row)
            data_api_response['records'] = []

            return formatted_rows

    def convert_query_and_query_args_for_data_api(self, query: str, query_args: Union[dict, list, tuple]) -> tuple:
        """
        Adaptor to convert parameterized query and query arguments for PyMySQL to Data API Form
        Because PyMySQL has two types of parameterized queries:
            1) Where %s used as a placeholder and query_args is either list or tuple
            2) Where %(arg_name)s used as a placeholder and query_args is dictionary

        Args:
            query (str): Parameterized SQL query
            query_args (dict, list or tuple): Data structure contains data to fill placeholders in query

        Returns:
            (new_query, new_query_args): Tuple contains new parameterized query and new query arguments

        """
        if '%s' in query:
            return self.__convert_query_and_list_or_tuple_query_args(query, query_args)
        if re.search(r'%(\S+)s', query):
            return self.__convert_query_and_dict_query_args(query, query_args)
            
    def __convert_query_and_list_or_tuple_query_args(self, query: str, query_args: Union[list, tuple]) -> tuple:
        """
        Converts parameterized query and query arguments for PyMySQL to Data API Form

        Args:
            query (str): Parameterized SQL query
            query_args (list or tuple): List or tuple contains values to fill placeholders in query

        Returns:
            (new_query, new_query_args): Tuple contains new parameterized query and new query arguments

        """
        new_query = query
        new_query_args = []

        for index, query_arg in enumerate(query_args):
            # Alter query
            placeholder_name = f'parameter_{index}'
            new_query = new_query.replace('%s', f':{placeholder_name}', 1)

            # Modify query_arg and append to new query_args
            new_query_arg = self.__map_argument_pair_to_data_api_arg(placeholder_name, query_arg)
            new_query_args.append(new_query_arg)

        return new_query, new_query_args

    def __convert_query_and_dict_query_args(self, query: str, query_args: dict) -> tuple:
        """
        Converts parameterized query and query arguments for PyMySQL to Data API Form

        Args:
            query (str): Parameterized SQL query
            query_args (dict): Dictionary contains argument name and value pairs to fill placeholders in query

        Returns:
            (new_query, new_query_args): Tuple contains new parameterized query and new query arguments

        """
        new_query = query
        new_query_args = []

        for arg_name, arg_value in query_args.items():
            # Alter query
            new_query = new_query.replace(f'%({arg_name})s', f':{arg_name}')

            # Modify query_arg and append to new query_args
            new_query_arg = self.__map_argument_pair_to_data_api_arg(arg_name, arg_value)
            new_query_args.append(new_query_arg)

        return new_query, new_query_args

    def __map_argument_pair_to_data_api_arg(self, arg_name: str,
                                            arg_value: Union[str, int, float, bool, None]) -> dict:
        """
        Maps argument name and value pair to new Data API structure
        Example:
            __map_argument_pair_to_data_api_arg('arg_one', 3) -> {'name': 'arg_one', 'value': {'longValue': 3}}

        Args:
            arg_name (str): Arguments name
            arg_value (str, int, float, bool, datetime, None): Arguments value

        Returns:
            dict: Dict in format 
                {
                    'name': arg_name,
                    'value': {
                        Data API datatype: arg_value(or True when None)
                    }
                }
        """
        mapping = {
            "<class 'str'>": 'stringValue',
            "<class 'int'>": 'longValue',
            "<class 'float'>": 'doubleValue',
            "<class 'bool'>": 'booleanValue',
            "<class 'datetime.datetime'>": 'stringValue',
            "<class 'NoneType'>": 'isNull'
        }
        
        constructed_parameter = {'value': {}}

        # Map to it's Data API datatype
        arg_datatype = mapping[str(type(arg_value))]

        # Add typeHint for datetime arguments and convert them to string
        if 'datetime' in str(type(arg_value)):
            constructed_parameter['typeHint'] = 'TIMESTAMP'
            arg_value = str(arg_value)

        # Making arg_value True if it's null(to make it {isNull: True})
        arg_value = arg_value if arg_value != None else True

        constructed_parameter['name'] = arg_name
        constructed_parameter['value'][arg_datatype] = arg_value

        return constructed_parameter