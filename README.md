# Data API adaptor
Want to use Rds Data API without breaking your code? Use this :palms_up_together:  

## Usage
### Old code
```python
import pymysql

# Create connection and cursor
connection = pymysql.connect('Credentials here')
cursor = connection.cursor()

# Execute queries
cursor.execute(simple_query)
cursor.execute(parameterized_query, query_parameters)
cursor.executemany(query, data)
```                  

### New code
```python
import RdsDataApiClient

# Create client object
data_api_client = RdsDataApiClient('Credentials here')

# Execute queries
data_api_client.execute(query)
data_api_client.execute(parameterized_query, query_parameters)
data_api_client.executemany(query, data)
```

## License
[MIT](https://choosealicense.com/licenses/mit/)
