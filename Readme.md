## Large File Processing
**To ingest large amount of given data and update it as needed.**


### Run
```
python app.py
# Also added Dockerfile
```

### Tables ( Collections )
```
1. 'products'

```json
{
    "name": 
        {
            "type": "string",
            "minlength": 1,
            "required": true
        },
    "sku":
        {
            "type": "string",
            "minlength": 1,
            "required": true
        },
    "sku_id": 
        {
            "type": "string",
            "minlength": 1,
            "required": true
        },
    "description":
        {
            "type": "string",
            "minlength": 1,
            "required": true
        }
}


2. 'aggregated'

```json
{
    "name":
        {
            "type": "string",
            "minlength": 1,
            "required": true
        },
    "no_of_products":
        {
            "type": "int",
            "minlength": 1,
            "required": true
        }
}
```

### Schema Generation
```
1. Automatically on the start of app: python app.py
   or
2. python schema/create_db_schema.py
```

### Points To Achieve - Done
```
1. OOPS followed
2. Used ThreadPoolExecutor by concurrent.futures to acheive multi-threading in insertion and updation of data into db.
3. Provided support of updating the data into all collections.
4. All product details are to be ingested into a single table.
5. An aggregated table on above rows with `name` and `no. of products` as the columns.
```

### Points To Achieve - Not Done
```
```

### API endpoints
```
1. Endpoint: /ingest_file

   Description: Ingest the given file into db

   Type: GET
   
   Response example: 
   ```json
   {'Exception': None, 'IsSuccessfullyIngested': True, 'TimeTaken': '2.20'}
   
   
   
2. Endpoint: /insert_update

   Description: Inserts or updates the collection as per the input data given

   Type: POST
   
   Request body example: 
   ```json
      {
      "data": [
        {
        	"name": "Nicola Tesla",
          	"sku": "scientist",
          	"description": " He invented the first alternating current (AC) motor and developed AC generation and transmission technology."
          },
        {
         	"name": "Warren Buffet",
          	"sku": "Berkshire Hathaway",
          	"description": "Warren Edward Buffett is an American business magnate, investor, and philanthropist. He is currently the chairman and CEO of Berkshire Hathaway."}
      	]
      }
      
   Response example:
   ```json
    {'Total Success': 2, 'Total Failed': 0, 'Exception': None, 'IsSuccessfull': True}
```

### Unit Test
```
python -m unittest
```

### Key Project Points
```
1. Wrapped into a Flask api, to run on the fly.
2. Inserted 500,000 rows under 1 min 40 secs, by making use of 20 threads parallely ( 4 cores, 5 threads each core at a time ).
3. Added unit test cases.
```

### Tools and Technologies
```
Language: Python
Api Framework: Flask
DB: MongoDB
DB Type: NoSql
Parallel processing: concurrent.futures
```

### To improve
```
Will try to learn pyspark and implement using pyspark.
```