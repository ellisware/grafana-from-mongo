######################################
# Custom Simple JSON Datasource for Grafana
#
# This is the main app file to be used
# in a python Flask dockerimage.
# image: https://github.com/tiangolo/uwsgi-nginx-flask-docker
#
# Mike Ellis
# December 3, 2020
######################################

#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# Database Structure
#
# mongodb
#   - table (database)
#     - group1 (collection)
#        - Date, column1, column2, etc.
#     - group2 (collection)
#        - Date, column1, column2, etc.
#   - timeseries (database)
#     - group1 (collection)
#        - Date, column1, column2, etc.
#     - group2 (collection)
#        - Date, column1, column2, etc.
#
#
# Grafana Structure
#
# table
#   - group1
#   - group2
# serie
#   - group1
#   - group2
#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


#----------- Imports ------------
from flask import Flask, request, json
import pandas as pd
import numpy as np
import datetime as dt
import pymongo as pm


#------ MongoB connection string ---
connection_string = "mongodb://172.17.0.1:27017/"

#------ Table Data Database --------
table_database = "table"

#------ Timeseries Database --------
timeseries_database = "timeseries"

# ----- Init for the Flask Application -----
app = Flask(__name__)


#~~~~~~~~~~~~~~~~~~~~~~~~~~
# Append CORS Headers before sending
#
# Warning: this also allows cross-site scripting
#~~~~~~~~~~~~~~~~~~~~~~~~~~
@app.after_request
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Accpet, Content-Type'
    return response


#~~~~~~~~~~~~~~~~~~~~~~~~~~
# Default Route /
#
# The default route must return "OK" with status 200
# Used to validate connection
#~~~~~~~~~~~~~~~~~~~~~~~~~~
@app.route("/")
def index():
    return "OK"


#~~~~~~~~~~~~~~~~~~~~~~~~~~
# Search Route  /search
#
# Returns a list of the names of
# the available data this datasource has
#~~~~~~~~~~~~~~~~~~~~~~~~~~
@app.route('/search', methods=['GET', 'POST'])
def search():

    client = pm.MongoClient(connection_string)

    # Get a list of all table collections
    db = client[table_database]
    collections = db.list_collection_names()

    # Get a list of all timeseries collections
    db = client[timeseries_database]
    collections += db.list_collection_names()

    # Remove duplicates
    collections = list(set(collections))

    return json.dumps(collections)


#~~~~~~~~~~~~~~~~~~~~~~~~~~
# Query Route  /query
#
# Returns the data for each data
#
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~
@app.route('/query', methods=['GET', 'POST'])
def query():

    # Establish the connection to the database
    client = pm.MongoClient(connection_string)

    # Identify the data that is being requested
    posted = request.get_json()


    # %%%%%%%%%%%%%%%%%%%%%%%%%%%
    # Table Output Format
    # {
    #   "columns": [
    #     {
    #       "text": "Header1",
    #       "type": "string"
    #     },
    #     {
    #       "text": "Header2",
    #       "type": "string"
    #     }
    #   ],
    #   "rows": [
    #     [
    #       "cell 1-1",
    #       "cell 1-2"
    #     ],
    #     [
    #       "cell 2-1",
    #       "cell 2-2"
    #     ]
    #   ],
    #   "type": "table"
    # }
    # %%%%%%%%%%%%%%%%%%%%%%%%%%

    # If the requested data type is a table
    if posted['targets'][0]['type'] == 'table':

        # Get the collection name
        series = request.json['targets'][0]['target']

        # Connect to the specified database and collection
        db = client[table_database]
        col = db[series]

        # Read the collection
        cursor = col.find({},{'_id':False})
        entries = list(cursor)
        df = pd.DataFrame(entries)

        # Condition the Data
        df.fillna('', inplace=True)

        # Get the header columns
        header = []

        for column in df.columns:
            cell = {}
            cell["text"] = column
            cell["type"] = "string"
            header.append(cell)

        # Get the rows
        rows = []

        # Iterate through the rows
        for row in df.itertuples(index=False):
            line = []
            for cell in row:
                line.append(cell)
            rows.append(line)

        # Assemble the complete list
        result = {}
        result["columns"] = header
        result["rows"] = rows
        result["type"] = "table"
        result = [result]


    # %%%%%%%%%%%%%%%%%%%%%%%%%%%
    # Timeserie Output Format
    # {
    #   "target": "Header 1",
    #   "datapoints": [
    #     [
    #       1, 1607242888000
    #     ],
    #     [
    #       2.5, 1607242934000,
    #     ]
    #   ]
    # }
    # %%%%%%%%%%%%%%%%%%%%%%%%%%


    # If the requested data type is series
    # Retrieve a single series, from a single column only
    else:

        # Get the collection name
        series = request.json['targets'][0]['target']

        # Get the date range as strings
        start, end = request.json['range']['from'], request.json['range']['to']

        # Convert the range to datetime objects
        # The grafana string is 2020-12-06T13:36:49.759Z
        start = dt.datetime.strptime(start, '%Y-%m-%dT%H:%M:%S.%fZ')
        end = dt.datetime.strptime(end, '%Y-%m-%dT%H:%M:%S.%fZ')

        # Connect to the specified database and collection
        db = client[timeseries_database]
        col = db[series]

        # Read the collection
        cursor = col.find({"Date":{"$gte": start, "$lte":end}},{'_id':False})
        entries = list(cursor)
        df = pd.DataFrame(entries)

        # Avoid index issues if the dataframe is empty for the date range
        if df.empty:
            df = pd.DataFrame(columns = ["Date", "Datapoint"])

        # Condition the Data
        df.fillna('', inplace=True)
        df["Date"] = df["Date"].astype('int64')//1e9
        df["Date"] = df["Date"].astype('int64')*1000

        # Rearrange the order of the colums
        # Placing Date at the end
        list_of_columns = list(df.columns.values)
        list_of_columns.pop(list_of_columns.index("Date"))
        df = df[list_of_columns + ["Date"]]
        
        # Retrieve each Row
        row_list =[]

        # Iterate over each row
        for rows in df.itertuples(index=False):
            # Create list for the current row
            row = []
            for cell in rows:
                row.append(cell)

            # append the list to the final list
            row_list.append(row)


        # Create the output list
        result = []
        result.append({'datapoints':row_list, 'target':series})


    # Convert the list to JSON and serve it
    return json.dumps(result)
