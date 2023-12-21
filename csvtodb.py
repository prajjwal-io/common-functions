import json
import os
import pandas
import login as login
import concurrent.futures

# Set up database connection
connection = login.connection("dev")

cursor = connection.cursor()


# Define the  parent directory containing the csv files
#parent_dir = os.getcwd()
files = os.listdir()
csv_files = [f for f in files if f[-3:] == 'csv']
#print(csv_files)

# data_file = parent_dir + '/6sense.csv'

# Load the Data.
#sixsense_df: pandas.DataFrame = pandas.read_csv(
    #data_file,
    #infer_datetime_format=True,
    #parse_dates=True
#)

# Convert the DataFrame to a RecordSet.
#df_records = sixsense_df.values.tolist()

# Define the maximum number of threads to use
#max_threads = 4

# Define the Insert Query.
sql_insert = """
INSERT INTO [dev].[dbo].[LinkedinID]
 (
    [Query],
    [QueryType],
    [TrackingID],
    [ID],
    [CompanyName],
    [Type],
    [SearchRank]
)
VALUES
(
    ?, ?, ?, ?, ?, ?, ?
)
"""
# Create the Table.
#cursor.execute(create_table_query)

# Commit the Table.
#cursor.commit()

# Process the JSON files concurrently using a thread pool
#with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
    #executor.map(sql_insert, df_records)

# Commit the changes and close the database connection
#connection.commit()
#connection.close()

# Read the CSV file in chunks and insert the data into the SQL database in batches
#chunksize = 10000
for file in csv_files:
    print(file)
    df_records = pandas.read_csv(file)
    df_records = df_records.values.tolist()
    #print(df_records)

    # Process the records concurrently using a thread pool
    #with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        #executor.map(cursor.execute, [(sql_insert, record) for record in df_records])
    try:
        cursor.fast_executemany = True
        cursor.executemany(sql_insert, df_records)
    except Exception as e:
        print(e)


    # Commit the changes after each chunk
    connection.commit()
    print('SENT TO DB')

# Close the database connection
connection.close()
