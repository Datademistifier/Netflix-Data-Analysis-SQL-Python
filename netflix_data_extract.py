import pandas as pd
import sqlalchemy as sal

# Load the data into a DataFrame
df = pd.read_csv('netflix_titles.csv') # Loading data from a CSV file

# Create an SQLAlchemy engine to connect to the SQL server
engine = sal.create_engine('<sql server link>') # Replace '<sql server link>' with the actual SQL server link
conn = engine.connect() # Establishing the connection to the SQL server

# Write the DataFrame to a SQL table
df.to_sql('netflix_raw', con=conn, index=False, if_exists='append') # Appending the DataFrame to the 'netflix_raw' table

# Close the connection to the SQL server
conn.close() # Closing the connection

# Display the first 5 rows of the DataFrame
print(df.head())

# Filter the DataFrame to get all columns for a specific show ID
print(df[df.show_id == '<showid where question mark is coming>']) # Displaying all columns for the show ID 's5023' example for nvarchar type


## max(df.<Column_name>.str.len()) ##repeat for every column to find the max length we need in sql for them

##max(df.description.dropna().str.len()) ### to avoid null values becoming as max


# Calculate the maximum length of non-null values in the 'description' column to avoid null values
max_description_length = max(df.description.dropna().str.len())
print("Max length of description:", max_description_length)

# Calculate the number of missing values in each column
missing_values = df.isna().sum()
print("Missing values in each column:\n", missing_values)
