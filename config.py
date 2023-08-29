import mysql.connector
import psycopg2
import pyodbc
import streamlit as st
import pandas as pd
import sqlite3

# Function to establish a MySQL connection
def connect_mysql(host, port, db_name, username, password):

    # Try to connect to the MySQL database
    try:
        # Create a connection object
        connection = mysql.connector.connect(
            host=host,
            port=port,
            database=db_name,
            user=username,
            password=password,
        )
        # Check if the connection is successful
        if connection.is_connected():
            # Return the connection object and a success message
            return connection, "Successfully connected to MySQL!"
    # If the connection fails, raise an exception
    except Exception as e:
        # Return None and the error message
        return None, str(e)



# Function to establish a PostgreSQL connection
def connect_postgres(host, port, db_name, username, password):
    # Try to connect to the PostgreSQL database
    try:
        # Create a connection object
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=db_name,
            user=username,
            password=password,
        )
        # Check if the connection is successful
        if connection:
            # Return the connection object and a success message
            return connection, "Successfully connected to PostgreSQL!"
    # If the connection fails, raise an exception
    except Exception as e:
        # Return None and the error message
        return None, str(e)

# Function to establish a MSSQL connection
def connect_mssql(host, port, db_name, username, password):
    # Try to connect to the MSSQL database
    try:
        # Create a connection object
        connection = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={host},{port};"
            f"DATABASE={db_name};"
            f"UID={username};"
            f"PWD={password};"
        )
        # Check if the connection is successful
        if connection:
            # Return the connection object and a success message
            return connection, "Successfully connected to MSSQL!"
    # If the connection fails, raise an exception
    except Exception as e:
        # Return None and the error message
        return None, str(e)
    
# Function to establish a SQLite connection
def connect_sqlite(dataframe):
    # Try to connect to the SQLite database
    try:
        # Create a connection object
        connection = sqlite3.connect('test.db')
        # Write the DataFrame to the SQLite database
        dataframe.to_sql(name='my_table', con=connection, if_exists='replace', index=False)
        # Print a success message
        st.success("Data stored in SQLite3!")
        # Create a query to select all data from the `my_table` table
        query = "SELECT * FROM my_table"
        # Read the data from the database
        df = pd.read_sql(query, connection)
        # Return the connection object and the DataFrame
        return connection, df
    # If the connection fails, raise an exception
    except Exception as e:
        # Return None and the error message
        return None, str(e)



def show_tables(connection, message, selected_db):
    # Check if the connection is established
    if connection:
        # Print a success message
        st.success(message)
        # Create a cursor object
        cursor = connection.cursor()
        # Select the tables based on the selected database
        if selected_db == "MySQL":
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]
        elif selected_db == "PostgreSQL":
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            tables = [table[0] for table in cursor.fetchall()]
        elif selected_db == "MSSQL":
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
            tables = [table[0] for table in cursor.fetchall()]
        elif selected_db == "Excel":
            cursor.execute("""SELECT name FROM sqlite_master WHERE type='table';""")
            tables = [table[0] for table in cursor.fetchall()]
        # Return the cursor object and the list of tables
        return cursor, tables
    # If the connection is not established, print a warning message
    else:
        st.warning(message)


def show_data(cursor, selected_table, tables):
    # Check if the selected table is in the list of tables
    if selected_table in tables:
        # Execute a SQL query to retrieve data from the selected table
        cursor.execute(f"SELECT * FROM {selected_table} LIMIT 10;")    
        # Display the SQL query for reference
        st.code(f'SELECT * FROM {selected_table};', language='sql')    
        # Fetch the retrieved rows
        rows = cursor.fetchall()   
        # Get the column names from the cursor description
        col_names = [desc[0] for desc in cursor.description]   
        # Display the first 10 rows of the data in a DataFrame
        st.write(pd.DataFrame(rows, columns=col_names).head())     
    # Return the column names for potential further use
    return col_names


def show_column(cursor, selected_table, tables):
    # Check if the selected table is in the list of tables
    if selected_table in tables:
        # Execute a SQL query to retrieve data from the selected table
        cursor.execute(f"SELECT * FROM {selected_table} LIMIT 10;")
        # Get the column names from the cursor description
        col_names = [desc[0] for desc in cursor.description]
    # Return the column names for potential further use
    return col_names


def list_convertin(selected_columns):
    # Convert the list to a string
    selected_columns_out = str(selected_columns)
    # Remove the opening square bracket '['
    selected_columns_out = selected_columns_out.replace("[", "")
    # Remove the closing square bracket ']'
    selected_columns_out = selected_columns_out.replace("]", "")
    # Remove any single quotes "'"
    selected_columns_out = selected_columns_out.replace("'", "") 
    # Return the modified string
    return selected_columns_out


def generate_query_fun(selected_agg, selected_table, cursor, options_out, metrics_out, groupby_col_out):
    # Check if the selected aggregation is "GROUP BY"
    if selected_agg == "GROUP BY":
        # Create a query for GROUP BY aggregation
        output_query = f"SELECT {metrics_out} FROM {selected_table} GROUP BY {groupby_col_out} LIMIT 100 OFFSET 0;"      
    else:
        # Create a query for non-GROUP BY aggregation
        output_query = f"SELECT {options_out} FROM {selected_table} LIMIT 100 OFFSET 0;"
    # Display the generated SQL query
    st.code(output_query, language='sql')
    # Execute the SQL query
    cursor.execute(output_query)
    # Fetch the retrieved rows
    rows = cursor.fetchall()
    # Display a message indicating the table being shown
    st.write(f":green[Showing data from {selected_table} table]")
    # Display the data in a DataFrame
    st.write(pd.DataFrame(rows, columns=[f"{options_out}"][0].split(",")))


def user_query_fun(cursor, tables):
    # Get the user's SQL query from a text area in the sidebar
    sql_query = st.sidebar.text_area("Enter your own SQL Query")
    # Display a message indicating that the user should write a SQL query
    st.write(f":red[WRITE A SQL QUERY ON {tables}]")  
    # Execute the user's SQL query
    cursor.execute(sql_query)  
    # Fetch the retrieved rows
    rows = cursor.fetchall()
    # Display a message indicating that data from the specified table is shown
    st.write(f":green[Showing data from {tables} table]")
    # Get the column names from the cursor description
    column_name = [col[0] for col in cursor.description]
    # Display the retrieved data in a DataFrame with appropriate column names
    st.write(pd.DataFrame(rows, columns=column_name))


def add_metric_fun(cursor, sql_query, matric_column_name, selected_table, data_type, connection):
    # Create the query to add a new metric column to the table using the provided SQL query
    matric_query = f'''ALTER TABLE {selected_table} ADD COLUMN {matric_column_name} {data_type} GENERATED ALWAYS AS (
        {sql_query}
    ) STORED;'''
    # Execute the metric query to add the new column
    cursor.execute(matric_query) 
    # Commit the changes to the database
    connection.commit()
    # Display a message indicating that data from the specified table is shown
    st.write(f":green[Showing data from {selected_table} table]")
    # Execute a query to retrieve the data from the updated table
    cursor.execute(f'SELECT * FROM {selected_table}')
    # Fetch the retrieved rows
    rows = cursor.fetchall()
    # Get the column names from the cursor description
    column_name = [col[0] for col in cursor.description] 
    # Display the retrieved data in a DataFrame with appropriate column names
    st.write(pd.DataFrame(rows, columns=column_name))

def join_metric_fun(cursor, right_table, left_table, join, right_selected_columns,
                    left_selected_columns, right_condition_column, left_condition_column):
    # Create result lists for columns from the right and left tables with table prefixes
    right_result_list = [f"{right_table}.{column}" for column in right_selected_columns]
    left_result_list = [f"{left_table}.{column}" for column in left_selected_columns] 
    # Create the join query
    join_query = f'''SELECT {list_convertin(right_result_list)}, {list_convertin(left_result_list)} 
    FROM {right_table} {join} {left_table} 
    ON {right_table}.{right_condition_column} = {left_table}.{left_condition_column};
    '''
    # Display the generated SQL join query
    st.code(join_query, language='sql')   
    # Execute the join query
    cursor.execute(join_query)
    # Display a message indicating the output of the join query
    st.write(f":green[Output of join query]")   
    # Fetch the retrieved rows
    rows = cursor.fetchall()   
    # Logic for handling the DataFrame creation based on selected columns
    if right_selected_columns[0] == '*' and left_selected_columns[0] != '*':
        # Create column names from the right table
        cursor.execute(f"SELECT * FROM {right_table} LIMIT 1;")
        col_names = [desc[0] for desc in cursor.description]
        right_result_list = [f"{right_table}.{column}" for column in col_names]       
        # Display the DataFrame with combined column names
        st.write(pd.DataFrame(rows, columns=right_result_list + left_result_list))       
    elif right_selected_columns[0] == '*' and left_selected_columns[0] == '*':
        # Create column names from both tables
        cursor.execute(f"SELECT * FROM {left_table} LIMIT 1;")
        col_names_left = [desc[0] for desc in cursor.description]
        left_result_list = [f"{left_table}.{column}" for column in col_names_left]       
        cursor.execute(f"SELECT * FROM {right_table} LIMIT 1;")
        col_names_right = [desc[0] for desc in cursor.description]
        right_result_list = [f"{right_table}.{column}" for column in col_names_right]     
        # Display the DataFrame with combined column names
        st.write(pd.DataFrame(rows, columns=right_result_list + left_result_list))     
    elif left_selected_columns[0] == "*" and right_selected_columns[0] != '*':
        # Create column names from the left table
        cursor.execute(f"SELECT * FROM {left_table} LIMIT 1;")
        col_names = [desc[0] for desc in cursor.description]
        left_result_list = [f"{left_table}.{column}" for column in col_names] 
        # Display the DataFrame with combined column names
        st.write(pd.DataFrame(rows, columns=right_result_list + left_result_list))
    else:
        # Display the DataFrame with original column names from both tables
        st.write(pd.DataFrame(rows, columns=right_result_list + left_result_list))

# adding the new column to existing table
def alter_function(cursor,connection,table_name,output):
    query=f'''alter table {table_name} {output};'''    
    cursor.execute(query)
    connection.commit()

def delete_function(cursor,connection,selected_table,selected_column):
    query=f'''ALTER TABLE {selected_table} DROP COLUMN {selected_column};'''    
    cursor.execute(query)
    connection.commit()