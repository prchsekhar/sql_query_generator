import mysql.connector
import psycopg2
import pyodbc
import streamlit as st
import pandas as pd
import sqlite3

# Function to establish a MySQL connection
def connect_mysql(host, port, db_name, username, password):
    try:
        connection = mysql.connector.connect(
            host=host,
            port=port,
            database=db_name,
            user=username,
            password=password,
        )
        if connection.is_connected():
            return connection, "Successfully connected to MySQL!"
    except Exception as e:
        return None, str(e)

# Function to establish a PostgreSQL connection
def connect_postgres(host, port, db_name, username, password):
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=db_name,
            user=username,
            password=password,
        )
        if connection:
            return connection, "Successfully connected to PostgreSQL!"
    except Exception as e:
        return None, str(e)

# Function to establish a MSSQL connection
def connect_mssql(host, port, db_name, username, password):
    try:
        connection = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={host},{port};"
            f"DATABASE={db_name};"
            f"UID={username};"
            f"PWD={password};"
        )
        if connection:
            return connection, "Successfully connected to MSSQL!"
    except Exception as e:
        return None, str(e)
    
# Function to establish a SQLite connection   
def connect_sqlite(dataframe):
    try:
        connection = sqlite3.connect('test.db')
        dataframe.to_sql(name='my_table', con=connection, if_exists='replace', index=False)
        st.success("Data stored in SQLite3!")
        query = "SELECT * FROM my_table"
        df = pd.read_sql(query, connection)
        return connection,df
    except Exception as e:
        return None,str(e)


def show_tables(connection,message,selected_db):
    if connection:
        st.success(message)
        cursor = connection.cursor()
        if selected_db == "MySQL":
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]
        elif selected_db == "PostgreSQL":
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            tables = [table[0] for table in cursor.fetchall()]
        elif selected_db == "MSSQL":
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
            tables = [table[0] for table in cursor.fetchall()]
        elif selected_db=="Excel":
            cursor.execute("""SELECT name FROM sqlite_master WHERE type='table';""")
            tables = [table[0] for table in cursor.fetchall()]
        return cursor,tables
    else:
        st.warning(message)
    

def show_data(cursor,selected_table,tables):
    if selected_table in tables:
        cursor.execute(f"SELECT * FROM {selected_table} LIMIT 10;")
        st.code(f'SELECT * FROM {selected_table};',language='sql')
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        st.write(pd.DataFrame(rows, columns=col_names).head())
    return col_names

def show_column(cursor,selected_table,tables):
    if selected_table in tables:
        cursor.execute(f"SELECT * FROM {selected_table} LIMIT 10;")
        col_names = [desc[0] for desc in cursor.description]
    return col_names

def list_convertin(selected_columns):
    selected_columns_out = str(selected_columns)
    selected_columns_out = selected_columns_out.replace("[","")
    selected_columns_out = selected_columns_out.replace("]","")
    selected_columns_out = selected_columns_out.replace("'","")
    return selected_columns_out

def generate_query_fun(selected_agg,selected_table,cursor,options_out,metrics_out,groupby_col_out):
    if selected_agg == "GROUP BY":
        output_query = f"SELECT {metrics_out} FROM {selected_table} GROUP BY {groupby_col_out} LIMIT 100  OFFSET 0;"      
    else:
        output_query = f"SELECT {options_out} FROM {selected_table}  LIMIT 100  OFFSET 0;"
    st.code(output_query,language='sql')
    cursor.execute(output_query)
    rows = cursor.fetchall()
    st.write(f":green[Showing data from {selected_table} table]")
    st.write(pd.DataFrame(rows, columns=[f"{options_out}"][0].split(",")))

def user_query_fun(cursor,tables):
    sql_query = st.sidebar.text_area("Enter your own SQL Query")
    st.write(f":red[WRITE A SQL QUERY ON {tables}]")
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    st.write(f":green[Showing data from {tables} table]")
    column_name =  [col[0] for col in cursor.description]
    st.write(pd.DataFrame(rows,columns=column_name))

def add_metric_fun(cursor,sql_query,matric_column_name,selected_table,data_type,connection):
    matric_query =f'''alter table {selected_table} add column {matric_column_name} {data_type} GENERATED ALWAYS AS (
	        {sql_query}
            ) STORED;'''
    cursor.execute(matric_query)
    connection.commit()
    st.write(f":green[Showing data from {selected_table} table]")
    cursor.execute(f'select * from {selected_table}')
    rows = cursor.fetchall()
    column_name =  [col[0] for col in cursor.description]
    st.write(pd.DataFrame(rows,columns=column_name))

def join_metric_fun(cursor,right_table,left_table,join,right_selected_columns,left_selected_columns,right_condition_column,left_condition_column):
    right_result_list = [f"{right_table}.{column}" for column in right_selected_columns]
    left_result_list =  [f"{left_table}.{column}" for column in left_selected_columns]
    join_query =f'''SELECT {list_convertin(right_result_list)},{list_convertin(left_result_list)} 
    FROM {right_table} {join} {left_table} 
    ON {right_table}.{right_condition_column} = {left_table}.{left_condition_column};
    '''
    st.code(join_query,language='sql')
    cursor.execute(join_query)
    st.write(f":green[output of join query]")
    rows = cursor.fetchall()
    if right_selected_columns[0]=='*' and left_selected_columns[0]!='*':
        cursor.execute(f"SELECT * FROM {right_table} LIMIT 1;")
        col_names = [desc[0] for desc in cursor.description]
        right_result_list = [f"{right_table}.{column}" for column in col_names]
        st.write(pd.DataFrame(rows,columns=right_result_list+left_result_list))
    elif right_selected_columns[0]=='*' and left_selected_columns[0]=='*':
        cursor.execute(f"SELECT * FROM {left_table} LIMIT 1;")
        col_names = [desc[0] for desc in cursor.description]
        left_result_list = [f"{left_table}.{column}" for column in col_names]
        cursor.execute(f"SELECT * FROM {right_table} LIMIT 1;")
        col_names = [desc[0] for desc in cursor.description]
        right_result_list = [f"{right_table}.{column}" for column in col_names]
        st.write(pd.DataFrame(rows,columns=right_result_list+left_result_list))
    elif left_selected_columns[0]=="*"and right_selected_columns[0]!='*':
        cursor.execute(f"SELECT * FROM {left_table} LIMIT 1;")
        col_names = [desc[0] for desc in cursor.description]
        left_result_list = [f"{left_table}.{column}" for column in col_names]
        st.write(pd.DataFrame(rows,columns=right_result_list+left_result_list))
    else:
        st.write(pd.DataFrame(rows,columns=right_result_list+left_result_list))
