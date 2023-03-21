import streamlit as st
import pandas as pd
from config import *
import base64
from PIL import Image

im = Image.open(r'D:\Raja\POC_1\3IINFOLTD.NS_BIG.png')
st.set_page_config(page_title='3i future tech', layout='centered', page_icon =im , initial_sidebar_state = 'auto')
def get_base64(bin_file):
    with open(bin_file, 'rb') as f:
        data = f.read()
    return base64.b64encode(data).decode()
def set_background(png_file):
    bin_str = get_base64(png_file)
    page_bg_img = '''
    <style>
    .stApp {
    background-image: url("data:image/png;base64,%s");
    background-size: cover;
    }
    </style>
    ''' % bin_str
    st.markdown(page_bg_img, unsafe_allow_html=True)

set_background(r'D:\Raja\POC_1\back3.jpg')



# Main function to run the Streamlit app
def main():
    # Create a dictionary to map the database options to their corresponding connection functions
    db_options = {
        "MySQL": connect_mysql,
        "PostgreSQL": connect_postgres,
        "MSSQL": connect_mssql,
    }

    # Create a dictionary to map the aggregation functions to their corresponding SQL queries
    agg_options = {
        "None": "",
        "SUM": "SUM",
        "COUNT": "COUNT",
        "AVG": "AVG",
        "MIN": "MIN",
        "MAX": "MAX",
    }

    # Create the Streamlit app layout
    st.title(':blue[Welcome to Data Engineer world] :sunglasses:')
    selected_db = st.selectbox(":red[Select a database]", ["Select a database","MySQL", "PostgreSQL", "MSSQL"])
    if selected_db in ["MySQL", "PostgreSQL", "MSSQL"]:
        host = st.text_input(":red[Host name]")
        port = st.text_input(":red[Port number]")
        db_name = st.text_input(":red[Database name]")
        username = st.text_input(":red[Username]")
        password = st.text_input(":red[Password]", type="password")

    # Create a button to connect to the selected database
        if st.button("Connect"):
            # Get the connection function for the selected database
            connect_func = db_options[selected_db]
            # Call the connection function with the provided credentials
            connection, message = connect_func(host, port, db_name, username, password)
            # Show the connection message
            tables,cursor = show_tables(connection,message,selected_db)
            # Create a dropdown to select a table
            selected_table = st.selectbox(":red[Select a table]",tables)
            if selected_table != "Select a table":
                col_names = show_data(cursor, selected_table, tables)
                selected_column = st.selectbox(":red[Select a column]",col_names)
                if selected_column != "Select a column":
                    selected_agg = st.selectbox(":red[Select an aggregation function]", ["COUNT","SUM", "AVG", "MIN", "MAX"])
                    if selected_agg != "None":
                        agg_query = agg_options[selected_agg]
                        query=f"SELECT {agg_query}({selected_column})  FROM {selected_table} LIMIT 100;"
                        st.write(f":green[Showing {selected_db} query from {selected_table} table]")
                        st.code(query,language='sql')
                        cursor.execute(query)
                        rows = cursor.fetchall()
                        st.write(f":green[Showing data from {selected_table} table]")
                        st.write(pd.DataFrame(rows, columns=[f"{selected_agg}({selected_column})"]))

            connection.close()


if __name__ == "__main__":
    main()
