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
Image = Image.open(r'D:\Raja\POC_1\3i-logo-black-bkg-1.png')
st.image(Image)



# Main function to run the Streamlit app
def main():
    # Create a dictionary to map the database options to their corresponding connection functions
    db_options = {
        "MySQL": connect_mysql,
        "PostgreSQL": connect_postgres,
        "MSSQL": connect_mssql,
        "Excel": connect_sqlite
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
    st.title(":blue[Welcome to Data Engineer's world]:sunglasses:")
    selected_db = st.selectbox(":red[Select a database]", ["Select a database","MySQL", "PostgreSQL", "MSSQL","Excel"])
    if selected_db in ["MySQL", "PostgreSQL", "MSSQL"]:
        host = st.text_input(":red[Host name]")
        port = st.text_input(":red[Port number]")
        db_name = st.text_input(":red[Database name]")
        username = st.text_input(":red[Username]")
        password = st.text_input(":red[Password]", type="password")
        if host=='' or port=='' or db_name=='' or username=='' or password=='' :
            st.write(":red[Pleace fill the details]")
        else:
            connect_func = db_options[selected_db]
            # Call the connection function with the provided credentials
            connection, message = connect_func(host, port, db_name, username, password)
            cursor,tables = show_tables(connection,message,selected_db)
            # Create a dropdown to select a table
            selected_table = st.selectbox(":red[Select a table]",tables)
            if selected_table != "Select a table":
                col_names = show_data(cursor, selected_table, tables)
                selected_column = st.multiselect(":red[Select a column]",col_names)
                if selected_column != "Select a column":
                   from_qry = f"FROM {selected_table}"
                selected_agg = st.selectbox(":red[Select an aggregation function]",["GROUP BY", "COUNT", "SUM", "DISTINCT", "MIN", "MAX", "AVG"])
                if selected_agg == "GROUP BY":
                    groupby_col = st.multiselect(':red[Choose columns to apply group by]', selected_column)
                    groupby_col_out = str(groupby_col)
                    groupby_col_out = groupby_col_out.replace("[","")
                    groupby_col_out = groupby_col_out.replace("]","")
                    groupby_col_out = groupby_col_out.replace("'","")

                    metrics = st.selectbox(':red[Choose Metrics]', ( "COUNT", "SUM", "DISTINCT", "MIN", "MAX", "AVG"))
                    col_metrics = st.multiselect(':red[Choose columns to apply metrics]', selected_column)

                    metrics_out = [f"{metrics}({col}) AS {col}" for col in col_metrics] + list(set(selected_column) - set(col_metrics))
                    metrics_out = str(metrics_out)
                    metrics_out = metrics_out.replace("[","")
                    metrics_out = metrics_out.replace("]","")
                    metrics_out = metrics_out.replace("'","")

                    output_query = f"SELECT {metrics_out} {from_qry} GROUP BY {groupby_col_out} LIMIT 100  OFFSET 0;"
                    st.code(output_query,language='sql')
                    cursor.execute(output_query)
                    rows = cursor.fetchall()
                    st.write(f":green[Showing data from {selected_table} table]")
                    st.write(pd.DataFrame(rows, columns=[f"{metrics_out}"][0].split(",")))
                else:
                    #option_col = st.multiselect('Choose columns to apply the method', choose_first_column)
                    options_out = [f"{selected_agg}({col2}) AS {col2}" for col2 in selected_column]
                    options_out  = str(options_out)
                    options_out = options_out.replace("[","")
                    options_out = options_out.replace("]","")
                    options_out = options_out.replace("'","")

                    output_query = f"SELECT {options_out} {from_qry} LIMIT 100  OFFSET 0;"
                    st.code(output_query,language='sql')
                    cursor.execute(output_query)
                    rows = cursor.fetchall()
                    st.write(f":green[Showing data from {selected_table} table]")
                    st.write(pd.DataFrame(rows, columns=[f"{options_out}"][0].split(",")))

            else:
                st.write(message)      
    elif selected_db=="Excel":
        uploaded_files = st.file_uploader("Choose a CSV file")
        if uploaded_files is not None:
            dataframe = pd.read_csv(uploaded_files, encoding= 'unicode_escape')
            connection,df=connect_sqlite(dataframe)
            st.write(df.head())
            message='Uploaded Successfully'
            cursor,tables = show_tables(connection,message,selected_db)
            cursor = cursor.execute("SELECT tbl_name, (SELECT GROUP_CONCAT(name, ',') FROM PRAGMA_TABLE_INFO(tbl_name)) as columns FROM sqlite_schema WHERE type = 'table';")
            column_name = [table[1] for table in cursor.fetchall()]
            column_names=column_name[0].split(",")
            selected_column = st.multiselect(":red[Select a column]",['Select a column']+column_names)
            if selected_column != "Select a column":
                    from_qry = f"FROM {tables[0]}"
                    selected_agg = st.selectbox(":red[Select an aggregation function]", ["select a function"]+["GROUP BY", "COUNT", "SUM", "DISTINCT", "MIN", "MAX", "AVG"])
                    if selected_agg == "GROUP BY":
                        groupby_col = st.multiselect(':red[Choose columns to apply group by]', selected_column)
                        groupby_col_out = str(groupby_col)
                        groupby_col_out = groupby_col_out.replace("[","")
                        groupby_col_out = groupby_col_out.replace("]","")
                        groupby_col_out = groupby_col_out.replace("'","")

                        metrics = st.selectbox(':red[Choose Metrics]', ( "COUNT", "SUM", "DISTINCT", "MIN", "MAX", "AVG"))
                        col_metrics = st.multiselect(':red[Choose columns to apply metrics]', selected_column)

                        metrics_out = [f"{metrics}({col}) AS {col}" for col in col_metrics] + list(set(selected_column) - set(col_metrics))
                        metrics_out = str(metrics_out)
                        metrics_out = metrics_out.replace("[","")
                        metrics_out = metrics_out.replace("]","")
                        metrics_out = metrics_out.replace("'","")

                        output_query = f"SELECT {metrics_out} {from_qry} GROUP BY {groupby_col_out} LIMIT 100  OFFSET 0;"
                        st.code(output_query,language='sql')
                        cursor.execute(output_query)
                        rows = cursor.fetchall()
                        st.write(f":green[Showing data from {tables[0]} table]")
                        st.write(pd.DataFrame(rows, columns=[f"{metrics_out}"][0].split(",")))
                    else:
                        #option_col = st.multiselect('Choose columns to apply the method', choose_first_column)
                        options_out = [f"{selected_agg}({col2}) AS {col2}" for col2 in selected_column]
                        options_out  = str(options_out)
                        options_out = options_out.replace("[","")
                        options_out = options_out.replace("]","")
                        options_out = options_out.replace("'","")

                        output_query = f"SELECT {options_out} {from_qry} LIMIT 100  OFFSET 0;"
                        st.code(output_query,language='sql')
                        cursor.execute(output_query)
                        rows = cursor.fetchall()
                        st.write(f":green[Showing data from {tables[0]} table]")
                        st.write(pd.DataFrame(rows, columns=[f"{options_out}"][0].split(",")))
            disconnect_button = st.button("disConnect")
            if disconnect_button:
                query='DROP TABLE my_table;'
                cursor.execute(query)
                connection.commit()
                st.write(":red[disconnected]")
    else:
        st.write(':red[select database]')
                




if __name__ == "__main__":
    main()


