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
    agg_options = ["GROUP BY", "COUNT", "SUM", "DISTINCT", "MIN", "MAX", "AVG","OUR CHOICE"]
       
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
            connection, message = connect_func(host, port, db_name, username, password)
            cursor,tables = show_tables(connection,message,selected_db)
            selected_table = st.selectbox(":red[Select a table]",tables)
            if selected_table in tables:
                col_names = show_data(cursor, selected_table, tables)
                selected_column = st.multiselect(":red[Select a column]",col_names)
                selected_agg = st.selectbox(":red[Select an aggregation function]",agg_options)
                if selected_agg == "GROUP BY":
                    groupby_col = st.multiselect(':red[Choose columns to apply group by]', selected_column)
                    groupby_col_out =list_convertin(groupby_col)
                    metrics = st.selectbox(':red[Choose Metrics]', agg_options[1:])
                    col_metrics = st.multiselect(':red[Choose columns to apply metrics]', selected_column)
                    metrics_out = [f"{metrics}({col}) AS {col}" for col in col_metrics] + list(set(selected_column) - set(col_metrics))
                    metrics_out = list_convertin(metrics_out)
                    generate_query_fun(selected_agg,selected_table,cursor,metrics_out,metrics_out,groupby_col_out)
                elif selected_agg=="OUR CHOICE":
                    user_query_fun(cursor,selected_table)
                else:
                    options_out = [f"{selected_agg}({col2}) AS {col2}" for col2 in selected_column]
                    options_out = list_convertin(options_out)
                    generate_query_fun(selected_agg,selected_table,cursor,options_out,metrics_out=None,groupby_col_out=None)
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
            selected_column = st.multiselect(":red[Select a column]",column_names)
            selected_agg = st.selectbox(":red[Select an aggregation function]" ,agg_options)
            if selected_agg == "GROUP BY":
                groupby_col = st.multiselect(':red[Choose columns to apply group by]', selected_column)
                groupby_col_out =list_convertin(groupby_col)
                metrics = st.selectbox(':red[Choose Metrics]', agg_options[1:])
                col_metrics = st.multiselect(':red[Choose columns to apply metrics]', selected_column)
                metrics_out = [f"{metrics}({col}) AS {col}" for col in col_metrics] + list(set(selected_column) - set(col_metrics))
                metrics_out = list_convertin(metrics_out)
                generate_query_fun(selected_agg,tables[0],cursor,metrics_out,metrics_out,groupby_col_out)
            elif selected_agg=="OUR CHOICE":
                user_query_fun(cursor,tables[0])
            else:
                options_out = [f"{selected_agg}({col2}) AS {col2}" for col2 in selected_column]
                options_out = list_convertin(options_out)
                generate_query_fun(selected_agg,tables[0],cursor,options_out,metrics_out=None,groupby_col_out=None)
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


