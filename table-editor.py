import streamlit as st
import pandas as pd
import numpy as np
from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as func
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import when_not_matched, when_matched, parse_json
from st_aggrid import AgGrid, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
import json
from datetime import timezone

st.set_page_config(layout="wide")

if 'reload_data' not in st.session_state:
    st.session_state.reload_data=False

####################################################################################################################################
##                                                       Initialize                                                               ##
####################################################################################################################################
def get_snowflake_session(_force_login):
    #global session
    if 'snowparksession' not in st.session_state or _force_login:
        try:
            with open('creds.json') as f:
                connection_parameters = json.load(f)
                st.session_state.account = connection_parameters["account"]
                st.session_state.user = connection_parameters["user"]
                st.session_state.password = connection_parameters["password"]
                st.session_state.role = connection_parameters["role"]
                st.session_state.warehouse = connection_parameters["warehouse"]
            try:
                session = Session.builder.configs(connection_parameters).create()
                st.session_state.snowparksession = session
            except BaseException as e:
                st.error('Login Failed from Config file.  ' + str(e))
                st.session_state.snowparksession =""
        except BaseException as e:
            st.session_state.snowparksession =""
    else:
        session=st.session_state.snowparksession
        try:
            st.session_state.snowparksession.sql('Select 1').collect()
        except BaseException as e:
            st.error('Login Failed. Please login again.')# + str(e))
            get_snowflake_session(True)
#             st.session_state.database_list=[]
# st.session_state.snowparksession =""
get_snowflake_session(False)

if 'account' not in st.session_state or 'user' not in st.session_state or 'password' not in st.session_state or 'role' not in st.session_state:
    st.session_state.account =""
    st.session_state.user =""
    st.session_state.password =""
    st.session_state.role =""
    st.session_state.authenticator=""
    st.session_state.warehouse=""

if 'login_expanded' not in st.session_state and 'snowparksession' not in st.session_state:
    st.session_state.login_expanded = True
else:
    st.session_state.login_expanded = False

def logout_callback():
    sess = st.session_state.snowparksession.sql('select current_session() sessid').toPandas()
    sessionid = sess['SESSID'].iloc[0]
    st.session_state.snowparksession.sql(f'select SYSTEM$ABORT_SESSION( {sessionid} )').collect()

def login_callback():
    st.session_state.database_list =""
    connection_parameters = {
   "account": st.session_state.account,
    "user": st.session_state.user,
    "role": st.session_state.role,
    "warehouse":st.session_state.warehouse
    }
    if st.session_state.authenticator!="":
        connection_parameters["authenticator"] = st.session_state.authenticator
    else:
        connection_parameters["password"] = st.session_state.password
    try:
        session = Session.builder.configs(connection_parameters).create()
        st.session_state.snowparksession = session   
        st.session_state.login_expanded = False
    except BaseException as e:
        st.error('Login Failed: ' + str(e))

if st.session_state.snowparksession !="":
    expander_message = "Connected: " + st.session_state.account
else:
    expander_message = "Login"

# Sidebar Login
with st.sidebar.expander(expander_message, expanded=st.session_state.login_expanded):
    with st.form(expander_message):
        login_mode = st.radio("Login Type",("User and Password","SSO"))
        st.text_input("Account", key='account', value=st.session_state.account)
        st.text_input("User ID", key='user',  value=st.session_state.user)
        if login_mode == "User and Password":
            st.text_input("Password", type="password", key='password',  value=st.session_state.password)
            st.session_state.authenticator=""
        else:
            st.session_state.authenticator='externalbrowser'
        st.text_input("Role", key='role',  value=st.session_state.role)
        st.text_input("Warehouse", key='warehouse',  value=st.session_state.warehouse)
        st.form_submit_button("Login", on_click=login_callback)
#used to test session expiring
#st.button("Logout", on_click=logout_callback)

if 'database_list' not in st.session_state:
    st.session_state.database_list =""

if 'maindf' not in st.session_state:
    st.session_state.maindf = ""

if 'initialdf' not in st.session_state:
    st.session_state.initialdf = ""

if 'grid_key' not in st.session_state:
    st.session_state.grid_key = 0

if 'reload_bt' not in st.session_state:
    st.session_state.reload_bt = False

if 'load_type' not in st.session_state:
    st.session_state.load_type=""

# if 'table_selected' not in st.session_state:
#     st.session_state.table_selected= ""

if 'fully_qualified_table_selected' not in st.session_state:
    st.session_state.fully_qualified_table_selected= ""

if 'table_selected' not in st.session_state:
    st.session_state.table_selected= ""

if 'new_table' not in st.session_state:
    st.session_state.new_table= ""

# if 'truncate_table_cb' not in st.session_state:
#     st.session_state.truncate_table_cb=False
if 'db_objects_selectors_expanded' not in st.session_state:
    st.session_state.db_objects_selectors_expanded = False

if 'uploaded_file' not in st.session_state:
    st.session_state.uploaded_file=""    
####################################################################################################################################
##                                                Function Definitions                                                            ##
####################################################################################################################################
def database_selected_callback():
    #This is needed because of a bug. All the callbacks are called when a file is already loaded and another one is selected
    if not st.session_state.file_chosen:
        st.session_state.database_changed=True
        st.session_state.schema_changed=True
    
def schema_selected_callback():
    if not st.session_state.file_chosen:
        st.session_state.schema_changed=True
        st.session_state.reload_data = True

def table_selected_callback():
    if not st.session_state.file_chosen:
        st.session_state.reload_data = True
        st.session_state.load_type="table"

def get_database_list():
    st.session_state.snowparksession.sql("show databases").collect()
    #df.show(100)
    get_dbs_sql = "WITH databases (a,name,b,c,d,e,f,g,h) as (select * from table(result_scan(last_query_id()))) select name from databases"        
    db_df = st.session_state.snowparksession.sql(get_dbs_sql)
    return db_df.toPandas()

def get_current_database():
    df = st.session_state.snowparksession.sql("select current_database() DB").toPandas()
    return df['DB'].iloc[0]

def load_data():
    try:
        df=st.session_state.snowparksession.table(st.session_state.fully_qualified_table_selected).limit(row_limit)
        st.session_state.initial_sf_df = df
        pdf = df.toPandas() 
    except BaseException as e:
        st.error('Failed to do load table: ' + str(e))
        pdf = pd.DataFrame
    return pdf

grid_loaded=False

################## AGGrid  ###################

def display_grid(_pdf):
    #This is need to work around a bug in AGGrig. When a value is updated all the nulls turn into 'None', so they are not actually null
    c= _pdf.select_dtypes(exclude=np.number).columns
    _pdf[c] = _pdf[c].fillna('null')
    gd = GridOptionsBuilder.from_dataframe(_pdf)
    gd.configure_pagination(enabled=True)
    gd.configure_default_column(editable=True, groupable=True)
    gd.configure_selection(selection_mode="multiple", use_checkbox=True)
    gd.configure_auto_height(autoHeight=False)
    gridoptions = gd.build()
    grid_table = AgGrid(
            _pdf,
            height=700,
            key=st.session_state.grid_key, #This is needed because of a refresh bug
            gridOptions=gridoptions,
            theme="material",
            data_return_mode='AS_INPUT', 
            update_mode=GridUpdateMode.FILTERING_CHANGED.MODEL_CHANGED, #MODEL_CHANGED, #'VALUE_CHANGED',
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True, #Set it to True to allow jsfunction to be injected
            enable_enterprise_modules=True,
            try_to_convert_back_to_original_types = True,
            #conversion_errors = 'ignore',
        )
    st.session_state.maindf = grid_table['data']
   #This is need to work around a bug in AGGrig. When a value is updated all the nulls turn into 'None', so they are not actually null
    c= st.session_state.maindf.select_dtypes(exclude=np.number).columns
    st.session_state.maindf[c] = st.session_state.maindf[c].replace('null',None)
    global grid_loaded
    grid_loaded=True
    st.session_state.grid = grid_table

################## Update DB Functions ###################
#This is needed because of a refresh bug
def redraw_grid_next_time():
    st.session_state.grid_key = st.session_state.grid_key +1

def make_cols_timezone_aware(_df):
    #need to make date columns timezone aware because of a bug in snowpark. 
    for colname in _df.columns :
        if _df[colname].dtype.name == 'datetime64[ns]':
            _df[colname] = _df[colname].map(lambda x: x.tz_localize(timezone.utc))

def make_cols_convert_to_variant(_df):
    #need to make date columns timezone aware because of a bug in snowpark. 
    for colname in st.session_state.variant_list:
        _df=_df.with_column(colname, _df(parse_json(colname)))
            
def create_clean_sf_dataframe(_df):
    _reorder_table_columns = False
    variant_list = [structField.name for structField in st.session_state.initial_sf_df.schema.fields if type(structField.datatype).__name__ == 'VariantType']
    d = st.session_state.snowparksession.create_dataframe(_df)
    original_column_list = d.columns
    for colname in variant_list:
        d=d.with_column(colname, parse_json(d[colname]).cast(VariantType()))
        _reorder_table_columns = True
    if _reorder_table_columns:
        d = d.select(original_column_list)
    return d

def insert_data_callback(_Append_Or_Overwrite, _table_name=st.session_state.table_selected):
    df = create_clean_sf_dataframe(st.session_state.maindf)
    _table_name = f'"{_table_name}"'
    df.write.save_as_table(_table_name, mode=_Append_Or_Overwrite)
    st.success("Inserted Successful")
    #st.session_state.reload_data=True
    
def create_new_table_callback():
    insert_data_callback('overwrite',st.session_state.new_table)
    s = pd.DataFrame({'TABLE_NAME':[st.session_state.new_table]})
    st.session_state.table_list = pd.concat([st.session_state.table_list, s])
    st.session_state.table_selected = st.session_state.new_table

def get_key_join_clasue(_source_df,_target_df):
    key_cols = ""
    for key_col_name in table_keys:
        if key_cols != "":
            key_cols = key_cols + " & "
        key_cols = key_cols + f' ({_target_df}.{key_col_name} == {_source_df}.{key_col_name} )'  
    return key_cols



def delete_data_callback():
    if  not grid_loaded:
        st.error("No data available.")
        return       
    if  len(table_keys) ==0:
        st.error("Please define a Key before deleting")
        st.session_state.reload_data=False
        return
    df=pd.DataFrame(st.session_state.grid['selected_rows'])
    if len(df)==0:
        st.error("No rows selected.")
        return
    source_df = st.session_state.snowparksession.create_dataframe(df)
    target_df = st.session_state.snowparksession.table(st.session_state.fully_qualified_table_selected)
    #st.write(target_df.delete(target_df["CP_CATALOG_PAGE_SK"] == source_df.CP_CATALOG_PAGE_SK, source_df))
    key_cols1 = get_key_join_clasue("source_df","target_df")
    try:
        deleted_rows_count = target_df.delete(eval(key_cols1) , source_df)
        st.success(f"Deleted Rows: {deleted_rows_count.rows_deleted}")
        #st.session_state.reload_data=True
    except BaseException as e:
        st.error('Failed to do delete: ' + str(e))

def get_updated_rows(_initial_df,_updated_df):
    ## This does not work when there is a float. The trailing zeros 
    #_initial_df
    #_updated_df
    #origHash = _initial_df.astype(str).apply(lambda x: hash(tuple(x)), axis = 1)
    newdf = _updated_df[_initial_df.ne(_updated_df).any(axis=1)]
    # _updated_df['Hash'] = _updated_df.astype(str).apply(lambda x: hash(tuple(x)), axis = 1)
    # origHash = _initial_df.astype(str).apply(lambda x: hash(tuple(x)), axis = 1)
    # _updated_df=_updated_df[~_updated_df['Hash'].isin(origHash)].drop('Hash',1)
    # st.session_state.maindf=st.session_state.maindf.drop('Hash',1)
    return newdf

def write_info_message(_message, _color="red"):
    st.sidebar.markdown(f'<a style="color:{_color};font-size:20px;">{_message}</a>', unsafe_allow_html=True)

def merge_data_callback():
    if  not grid_loaded:
        st.error("No data available.")
        return
    if  len(table_keys) ==0:
        st.error("Please define a Key before merging")
        return
    make_cols_timezone_aware(st.session_state.initialdf)
    updated_df = get_updated_rows(st.session_state.initialdf,st.session_state.maindf)
    st.session_state.initialdf = st.session_state.maindf.copy(deep=True)
    if len(updated_df.index)==0:
        st.success("Inserted Rows: 0,  Updated Rows: 0")
    else:
        target = st.session_state.snowparksession.table(st.session_state.fully_qualified_table_selected)
        source = create_clean_sf_dataframe(updated_df)
        update_clause_dict={}
        insert_clause_list=[]
        
    # Create INSERT & UPDATE clause
        for (columnName,data) in st.session_state.maindf.items():
            insert_clause_list.append(source[columnName])
            #if columnName != key_column:
            if table_keys.count(columnName)==0:
                update_clause_dict[columnName]= source[columnName] 
        #key_cols should look like this "(target[key_column] == source[key_column]) & (target[key_column] == source[key_column])"       
        key_cols = ""
        for key_col_name in table_keys:
            if key_cols != "":
                key_cols = key_cols + " & "
            key_cols = key_cols + ' (target["'+key_col_name +'"] == source["'+key_col_name+'"] )'                    
        merged_row_response = target.merge(source, 
            eval(key_cols) , 
            [when_not_matched().insert(insert_clause_list), 
            when_matched().update({**update_clause_dict})])
        st.success(f"Inserted Rows: {merged_row_response.rows_inserted},  Updated Rows: {merged_row_response.rows_updated}")

def add_row_callback():
    # this add a row to the top
    df1 = pd.DataFrame([[np.nan] * len(st.session_state.maindf.columns)], columns=st.session_state.maindf.columns)
    st.session_state.maindf=df1.append(st.session_state.maindf, ignore_index=True)
    # this add a row to the bottom
    #st.session_state.maindf=st.session_state.maindf.append(pd.Series(), ignore_index=True)
    redraw_grid_next_time()  

####################################################################################################################################
##                                                       Sidebar                                                                  ##
####################################################################################################################################

if st.session_state.snowparksession=="":
    st.stop()

############# DB, Schema, Table Selectors #############
#def load_db_schema_table_selectboxes(database_changed=False, schema_changed=False):

table_message = "Select Table to Load"
if st.session_state.table_selected !="":
    table_message = f"Selected Table: {st.session_state.table_selected}"

st.session_state.db_objects_selectors_expanded = True
with st.sidebar.expander(table_message, expanded=True):
    #Get Databases
    if len(st.session_state.database_list) ==0 :
        st.session_state.firs_pass = True
        st.session_state.database_list = get_database_list()
        default_db = get_current_database()
        if default_db is None:
            st.session_state.db_to_select=0    
        else:
            st.session_state.db_to_select = st.session_state.database_list.index[st.session_state.database_list['NAME']==default_db].tolist()[0]
        st.session_state.database_changed=True
    st.selectbox("Databases",st.session_state.database_list, on_change=database_selected_callback, key = 'database_selected', index= st.session_state.db_to_select)        
    if st.session_state.database_changed:
        #Get Schemas
        get_schemas_sql = f"select schema_name from {st.session_state.database_selected}.information_schema.schemata "
        st.session_state.schema_list = st.session_state.snowparksession.sql(get_schemas_sql).toPandas()
        st.session_state.schema_changed = True
        st.session_state.database_changed = False
    schema_selected=  st.selectbox("Schemas",st.session_state.schema_list, on_change=schema_selected_callback, key='schema_selected')

    #Get Tables
    if st.session_state.schema_changed:
        st.session_state.snowparksession.sql(f'use schema "{st.session_state.database_selected}"."{st.session_state.schema_selected}"').collect()
        get_tables_sql = f"select table_name from {st.session_state.database_selected}.information_schema.tables where table_schema = '{st.session_state.schema_selected}'"
        st.session_state.table_list = st.session_state.snowparksession.sql(get_tables_sql).toPandas()
        st.session_state.table_list.loc[-1] = ['']  # adding a row
        st.session_state.table_list.index = st.session_state.table_list.index + 1  # shifting index
        st.session_state.table_list.sort_index(inplace=True) 
        st.session_state.schema_changed = False
        st.session_state.reload_data = False
    st.selectbox("Tables",st.session_state.table_list, on_change=table_selected_callback, key = 'table_selected')
    st.session_state.fully_qualified_table_selected=f'"{st.session_state.database_selected}"."{st.session_state.schema_selected}"."{st.session_state.table_selected}"'
    #row limit clause
    row_limit = st.number_input("Limit Rows", value=1000,step=100)

####################################################################################################################################
##                                                      Update DB Section                                                         ##
####################################################################################################################################

def update_db_callback(_update_type):
    try:
        if _update_type != 'Delete Selected':
            make_cols_timezone_aware(st.session_state.maindf)
        if _update_type == 'Merge':
            merge_data_callback()            
        elif _update_type == 'Insert - Append':
            insert_data_callback('append')
        elif _update_type == 'Insert - Overwrite':
            insert_data_callback('overwrite')
        elif _update_type == 'Delete Selected':
                delete_data_callback()
        else:
            if _update_type == 'Create New Table':
                create_new_table_callback()
        st.session_state.reload_data=True
    except BaseException as e:
            st.error('Update Failed: ' + str(e))

if 'file_chosen' not in st.session_state:
        st.session_state.file_chosen = False

def loadfile_callback():
    st.session_state.load_type="file"
    st.session_state.file_chosen = True
    if st.session_state.file is not None:
        st.session_state.reload_data = True

#load_csv_bt = st.sidebar.button("Load CSV")
if "file" not in st.session_state or st.session_state.file is None:
    file_upload_message = "Load CSV"
else:
    file_upload_message = "File: " + st.session_state.file.name

with st.sidebar.expander(file_upload_message):
    uploaded_file = st.file_uploader("", on_change=loadfile_callback, key="file")

#Load Data
if not st.session_state.firs_pass:
    if (st.session_state.reload_bt or st.session_state.reload_data or st.session_state.file_chosen):
            st.session_state.file_chosen = False
            st.session_state.reload_data=False
            st.session_state.reload_bt=False
            if st.session_state.load_type == "file":
                if st.session_state.file is not None:
                    initial_df = pd.read_csv(st.session_state.file)
                else:
                    st.session_state.load_type ='table'
            if st.session_state.load_type == "table":
                if st.session_state.table_selected==None or st.session_state.table_selected=="" or 'table_selected' not in st.session_state: 
                    st.error("Please select a table")
                else:
                    initial_df = load_data() 
            if 'initial_df' in locals() and not initial_df.empty:  
                redraw_grid_next_time()  
                display_grid(initial_df)
                st.session_state.initialdf = st.session_state.maindf.copy(deep=True)
    elif len(st.session_state.maindf)>0:  # if the data from the grid previously existed then use that
       display_grid(st.session_state.maindf)

#Database updates
st.session_state.firs_pass = False
if len(st.session_state.maindf)>0: 
    with st.sidebar.expander("Save Data to Snowflake"):
        update_type = st.selectbox('Select Update Type:',('Merge', 'Insert - Append','Insert - Overwrite', 'Delete Selected', 'Create New Table'))
        if update_type == 'Merge' or update_type =='Delete Selected':
            if 'table_keys' not in st.session_state:
                st.session_state.table_keys='';
            table_keys = st.session_state.table_keys
            table_keys = st.multiselect("Select Keys:", list(st.session_state.maindf.columns))
            st.session_state.table_keys = table_keys
        # if update_type == 'Insert':
        #     st.checkbox('Truncate table before Insert', key='truncate_table_cb') 
        if update_type == 'Create New Table':
            st.text_input("Table Name", key='new_table')
        st.button(update_type, on_click=update_db_callback, args=(update_type,))
         #Load Button

def reload_callback():
    st.session_state.reload_bt = True

st.sidebar.button("Reload Data", on_click=reload_callback)
st.sidebar.button("Add Row", on_click=add_row_callback)

# with st.expander('Known Issues:'):
#     st.markdown("""
#     ### Timestamp Columns - Can't write back
#     There is a Snowpark bug that can be addressed by setting account params in the production deployments
#     ``` 
#     alter account <account_locator> set enable_parquet_timestamp_new_logical_type=true 
#     alter account <account_locator> set enable_parquet_new_logical_type_infer_schema=true
#     ```  
#     ### Working on Variant writebacks
#     """)
