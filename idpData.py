'''Helper functions for IDP Python users:
    - `idpDataInit()`- function to initialise the notebook and install | import required `python` libraries
    - `idpGetConnectionInfo()`- function that returns a JSON representation of the connection info required to connect to IDP's virtual database
    - `idpDataQuery()`- function to execute a SQL query against the virtual database, returning a pandas dataframe object
    - `idpDataDisconnect()`- function to close an open connection to IDP's virtual database
'''    

def install_and_import(module):
    import importlib
    try:
        print (f'Importing module: {module}')
        importlib.import_module(module)
    except ImportError:
        import pip
        print (f'Installing module: {module}')
        pip.main(['install', module])
    finally:
        globals()[module] = importlib.import_module(module)


def import_modules(modules):
    print (f'Importing modules...')
    for module in modules:
        install_and_import(module)


def idpDataInit():
    
    '''Initialises the notebook and install | import required python libraries.
            Parameters: 
                    None
            Returns:
                    None
    '''
    
    print (f'Initialising...')
    import_modules(["jaydebeapi","socket","pandas","json","decimal","numpy","getpass"])


def get_config():
    with open('idpDataConfig.json') as config_file:
        config = json.load(config_file)    
    return config    

def idp_get_connection_info(database):
    
    config = get_config()
    
    conn_info = {}
    conn_info['denododriver_class'] = config['denododriver_class']
    conn_info['denododriver_path']  = config['denododriver_path']
    
    denodoserver_database  = database    
    
    config = get_config()
    denodoserver_name      = config['denodoserver_name']
    denodoserver_jdbc_port = config['denodoserver_jdbc_port']
    
    client_hostname = socket.gethostname()
    useragent = "%s-%s" % (jaydebeapi.__name__,client_hostname)
    conn_uri = "jdbc:vdb://%s:%s/%s?userAgent=%s" % (denodoserver_name,denodoserver_jdbc_port,denodoserver_database,useragent)
    conn_info['conn_uri']  = conn_uri
    
    return conn_info


def map_datatypes(crsr):
        
    results = crsr.fetchall()
    columns = [c[0] for c in crsr.description]
    df_results = pandas.DataFrame.from_records(results, columns=columns)
    
    types_dict = {
            int                                               :  pandas.Int64Dtype(),
            decimal.Decimal                                   :  numpy.float,
            jaydebeapi.DBAPITypeObject._mappings['DATE']      :  numpy.datetime64(),
            jaydebeapi.DBAPITypeObject._mappings['TIMESTAMP'] :  numpy.datetime64(),
            jaydebeapi.DBAPITypeObject._mappings['CHAR']      :  object,
            jaydebeapi.DBAPITypeObject._mappings['INTEGER']   :  pandas.Int64Dtype(),
            jaydebeapi.DBAPITypeObject._mappings['FLOAT']     :  numpy.float
            }
    
    types = [types_dict[c[1]] for c in crsr.description]
        
    for c,tp in  zip(df_results.columns,types):
        df_results[c] = df_results[c].astype(tp)
            
    return df_results


def idpDataConnect(username, database):
    
    '''Opens a connection to IDP's virtual database, returning a connection object.
            Parameters:
                    username (str): your work email address
                    database (str): the name of the Virtual Database to connect to
            Returns:
                    conn : a connection object
    '''  
    
    conn_info = idp_get_connection_info(database)
    
    conn = jaydebeapi.connect(conn_info['denododriver_class'],
                              conn_info['conn_uri'],
                              driver_args = {"user":username,
                                             "password":getpass.getpass()},
                              jars = conn_info['denododriver_path']
                             )
    
    return conn


def idpDataDesc(database,dataset,conn):

    '''Describes the schema of a dataset within the virtual database, returning a pandas dataframe object.
            Parameters:
                    database (str): the name of the Virtual Database
                    dataset (str): the name of the dataset to describe
                    conn : a connection object
            Returns:
                    df_schema : a Pandas dataframe
    '''
    
    df_schema = idpDataQuery(sql="SELECT column_name, column_type_name, column_type_length \
                                  FROM   CATALOG_METADATA_VIEWS() \
                                  WHERE  input_database_name = '"+database+"' \
                                  AND    input_view_name =  '"+dataset+"';",
                             conn=conn)
    return df_schema


def idpDataQuery(sql,conn):

    '''Executes a SQL query against the virtual database, returning a pandas dataframe object.
            Parameters:
                    sql (str): a valid SQL query
                    conn : a connection object
            Returns:
                    df_results : a Pandas dataframe
    '''
    
    cursor = conn.cursor()
    cursor.execute(sql)
    df_results = map_datatypes(cursor)
    return df_results 


def idpDataDisconnect(conn):
    
    '''Closes an open connection to IDP's virtual database.
            Parameters:
                    conn : a connection object
            Returns:
                    None
    '''
    conn.close()
