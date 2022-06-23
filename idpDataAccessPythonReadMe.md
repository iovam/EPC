# IDP - GCP Vertex Notebooks

## Basic guide to getting started accessing data in GCP Vertex Notebooks.

### Pre-Requesites
#### The following files need to be loaded into your home directory i.e. `/home/jupyter`:
- `idpData.py` - python module with some helper functions
- `idpDataConfig.json` - config information, primarily used to connect to IDP's virtual database
- `denodo-vdp-jdbcdriver-8.0-ga.jar` - JDBC driver used to connect to IDP's virtual database

### Import idpData Modules
#### There are a number of functions which can be imported from the idpData module:
- `idpDataInit()` - function to initialise the notebook and install | import required python libraries
- `idpDataConnect()` - function to connect to IDP's virtual database, returing a connection object. NB Prompts for your Denodo password.
- `idpDataDesc()` - function that describes the schema of a dataset within the virtual database, returning a pandas dataframe object
- `idpDataQuery()` - function to execute a SQL query against the virtual database, returning a pandas dataframe object
- `idpDataDisconnect()` - function to close an open connection to IDP's virtual database

#### NB Python help is available on each module e.g.:
`from idpData import idpDataQuery`

`help (idpDataQuery)`

### The `idpDataInit()` function
#### Inputs: `None`
#### Returns: `None`

### The `idpConnect()` function
#### Opens a connection to IDP's virtual database, returning a connection object. NB Prompts for your Denodo password.
#### Inputs: 
- `username` : your work email address
- `database` : the name of the Virtual Database to connect to
#### Returns: 
- `conn` : a connection object

### The `idpDataDesc()` function
#### Inputs:
- `database` : the name of the Virtual Database
- `dataset` : the name of the dataset to describe
- `conn` : the connection object returned from the `idpDataConnect` function
#### Returns: 
- `dataframe` : a pandas dataframe

### The `idpDataQuery()` function
#### Inputs: 
- `sql` : a valid SQL query
- `conn` : the connection object returned from the `idpDataConnect` function
#### Returns: 
- `dataframe` : a pandas dataframe

### The `idpDataDisconnect()` function
#### Inputs: 
- `connection` : the connection object returned from the `idpDataConnect` function
#### Returns: 
- `None`

## Worked example:
`import sys`

`import os`


#### Initialise the notebook, after opening or re-starting the kernel
`from idpData import idpDataInit`

`idpDataInit()`

#### Open a connection into the Virtual Database. NB Prompts for your Denodo password.
`from idpData import idpDataConnect`

`conn = idpDataConnect("firstname.lastname","ids")`


#### Query the virtual database, passing an SQL query and the conn (connection) object:
##### NB Queries currently require columns to be double quoted (complete with an escape character `\`) and column names are case sensitive.
##### This is because the Denodo database has to run in UNICODE rather than RESTRICTED mode, a solution to improve this incovenience is underway.
`from idpData import idpDataQuery`

`df1 = idpDataQuery(sql="SELECT \"index\", \"HGOR\" FROM ids.uv_ashe_2020 limit 10",conn=conn)`
                   
`df1`


#### Close the connection at the end of your session,  passing in the conn onject:
`from idpData import idpDataDisconnect`

`idpDataDisconnect(conn)`

