# Reagan

Python package use to streamline data transfer and credential management

AWS Secret and Client Secret must be added to environment variables before use.

![Dropwizard](https://schuster-hosted-images.s3.amazonaws.com/reagan.jpg )

## Package Installation

```pip install reagan```

or

```python -m pip install reagan```

## Modules

### SQLServer
--------------

Manages interactions with SQL Server.

 **Credentials**
 
Managed in Amazon AWS Parameter Sotre.
```python
/sqlserver/[server alias] = [SQL Server Connection String]
```
**Functions (see documentation within functions)**

*execute* - Executes a DML statement 

*to_df* - Loads results from a sql query to a pandas dataframe

*to_list* - Loads results from a sql query to a list

*to_dict* - Executes query and returns results into a dictionary object with user specified key/valuesw

*get_scalar* - Loads a single result from a sql query

*to_sql* - Dumps data from a pandas dataframe to a sql table

**Examples**

```python
from reagan import SQLServer

# establish connection by instanciating the class
ss = SQLServer([server alias])

# forumlate query
query = '''
    SELECT TOP (20) * 
    FROM sys.tables
    WHERE Create_Date BETWEEN '[START DATE]' AND '[END DATE]'   
'''

# set any query parameters
replacements = {
    '[START DATE]' : '2019-01-01',
    '[END DATE]' : '2019-02-01',
}

# transfer from SQL Server to memory (pandas DataFrame)
df = ss.to_df(query=query, replacements=replacements)
```

### PSQL
--------------

Manages interactions with PostgreSQL.

 **Credentials**
 
Managed in AWS Parameter Store
```python
/postgres/[server alias] = [psql connection dict]
```
**Functions (see documentation within functions)**

*to_df* - Loads results from a sql query to a pandas dataframe

*to_sql* - Dumps data from a pandas dataframe to a sql table

**Examples**

```python
from reagan import PSQL

# establish connection by instanciating the class
ps = PSQL('102')

# forumlate query
query = '''
    SELECT *
    FROM carat_gm.dcm_date
    WHERE Date BETWEEN '[START DATE]' AND '[END DATE]' 
    LIMIT 1000  
'''

# set any query parameters
replacements = {
    '[START DATE]' : '2019-01-01',
    '[END DATE]' : '2019-02-01',
}

# transfer from SQL Server to memory (pandas DataFrame)
df = ss.to_df(query=query, replacements=replacements)
```

### DCMAPI
--------------

Manages interactions with the DCM API using Google's Python libraries.

 **Credentials**
 
Managed in AWS Parameter Store. Interactions with DCM made via service account.

```python
/dcm/service_account_path = [path to the servie account .json file] 
```
**Functions**

*list* - Makes a list call to DCM and returns the object in the form of a list of dictionaries.

*update* - Makes an update call to DCM.

*patch* - Makes a patch call to DCM.

*to_df* - Makes a list call to DCM then converts that to a pandas dataframe.

*set_profile_id* - Populate the DCM profileId based on the network you select.

*report_to_df* - Pulls a report to a pandas dataframe.

**Examples**

```python
from reagan import DCMAPI

# first connect to DCM via service account by instanciating the class
dcm = DCMAPI()

# set the object specifications
obj = "ads"
arguments = {"active": True}
columns = ["id", "name", "placementAssignments_placementId"]
all = False

# Pulling the cities to raw dcm object
ads = dcm.list(obj, arguments=arguments, all=all)

# Pulling a dataframe of the cities
df_ads = dcm.to_df(obj=obj, columns=columns, all=all, arguments=arguments)
```

### SA360
--------------

Manages interactions with the SA360 API using Google's Python libraries.

 **Credentials**
 
Managed in AWS Parameter Store. Interactions with DCM made via service account.

```python
/sa360/service_account_path = [path to the servie account .json file] 
```
**Functions**

*reports_to_df* - Makes a list call to DCM and returns a generator that yields a pandas dataframe with 1000000 rows with the report specifications

**Examples**

```python
from reagan import SA360

# first connect to DCM via service account by instanciating the class
sa = SA360()

# set the report specifications
    report_type = 'campaign'
    agency_id = 123456
    columns = ['campaignId','campaign','campaignStartDate','campaignEndDate']

# Look through the generator to get files
    for df in sa.reports_to_df(report_type = report_type, agency_id = agency_id, columns=columns):
        print(df.head())
```

### Drive
--------------

Manages interactions with the Google Drive API using Google's Python libraries.

 **Credentials**
 
Added in version 1.4.0  

Managed in AWS Parameter Store. Interactions with DCM made via service account. The service account path is hardcoded to point to the json file located on 102.

```python
/drive/service_account_path =  [path to the servie account .json file] 
```
**Functions**

*retrieve_all_files* - Makes a list call to the API and returns all of the files shared with the service account.

*download_file* - Makes an update call to DCM.

**Examples**

```python
from reagan import Drive

# first connect to DCM via service account by instanciating the class
dr = Drive()

# Set the parameters
file_id = '16w136pklr3LjDf67PSXSo_GzE4Ey_bYg'
path = "C:\\Users\\Public\\Downloads\\hi.gif"

# Download the file
dr.download_file(file_id, path)
```

### SmartsheetsAPI
--------------

Manages interaction with the Smartsheet API via the smartsheet-python-sdk module

**Credentials**

Connections to the Smartsheet API are made using a bearer token generated through the smartsheet developer UI. 

```python
    /smartsheets/bearer_token =  [bearer token] 
```

**Functions**

*sheet_to_df* - Makes GET call for Sheets and returns the object in a DataFrame. Through some manipulation in the raw json object, each row in the DataFrame will correspond to a cell in the DataFrame. It will have a row number and column number to identify the position on the sheet.

*discussions_to_df* - Makes GET call for Discussions and returns the object in a DataFrame

*get_attachment_url* - Makes GET call for Attachments and returns the url of the attachment that was generated. As of this update, per the Smartsheet API documentation the link will expire after two minutes.

*attachment_to_df* - **Currently only supports attachments in Excel**. First uses the get_attachments_url method to get the url for the attachment. Then returns a dictionary object with each key as the sheet name in the attachment and the corresponding DataFrame as the value.

**Examples**

```python
from reagan import SmartsheetAPI

# First instanciate the object to ensure the bearer token is pulled in
ss = SmartsheetAPI()

# Set the sheet id for which you want to pull
sheet_id = 

# Pull a dataframe for that sheet
df = ss.sheet_to_df(sheet_id)
```

### GCP
--------------

Manages interaction with the Google Cloud Platorm (as of now only supports compute instances)

**Credentials**

Managed in AWS Parameter Store. Interactions with DCM made via service account.

```python
    /gcp/service_account_path = [path to the servie account .json file]
    /gcp/project = [default project to use]
    /gcp/zone = [default zone]
    /gcp/region = [default region]
```


**Functions**

*create_instance* - Creates a boots up a compute instance on the GCP. Only supports limited functionality

*list_to_df* - Lists all of the Compute Instances currently running

*delete_instance* - Deletes an instances on GCP

**Examples**

```python
from reagan import GCP

# First instanciate the object to pull in default settings
ss = GCP()

# Set compute instance parameters
name = "test-instance1"
machine_type = "g1-small"
source_disk_image = "https://www.googleapis.com/compute/v1/projects/us-gm-175021/global/images/adstxt-image"

# Make the API call to create the instance
instance = gcp.create_instance(name=name,machine_type=machine_type,source_disk_image=source_disk_image)


```

### DevOpsAPI
--------------

Manages interaction with the Microsoft DevOps API

**Credentials**

Managed in AWS Parameter Store. Interactions with DevOps made via user account.

```python
    /devops/personal_access_token = [Generated user access token]
    /devops/organization = [DevOps Organization Name]
```

**Functions**

*to_df* - Pulls in objects to a pandas dataframe

**Examples**

```python
from reagan import DevOpsAPI

# First instanciate the object to pull in default settings
dva = DevOpsAPI()

# Pull in the list of all projects
df = dva.to_df(obj = 'projects')


```
