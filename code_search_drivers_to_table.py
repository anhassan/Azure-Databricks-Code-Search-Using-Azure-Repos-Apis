# Databricks notebook source
def get_all_ingested_tables(schemas=[]):
  if len(schemas) == 0:
    schemas_df = spark.sql("SHOW DATABASES")
    schemas = [row.databaseName for row in schemas_df.collect()]
  tables = []
  for schema in schemas:
    tables_df  = spark.sql("SHOW TABLES IN {}".format(schema))
    tables_schema = ["{}.{}".format(schema,row.tableName).upper() for row in tables_df.collect()]
    tables.extend(tables_schema)
  return tables

# COMMAND ----------

def get_code(repo,path,project="Quantum",organization="EntDataProd"):
  
  url = "https://dev.azure.com/{}/{}/_apis/git/repositories/{}/items?path={}&includeContent={}&api-version=6.1-preview.1" \
         .format(organization,project,repo,path,True,True)
  pat_token = "4qonvju4jxyxwoqbuox5a3h2jseihndn5swkja2tbudrwgq5vciq"

  authorization = str(base64.b64encode(bytes(':'+pat_token, 'ascii')), 'ascii')

  headers = {
      'Content-type': 'application/json',
      'Authorization': 'Basic' + authorization
  }
  try:
      response = requests.get(url,headers=headers)
      code = response.content.decode("ascii")
  except:
      code = ""
  return code

# COMMAND ----------

import json

def get_drivers(repo,project="Quantum",organization="EntDataProd"):
  
  path = "/driver_notebooks"
  url = "https://dev.azure.com/{}/{}/_apis/git/repositories/{}/items?scopePath={}&recursionLevel=Full&includeContentMetadata=true&api-version=6.0".format(organization,project,repo,path)

  pat_token = "4qonvju4jxyxwoqbuox5a3h2jseihndn5swkja2tbudrwgq5vciq"

  authorization = str(base64.b64encode(bytes(':'+pat_token, 'ascii')), 'ascii')

  headers = {
      'Content-type': 'application/json',
      'Authorization': 'Basic' + authorization
  }
  
  try:
      response = requests.get(url,headers=headers)
      drivers_obj = response.content.decode("ascii")
  except:
      raise Exception("Request Failed For Searching driver_notebooks folder in Repo : {}".format(repo))
  
  try:
    drivers_json = json.loads(drivers_obj)
    driver_notebooks = [obj["path"] for obj in drivers_json["value"]][1:]
  except:
    driver_notebooks = []
  
  return driver_notebooks

# COMMAND ----------

def get_notebook_paths(repo,driver_path):
  
  task_types = ["parallel","series"]
  total_tasks = []
  code = get_code(repo,driver_path)
  
  if len(code) > 0:
    driver_code = code.split("\n")
    for task_type in task_types:
      
      try:
        base_dir_meta = [code for code in driver_code if len(code) > 0 and code[0] !="#" and "{}_tasks".format(task_type) in code and "run_{}".format(task_type) in code][0]
        base_dir = "/"+ base_dir_meta[base_dir_meta.find("base_path")+len("base_path") : base_dir_meta.find(")")].replace(",","").replace('"',"") + "/"
      except:
        base_dir = ""
        
      try:
        tasks_meta = [code for code in driver_code if len(code) > 0 and code[0] != "#" and "{}_tasks".format(task_type) in code and "run_{}".format(task_type) not in code][0]
        tasks_text = tasks_meta[tasks_meta.find("[")+1:tasks_meta.find("]")].split(",")
        tasks = [base_dir + task.replace('"',"") for task in tasks_text]
        total_tasks += tasks
      except:
        pass
    
    return total_tasks

# COMMAND ----------

import requests
import base64

def get_notebook_tables(repo,notebook_path,ingested_tables,project="Quantum",organization="EntDataProd"):
  
  notebook_path = notebook_path + ".py"
  notebook_code = get_code(repo,notebook_path)
  tables_found = [table for table in ingested_tables if table in notebook_code]
  return tables_found

# COMMAND ----------

def get_driver_notebook_tables(repo):
  tables = []
  tab_data = []
  tables = get_all_ingested_tables()
  driver_paths = get_drivers(repo)
  for driver_path in driver_paths:
    notebook_paths = get_notebook_paths(repo,driver_path)
    for notebook_path in notebook_paths:
      notebook_tables = list(set(get_notebook_tables(repo,notebook_path,tables)))
      tables += notebook_tables
      for table in notebook_tables:
        tab_data.append([repo,driver_path[driver_path.rfind("/")+1:],notebook_path[notebook_path.rfind("/")+1:],table])
  return tables,tab_data
      

# COMMAND ----------

repos = ["curated","semantic_bi","semantic_ca"]
driver_notebooks_tables = []
for repo in repos:
  driver_notebooks_tables.extend(get_driver_notebook_tables(repo)[1])

# COMMAND ----------

import pandas as pd 
driver_notebooks_tables_df = spark.createDataFrame(pd.DataFrame(driver_notebooks_tables,columns=["RepoName","DriverName","NotebookName","TableName"]))
display(driver_notebooks_tables_df)

# COMMAND ----------


