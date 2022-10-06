# Databricks notebook source
import requests
import base64
import json

def get_all_repo_files(repo,project="Quantum",organization="EntDataProd"):

    url = "https://dev.azure.com/{}/{}/_apis/git/repositories/{}/items?recursionLevel=Full&api-version=6.0".format(organization,project,repo)
    pat_token = "t*******************************************************q"

    authorization = str(base64.b64encode(bytes(':'+pat_token, 'ascii')), 'ascii')

    headers = {
          'Content-type': 'application/json',
          'Authorization': 'Basic' + authorization
      }
    
    try:
      response = requests.get(url,headers=headers)
      response_json = json.loads(response.content.decode("ascii"))
    except Exception as error:
      print("Exception : ",error)
      return []
    
    file_paths = [content["path"] for content in response_json["value"]]
    return file_paths
      

# COMMAND ----------

def get_code(repo,path,project="Quantum",organization="EntDataProd"):
  
  url = "https://dev.azure.com/{}/{}/_apis/git/repositories/{}/items?path={}&includeContent={}&api-version=6.1-preview.1" \
         .format(organization,project,repo,path,True,True)
  pat_token = "t*******************************************************q"

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

def get_files_with_txt(repo,search_text,project="Quantum",organization="EntDataProd"):
  
  file_paths = get_all_repo_files(repo) 
  code_files_path = [file_path for file_path in file_paths if ".py" in file_path]
  files_with_text_meta = [[repo,file_path[0:file_path.rfind("/")],file_path[file_path.rfind("/")+1:]]for file_path in code_files_path if search_text in get_code(repo,file_path)]
  return files_with_text_meta


# COMMAND ----------

import pandas as pd

repos = ["curated","semantic_bi","semantic_ca","semantic_ds"]
search_texts = ["/mnt"]
search_results = []

for repo in repos:
  for search_text in search_texts:
      search_results.extend(get_files_with_txt(repo,search_text))
  
search_results_spark = spark.createDataFrame(pd.DataFrame(search_results,columns=["RepoName","NotebookFolderPath","NotebookName"]))

# COMMAND ----------

display(search_results_spark)
