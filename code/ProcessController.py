
#!pip install sqlalchemy pandas
import os
import json
from concurrent.futures import ThreadPoolExecutor
from DBController import InsertToDBFromJSON, ConnectDB, LoadConfig, CreateAllTables, DropAllTables


def ProcessFolder(FolderPath, BatchName, engine, mode="insert"):
    #Recursively process all JSON data in FolderPath
    AllData = []
    with ThreadPoolExecutor() as executor:
        futures = []
        for root, dirs, files in os.walk(FolderPath):
            for file in files:
                if file.endswith(".json"):
                    JsonPath = os.path.join(root, file)
                    futures.append(executor.submit(ProcessJSON, JsonPath, engine, BatchName, 10000, mode))
            for dir in dirs:
                DirectoryPath = os.path.join(root, dir)
                if any(f.endswith('.json') for f in os.listdir(DirectoryPath)):
                    futures.append(executor.submit(ProcessFolder, DirectoryPath, dir, engine, mode))

            for future in futures:
                AllData += future.result()
    if mode == 'insert':
        return len(AllData)
    return AllData

def ProcessJSON(JSONPath, engine, BatchName, BatchSize=10000, mode="insert"):

    #Process a single JSON file below
    data = LoadJSONFile(JSONPath)
    AllData = data.get('data', [])
    batches = [AllData[i:i + BatchSize] for i in range(0, len(AllData), BatchSize)]
    
    for batch in batches:
        for tweet in batch:
            tweet['batch_task'] = BatchName
        if mode == "insert":
            InsertToDBFromJSON(engine, batch, BatchName, BatchSize)
    
    return AllData


def LoadJSONFile(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

if __name__ == '__main__':

    path = "tweets/aftonbladet"

    config = LoadConfig()
    engine = ConnectDB(config)

    DropAllTables(engine)
    CreateAllTables(engine)

    Records = 0
    Records = ProcessFolder(path, engine)
    
    print(f"Total tweets processed: {Records}")
