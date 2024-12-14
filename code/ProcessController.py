
#!pip install sqlalchemy pandas
import os
from concurrent.futures import ThreadPoolExecutor
from DBController import InsertToDBFromJSON, LoadJSONFile, ConnectDB, LoadConfig, CreateAllTables, DropAllTables


def ProcessFolder(FolderPath, BatchName, engine):

    #Recursively process all JSON data in FolderPath
    TotalProcessedCount = 0
    for root, dirs, files in os.walk(FolderPath):
        with ThreadPoolExecutor() as executor:
            futures = []
            for file in files:
                if file.endswith(".json"):
                    JsonPath = os.path.join(root, file)
                    futures.append(executor.submit(ProcessJSON, JsonPath, engine, BatchName=BatchName))
            
            for dir in dirs:
                DirectoryPath = os.path.join(root, dir)
                if any(f.endswith('.json') for f in os.listdir(DirectoryPath)):
                    futures.append(executor.submit(ProcessFolder, DirectoryPath, dir, engine))

            for future in futures:
                TotalProcessedCount += future.result()

    return TotalProcessedCount

def ProcessJSON(JSONPath, engine, BatchName, BatchSize=1000):

    #Process a single JSON file below
    data = LoadJSONFile(JSONPath)
    AllData = data.get('data', [])
    batches = [AllData[i:i + BatchSize] for i in range(0, len(AllData), BatchSize)]
    
    for batch in batches:
        for tweet in batch:
            tweet['batch_task'] = BatchName
        InsertToDBFromJSON(engine, batch, BatchName)
    return len(AllData)


if __name__ == '__main__':

    path = "tweets/aftonbladet"

    config = LoadConfig()
    engine = ConnectDB(config)

    DropAllTables(engine)
    CreateAllTables(engine)

    Records = 0
    Records = ProcessFolder(path, engine)
    
    print(f"Total tweets processed: {Records}")
