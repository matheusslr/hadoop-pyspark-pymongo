import logging
import pandas as pd
from pymongo import MongoClient

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S')

def load_mongo_data(csv_path, db_name, collection_name, db_url="mongodb://mongodb", db_port=27017):
    client = MongoClient(db_url, db_port, authSource='admin&readPreference=secondary&directConnection=true&ssl=false')

    df = pd.read_csv(csv_path, sep=",")
    data = df.to_dict(orient="records")

    db = client[db_name]
    collection = db[collection_name]
    collection.insert_many(data)

    logging.info("[INFO] Successfully loaded data")

if __name__ == "__main__":
    load_mongo_data(csv_path='hdfs://node-master:9000/user/root/dataset/adm_data.csv', 
                    db_name='adm_data', 
                    collection_name='Candidates')