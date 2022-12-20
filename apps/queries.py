import logging
from pymongo import MongoClient
from utils.report import report

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S')

def connect_to_mongodb(db_name, db_url="mongodb://mongodb", db_port=27017):
    client = MongoClient(db_url, db_port, authSource='admin&readPreference=secondary&directConnection=true&ssl=false')

    db = client[db_name]

    logging.info("[INFO] Successfully connected to mongo")

    return db

def get_top_100_candidates(db):
    results = db.Candidates.find({},{"_id": 0, "GRE Score": 1, "TOEFL Score": 1, "University Rating": 1, "Research": 1, "Chance of Admit ": 1}).sort("Chance of Admit ", -1).limit(100)

    logging.info("[INFO] Top 100 Candidates")
    report(results)
    
def get_avg_chance_with_no_research(db):
    results = db.Candidates.aggregate([{ "$match": {"Research": 0}}, {"$group": {"_id": 0, "avg_chance" : { "$avg" : "$Chance of Admit "}}}])
    
    logging.info("[INFO] Average chances of candidates without research")
    report(results)

def get_avg_chance_with_research(db):
    results = db.Candidates.aggregate([{ "$match": {"Research": 1}}, {"$group": {"_id": 0, "avg_chance" : { "$avg" : "$Chance of Admit "}}}])
    
    logging.info("[INFO] Average chances of candidates with research")
    report(results)

def get_avg_gre_score_universities(db):
    result_uni_1 = db.Candidates.aggregate([{ "$match": {"University Rating": 1}}, {"$group": {"_id": 0, "avg_gre_score": {"$avg" : "$GRE Score"}}}])
    result_uni_2 = db.Candidates.aggregate([{ "$match": {"University Rating": 2}}, {"$group": {"_id": 0, "avg_gre_score": {"$avg" : "$GRE Score"}}}])
    result_uni_3 = db.Candidates.aggregate([{ "$match": {"University Rating": 3}}, {"$group": {"_id": 0, "avg_gre_score": {"$avg" : "$GRE Score"}}}])
    result_uni_4 = db.Candidates.aggregate([{ "$match": {"University Rating": 4}}, {"$group": {"_id": 0, "avg_gre_score": {"$avg" : "$GRE Score"}}}])
    result_uni_5 = db.Candidates.aggregate([{ "$match": {"University Rating": 5}}, {"$group": {"_id": 0, "avg_gre_score": {"$avg" : "$GRE Score"}}}])

    logging.info("[INFO] University Rating: 1")
    report(result_uni_1)
    logging.info("[INFO] University Rating: 2")
    report(result_uni_2)
    logging.info("[INFO] University Rating: 3")
    report(result_uni_3)
    logging.info("[INFO] University Rating: 4")
    report(result_uni_4)
    logging.info("[INFO] University Rating: 5")
    report(result_uni_5)


if __name__ == "__main__":
    db = connect_to_mongodb(db_name='adm_data')

    get_avg_chance_with_no_research(db)
    get_avg_chance_with_research(db)
    get_top_100_candidates(db)
    get_avg_gre_score_universities(db)