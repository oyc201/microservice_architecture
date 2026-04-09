from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml

with open("/config/storage_conf.yml", "r") as f:
    app_config = yaml.safe_load(f)

USER = app_config["datastore"]["user"]
PASS = app_config["datastore"]["password"]
HOST = app_config["datastore"]["hostname"]
PORT = app_config["datastore"]["port"]
DB = app_config["datastore"]["db"]
URL = f"mysql+pymysql://{USER}:{PASS}@{HOST}:{PORT}/{DB}"

ENGINE = create_engine(
    URL,    
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=5)
session = sessionmaker(bind=ENGINE)

def makeSession():
    return session() 