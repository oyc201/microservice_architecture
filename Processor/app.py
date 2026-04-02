import connexion, yaml, logging.config, json, os, requests
import datetime as dt
from apscheduler.schedulers.background import BackgroundScheduler
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware

# Load config files
with open("/config/processor_conf.yml", "r") as f:
    app_config = yaml.safe_load(f)


with open("/config/processor_log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())

    logging.config.dictConfig(LOG_CONFIG)
    logger = logging.getLogger('basicLogger')

DATA_FILE = app_config["datastore"]["filename"]
PLAYER_URL = app_config["events"]["player"]["url"]
MATCH_URL = app_config["events"]["match"]["url"]
INTERVAL = app_config["scheduler"]["interval"]

def populate_stats():
    logger.info("Periodic processing started")

    #Read 
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            stats = json.load(f)
    else:
        stats = {
            "num_player_snapshots": 0, 
            "num_match_events": 0,          
            # "avg_gold": None,                 
            # "avg_game_time_seconds": None,
            "last_updated": "2025-01-01T00:00:00Z"
        }

    #Get timestamps 
    current_timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    last_updated = stats['last_updated']
    
    #Fetch 
    player_resp = requests.get(PLAYER_URL, params={"start_timestamp": last_updated, "end_timestamp": current_timestamp})
    match_resp = requests.get(MATCH_URL, params={"start_timestamp": last_updated, "end_timestamp": current_timestamp})

    #Log ERROR 
    if player_resp.status_code != 200:
        logger.error(f"Player snapshots GET failed with status {player_resp.status_code}")
        player_events = []
    else:
        player_events = player_resp.json()

    if match_resp.status_code != 200:
        logger.error(f"Match events GET failed with status {match_resp.status_code}")
        match_events = []
    else:
        match_events = match_resp.json()

    #Log INFO
    logger.info(f"Received {len(player_events)} player snapshots")
    logger.info(f"Received {len(match_events)} match events")

    #Update totals -> issues with this  
    stats["num_player_snapshots"] += len(player_events)
    stats["num_match_events"] += len(match_events)

    #Update gold
    # if player_events:
    #     gold_values = [e["gold"] for e in player_events]
    #     stats["avg_gold"] = sum(gold_values) / len(gold_values)

    #Update time
    # if match_events:
    #     times = [e["game_time_seconds"] for e in match_events]
    #     stats["avg_game_time_seconds"] = sum(times) / len(times)

    stats["last_updated"] = current_timestamp

    #Write to JSON
    with open(DATA_FILE, "w") as f:
        json.dump(stats, f, indent=2)

    logger.debug(f"Updated stats: {stats}")
    logger.info("Periodic processing ended")

def get_stats():
    logger.info("Stats request received")

    if not os.path.exists(DATA_FILE):
        logger.error("Statistics do not exist")
        return {"message": "Statistics do not exist"}, 404

    with open(DATA_FILE, "r") as f:
        stats = json.load(f)

    logger.debug(f"Stats: {stats}")

    logger.info("Stats request completed")
    return stats, 200

def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats,
                "interval",
                seconds=app_config["scheduler"]["interval"])
    sched.start()

app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)

app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, host="0.0.0.0")