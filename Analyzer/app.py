import connexion , json, yaml, logging, logging.config, os, time, random
from pykafka import KafkaClient
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware
from pykafka.common import OffsetType
from pykafka.exceptions import KafkaException

# Load configs
with open("/config/analyzer_conf.yml", "r") as f:
    app_config = yaml.safe_load(f)


with open("/config/analyzer_log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f)

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger("basicLogger")

hostname = app_config["events"]["hostname"]
port = app_config["events"]["port"]
topic_name = app_config["events"]["topic"]

def get_consumer():
    client = KafkaClient(hosts=f"{hostname}:{port}")
    topic = client.topics[topic_name.encode("utf-8")]
    logger.info(f"Available topics: {list(client.topics.keys())}")
    logger.info(f"Looking for topic: {topic_name.encode('utf-8')}")
    return topic.get_simple_consumer(
        reset_offset_on_start=True,
        auto_offset_reset=OffsetType.EARLIEST,
        consumer_timeout_ms=1000
    )

# get player snapshot by index
def get_player_snapshot(index):
    logger.info(f"Received request for player snapshot at index {index}")
    counter = 0
    for raw_msg in get_consumer():
        event = json.loads(raw_msg.value.decode("utf-8"))
        if event["type"] == "player_snapshot":
            if counter == index:
                logger.info(f"Found player snapshot at index {index}")
                return event["payload"], 200
            counter += 1
    logger.error(f"Player snapshot index {index} not found")
    return {"message": f"No player snapshot at index {index}"}, 404

#get match event by index
def get_match_event(index):
    logger.info(f"Received request for match event at index {index}")
    counter = 0
    for raw_msg in get_consumer():
        event = json.loads(raw_msg.value.decode("utf-8"))
        if event["type"] == "match_event":
            if counter == index:
                logger.info(f"Found match event at index {index}")
                return event["payload"], 200
            counter += 1
    logger.error(f"Match event index {index} not found")
    return {"message": f"No match event at index {index}"}, 404

def get_stats():
    logger.info("Received request for stats")
    num_player_snapshots = 0
    num_match_events = 0
    for raw_msg in get_consumer():
        event = json.loads(raw_msg.value.decode("utf-8"))
        if event["type"] == "player_snapshot":
            num_player_snapshots += 1
        if event["type"] == "match_event":
            num_match_events += 1
    logger.info(f"Stats calculated: players snapshots={num_player_snapshots}, match events={num_match_events}")
    return {"num_player_snapshots": num_player_snapshots, "num_match_events": num_match_events}, 200

app = connexion.FlaskApp(__name__, specification_dir=".")
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
    app.run(port=8110, host="0.0.0.0")