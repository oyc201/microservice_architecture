import connexion , json, yaml, logging, logging.config, os
from pykafka import KafkaClient
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware

# Load configs
with open("/config/analyzer_conf.yml", "r") as f:
    app_config = yaml.safe_load(f)


with open("/config/analyzer_log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f)

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger("basicLogger")

# get player snapshot by index
def get_player_snapshot(index):
    logger.info(f"Received request for player snapshot at index {index}")

    hostname = app_config["events"]["hostname"]
    port = app_config["events"]["port"]
    topic_name = app_config["events"]["topic"]

    client = KafkaClient(hosts=f"{hostname}:{port}")
    topic = client.topics[str.encode(f"{topic_name}")]
    consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)

    counter = 0

    for msg in consumer:

        msg_str = msg.value.decode("utf-8")
        msg = json.loads(msg_str)
        payload = msg["payload"]

        if msg["type"] == "player_snapshot":

            if counter == index:
                logger.info(f"Found player snapshot at index {index}")
                return payload, 200

            counter += 1

    logger.error(f"Player snapshot index {index} not found")

    return {"message": f"No player snapshot at index {index}"}, 404


# get match event by index
def get_match_event(index):
    logger.info(f"Received request for match event at index {index}")

    hostname = app_config["events"]["hostname"]
    port = app_config["events"]["port"]
    topic_name = app_config["events"]["topic"]

    client = KafkaClient(hosts=f"{hostname}:{port}")
    topic = client.topics[str.encode(f"{topic_name}")]
    consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)

    counter = 0

    for msg in consumer:

        msg_str = msg.value.decode("utf-8")
        msg = json.loads(msg_str)
        payload = msg["payload"]

        if msg["type"] == "match_event":

            if counter == index:
                logger.info(f"Found match event at index {index}")
                return payload, 200

            counter += 1

    logger.error(f"Match event index {index} not found")

    return {"message": f"No match event at index {index}"}, 404


def get_stats():
    logger.info("Received request for stats")

    hostname = app_config["events"]["hostname"]
    port = app_config["events"]["port"]
    topic_name = app_config["events"]["topic"]

    client = KafkaClient(hosts=f"{hostname}:{port}")
    topic = client.topics[str.encode(f"{topic_name}")]
    consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)

    num_player_snapshots = 0
    num_match_events = 0

    for msg in consumer:
        msg_str = msg.value.decode("utf-8")
        msg = json.loads(msg_str)

        if msg["type"] == "player_snapshot":
            num_player_snapshots += 1

        if msg["type"] == "match_event":
            num_match_events += 1

    logger.info(f"Stats calculated: players snapshots={num_player_snapshots}, match events={num_match_events}")

    stats = {
        "num_player_snapshots": num_player_snapshots,
        "num_match_events": num_match_events,
    }

    return stats, 200

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