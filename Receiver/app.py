import httpx, connexion, json, uuid, yaml, logging, logging.config, datetime, os
from connexion import NoContent
from pykafka import KafkaClient

with open('/config/receiver_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open("/config/receiver_log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())

    logging.config.dictConfig(LOG_CONFIG)
    logger = logging.getLogger('basicLogger')

# PLAYER_SNAPSHOT_URL = app_config["events"]["Player"]["url"]
# MATCH_EVENT_URL = app_config["events"]["Match"]["url"]

hostname = app_config["events"]["hostname"]
port = app_config["events"]["port"]
topic_name = app_config["events"]["topic"]

client = KafkaClient(hosts=f"{hostname}:{port}")
topic = client.topics[topic_name.encode('utf-8')]
producer = topic.get_sync_producer()

def receive_player_snapshots(body):
    match_id = body["match_id"]          # from receiver batch
    snapshots = body["snapshots"]        # list

    trace_id = str(uuid.uuid4())
    logger.info(f"Received event player_snapshot with a trace id of {trace_id}")

    for s in snapshots:
        payload = {
            "trace_id": trace_id,
            "match_id": match_id,      
            "player_puuid": s["player_puuid"],
            "game_time_seconds": s["game_time_seconds"],
            "kills": s["kills"],
            "deaths": s["deaths"],
            "assists": s["assists"],
            "cs": s["cs"],
            "gold": s["gold"],
            "lane": s["lane"],
        }

        # r = httpx.post(PLAYER_SNAPSHOT_URL, json=payload, timeout=5.0)

        msg = { "type": "player_snapshot",
        "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": payload
        }
        msg_str = json.dumps(msg)
        producer.produce(msg_str.encode('utf-8'))
        

        # logger.info(f"Response for event player_snapshot (id: {trace_id}) has status {r.status_code}")
        logger.info(f"Produced player_snapshot event with trace id {trace_id}")


    return NoContent, 201

def receive_match_events(body):
    match_id = body["match_id"]
    events = body["events"]

    trace_id = str(uuid.uuid4())
    logger.info(f"Received event match_event with a trace id of {trace_id}")


    for e in events:
        payload = {
            "trace_id": trace_id,
            "match_id": match_id,
            "event_type": e["event_type"],
            "game_time_seconds": e["game_time_seconds"],
            "team_id": e["team_id"],
            "x": e["x"],
            "y": e["y"],
            "killer_puuid": e.get("killer_puuid"),
        }

        # r = httpx.post(MATCH_EVENT_URL, json=payload, timeout=5.0)

        msg = { "type": "match_event",
        "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": payload
        }
        msg_str = json.dumps(msg)
        producer.produce(msg_str.encode('utf-8'))
    
        # logger.info(f"Response for event match_event (id: {trace_id}) has status {r.status_code}")
        logger.info(f"Produced match_event event with trace id {trace_id}")

    return NoContent, 201

# Define all required functions
app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0")
