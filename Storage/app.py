import connexion
from connexion import NoContent
from models import PlayerSnapshot, MatchEvent
from db import makeSession
import yaml, logging, logging.config, json, os
import datetime as dt
from sqlalchemy import select
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread

with open("/config/storage_log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())

    logging.config.dictConfig(LOG_CONFIG)
    logger = logging.getLogger('basicLogger')

"""POST FUNCTIONS"""
# def receive_player_snapshot(body):
#     trace_id=body["trace_id"]
#     session = makeSession()

#     try: 
#         snapshot = PlayerSnapshot(
#             trace_id=body["trace_id"],
#             match_id=body["match_id"],
#             player_puuid=body["player_puuid"],
#             game_time_seconds=body["game_time_seconds"],
#             kills=body["kills"],
#             deaths=body["deaths"],
#             assists=body["assists"],
#             cs=body["cs"],
#             gold=body["gold"],
#             lane=body["lane"]
#         )

#         session.add(snapshot)
#         session.commit()

#     finally:
#         session.close()
#         logger.debug(f"Received event player_snapshot with a trace id of {trace_id}")
    
#     return NoContent, 201

# def receive_match_event(body):
#     trace_id=body["trace_id"]
#     session = makeSession()

#     try:
#         event = MatchEvent(
#             trace_id=body["trace_id"],
#             match_id=body["match_id"],
#             event_type=body["event_type"],
#             game_time_seconds=body["game_time_seconds"],
#             team_id=body["team_id"],
#             x=body["x"],
#             y=body["y"],
#             killer_puuid=body["killer_puuid"]
#         )

#         session.add(event)
#         session.commit()

#     finally:
#         session.close()
#         logger.debug(f"Received event match_event with a trace id of {trace_id}")

#     return NoContent, 201

"""GET FUNCTIONS""" 
def get_player_snapshots(start_timestamp, end_timestamp):
    session = makeSession()

    # Timestamps -> Python datetime
    start_dt = dt.datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%SZ")
    end_dt = dt.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%SZ")

    """
    SELECT *
    FROM player_snapshot
    WHERE date_created >= :start
    AND date_created < :end;
    """
    statement = select(PlayerSnapshot).where(PlayerSnapshot.date_created >= start_dt, PlayerSnapshot.date_created < end_dt)

    rows = session.execute(statement).scalars().all()
    session.close()

    stats = [{
            "trace_id": r.trace_id,
            "match_id": r.match_id,
            "player_puuid": r.player_puuid,
            "game_time_seconds": r.game_time_seconds,
            "kills": r.kills,
            "deaths": r.deaths,
            "assists": r.assists,
            "cs": r.cs,
            "gold": r.gold,
            "lane": r.lane,
        } 
        for r in rows
    ]


    logger.debug(f"Found {len(stats)} player snapshots (start={start_dt} end={end_dt})")
    return stats
   


def get_match_events(start_timestamp, end_timestamp):
    session = makeSession()

    start_dt = dt.datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%SZ")
    end_dt = dt.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%SZ")

    statement = select(MatchEvent).where(MatchEvent.date_created >= start_dt, MatchEvent.date_created < end_dt)

    rows = session.execute(statement).scalars().all()
    session.close()

    stats = [{
            "trace_id": r.trace_id,
            "match_id": r.match_id,
            "event_type": r.event_type,
            "game_time_seconds": r.game_time_seconds,
            "team_id": r.team_id,
            "x": r.x,
            "y": r.y,
            "killer_puuid": r.killer_puuid,
        }
        for r in rows
    ]

    logger.debug(f"Found {len(stats)} player snapshots (start={start_dt} end={end_dt})")
    return stats


"""Kafka"""
with open('/config/storage_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())


hostsname = app_config["events"]["hostname"]
port = app_config["events"]["port"]
topic_name = app_config["events"]["topic"]

def process_messages():
    hostname = f"{hostsname}:{port}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(f"{topic_name}")]

    # Create a consume on a consumer group, that only reads new messages
    # (uncommitted messages) when the service re-starts (i.e., it doesn't read all the old messages from the history in the message queue).
    consumer = topic.get_simple_consumer(consumer_group=b'event_group', reset_offset_on_start=False, auto_offset_reset=OffsetType.LATEST)

    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info("Message: %s" % msg)
        payload = msg["payload"]
        session = makeSession()
        try: 
            if msg["type"] == "player_snapshot":
                # Store the event to the DB
                snapshot = PlayerSnapshot(
                    trace_id=payload["trace_id"],
                    match_id=payload["match_id"],
                    player_puuid=payload["player_puuid"],
                    game_time_seconds=payload["game_time_seconds"],
                    kills=payload["kills"],
                    deaths=payload["deaths"],
                    assists=payload["assists"],
                    cs=payload["cs"],
                    gold=payload["gold"],
                    lane=payload["lane"]
                )

                session.add(snapshot)

            elif msg["type"] == "match_event":
                # Store the event to the DB
                event = MatchEvent(
                    trace_id=payload["trace_id"],
                    match_id=payload["match_id"],
                    event_type=payload["event_type"],
                    game_time_seconds=payload["game_time_seconds"],
                    team_id=payload["team_id"],
                    x=payload["x"],
                    y=payload["y"],
                    killer_puuid=payload["killer_puuid"]
                )

                session.add(event)

            session.commit()
            # Commit the new message as being read
            consumer.commit_offsets()
        except Exception as e:
            session.rollback()
            logger.error("Error processing message: %s", e)
        finally:
            session.close()

def setup_kafka_thread():
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()

# Define all required functions
app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    setup_kafka_thread() # This is ran before the app.run because if it isnt the app.run will loop infinitely 
    app.run(port=8090, host="0.0.0.0")