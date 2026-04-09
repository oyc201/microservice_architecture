import connexion
from connexion import NoContent
from models import PlayerSnapshot, MatchEvent
from db import makeSession
import yaml, logging, logging.config, json, os, time, random
import datetime as dt
from sqlalchemy import select
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread
from pykafka.common import OffsetType
from pykafka.exceptions import KafkaException

#Create Tables
from db import ENGINE
from models import Base
Base.metadata.create_all(ENGINE)
print("Tables created.")

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


hostname = app_config["events"]["hostname"]
port = app_config["events"]["port"]
topic_name = app_config["events"]["topic"]

class KafkaWrapper:
    def __init__(self, hostname, topic):
        self.hostname = hostname
        self.topic = topic
        self.client = None
        self.consumer = None
        self.connect()

    def connect(self):
        """Infinite loop: will keep trying"""
        while True:
            logger.debug("Trying to connect to Kafka...")
            if self.make_client():
                if self.make_consumer():
                    break

            # Sleeps for a random amount of time (0.5 to 1.5s)
            time.sleep(random.randint(500, 1500) / 1000)

    def make_client(self):
        """
        Runs once, makes a client and sets it on the instance.
        Returns: True (success), False (failure)
        """
        if self.client is not None:
            return True

        try:
            self.client = KafkaClient(hosts=self.hostname)
            logger.info("Kafka client created!")
            return True
        except KafkaException as e:
            msg = f"Kafka error when making client: {e}"
            logger.warning(msg)
            self.client = None
            self.consumer = None
            return False

    def make_consumer(self):
        """
        Runs once, makes a consumer and sets it on the instance.
        Returns: True (success), False (failure)
        """
        if self.consumer is not None:
            return True

        if self.client is None:
            return False

        try:
            topic = self.client.topics[self.topic]
            self.consumer = topic.get_simple_consumer(
                consumer_group=b"event_group",
                reset_offset_on_start=False,
                auto_offset_reset=OffsetType.LATEST
            )
            return True
        except KafkaException as e:
            msg = f"Make error when making consumer: {e}"
            logger.warning(msg)
            self.client = None
            self.consumer = None
            return False

    def messages(self):
        """Generator method that catches exceptions in the consumer loop"""
        if self.consumer is None:
            self.connect()

        while True:
            try:
                for msg in self.consumer:
                    yield msg
            except KafkaException as e:
                msg = f"Kafka issue in consumer: {e}"
                logger.warning(msg)
                self.client = None
                self.consumer = None
                self.connect()

kafka_wrapper = KafkaWrapper(f"{hostname}:{port}", topic_name.encode("utf-8"))

def process_messages():
    # hostname = f"{hostsname}:{port}"
    # client = KafkaClient(hosts=hostname)
    # topic = client.topics[str.encode(f"{topic_name}")]

    # Create a consume on a consumer group, that only reads new messages
    # (uncommitted messages) when the service re-starts (i.e., it doesn't read all the old messages from the history in the message queue).
    # consumer = topic.get_simple_consumer(consumer_group=b'event_group', reset_offset_on_start=False, auto_offset_reset=OffsetType.LATEST)

    for msg in kafka_wrapper.messages():
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
            # consumer.commit_offsets()
            kafka_wrapper.consumer.commit_offsets()
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