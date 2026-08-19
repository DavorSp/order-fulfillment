import os

from dotenv import load_dotenv

load_dotenv()

AMQP_URL = os.environ["AMQP_URL"]
DB_URL = os.environ["INVENTORY_DB_URL"]
REDIS_URL = os.environ["REDIS_URL"]
