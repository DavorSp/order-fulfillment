import os

from dotenv import load_dotenv

load_dotenv()

AMQP_URL = os.environ["AMQP_URL"]
