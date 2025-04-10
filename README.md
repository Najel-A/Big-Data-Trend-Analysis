# README
Tasks

1. Set up Reddit & Twitter API ingestion + format messages
2. Set up Kafka producers/consumers, define topics
3. Implement exponential decaying window logic (Spark, Flink, or custom)
4. Generate insights (e.g. trending topics, keyword frequency) + visual report/dashboard

## Setup
Create a .env file with the following variables:
POSTGRES_USER=<your-username>

POSTGRES_PASSWORD=<your-password>

POSTGRES_DB=<your-database-name>


Create Python virtual environment (Must be Python 3.8-3.12):

python -m venv venv

source venv/bin/activate

pip install -r requirements.txt


## Run application
Command to run the application:
docker compose up

docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic twitter_sentiment --partitions 1 --replication-factor 1

Execute producer scripts
