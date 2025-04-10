from kafka import KafkaProducer
import json
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import spacy
import time
from faker import Faker
import random

# Initialize Faker
fake = Faker()

# NLP setup
nltk.download("vader_lexicon")
sia = SentimentIntensityAnalyzer()
nlp = spacy.load("en_core_web_sm")

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9093",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Keywords for Faker simulation
keywords = ["LeBron", "GOAT", "cooked", "mid", "Lakers", "Wemby", "Messi", "football", "Cricket", "Basketball"]

# Sentiment analysis function
def analyze_sentiment(text):
    score = sia.polarity_scores(text)
    return "Positive" if score['compound'] >= 0.05 else "Negative" if score['compound'] <= -0.05 else "Neutral"

# Entity extraction function
def extract_entities(text):
    doc = nlp(text)
    return [ent.text for ent in doc.ents if ent.label_ in ["PERSON", "ORG"]]

# Simulate tweets using Faker
def simulate_tweet():
    tweet_text = f"{fake.sentence()} {random.choice(keywords)} {fake.sentence()}"
    created_at = fake.iso8601()
    return {"text": tweet_text, "created_at": created_at}

# Simulation loop
num_batches = 5
batch_size = 10

for _ in range(num_batches):
    try:
        for _ in range(batch_size):
            tweet = simulate_tweet()
            sentiment = analyze_sentiment(tweet["text"])
            entities = extract_entities(tweet["text"])
            message = {
                "text": tweet["text"],
                "created_at": tweet["created_at"],
                "sentiment": sentiment,
                "entities": entities
            }
            producer.send("twitter_sentiment", message)
            print("Sent to Kafka:", message)

    except Exception as e:
        print("Error:", e)

    time.sleep(5)