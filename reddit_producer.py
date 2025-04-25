from kafka import KafkaProducer
import requests
import json
import os
from dotenv import load_dotenv
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import spacy
import time

# Load env
load_dotenv()
BEARER_TOKEN = os.getenv("BEARER_TOKEN")

# NLP
nltk.download("vader_lexicon")
sia = SentimentIntensityAnalyzer()
nlp = spacy.load("en_core_web_sm")

# Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Twitter API config
headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
keywords = ["LeBron", "GOAT", "cooked", "mid", "Lakers", "Wemby","Messi","football","Cricket","Basketball"]
query = " OR ".join(keywords) + " lang:en -is:retweet"
url = "https://api.twitter.com/2/tweets/search/recent"
params = {"query": query, "max_results": 10, "tweet.fields": "text,created_at"}

def analyze_sentiment(text):
    score = sia.polarity_scores(text)
    return "Positive" if score['compound'] >= 0.05 else "Negative" if score['compound'] <= -0.05 else "Neutral"

def extract_entities(text):
    doc = nlp(text)
    return [ent.text for ent in doc.ents if ent.label_ in ["PERSON", "ORG"]]

api_call = 0
max_calls = 5
while api_call < max_calls:
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            api_call += 1
            for tweet in response.json().get("data", []):
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
        else:
            print(f"Twitter API error {response.status_code}: {response.text}")
            if response.status_code == 429:
                print("Rate limit hit. Sleeping for 5 minutes before retrying...")
                time.sleep(300)
            else:
                 time.sleep(10)

    except Exception as e:
        print("Error:", e)
    time.sleep(30)
