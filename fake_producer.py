from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import spacy
import time
from faker import Faker
import random

# NLP setup
nltk.download("vader_lexicon")
sia = SentimentIntensityAnalyzer()
nlp = spacy.load("en_core_web_sm")

# Faker setup
fake = Faker()

# Kafka config
TOPIC_NAME = "twitter_sentiment"
BOOTSTRAP_SERVER = "localhost:29092"

# Sports-related keywords
keywords = ["LeBron", "GOAT", "cooked", "mid", "Lakers", "Wemby", "Messi", "football", "Cricket", "Basketball"]

# Ensure topic exists
admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVER)
existing_topics = admin_client.list_topics()

if TOPIC_NAME not in existing_topics:
    print(f"Creating topic: {TOPIC_NAME}")
    topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)
    admin_client.create_topics([topic])
else:
    print(f"Topic '{TOPIC_NAME}' already exists.")

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Sentiment analysis
def analyze_sentiment(text):
    score = sia.polarity_scores(text)
    return "Positive" if score['compound'] >= 0.05 else "Negative" if score['compound'] <= -0.05 else "Neutral"

# Entity extraction
def extract_entities(text):
    doc = nlp(text)
    return [ent.text for ent in doc.ents if ent.label_ in ["PERSON", "ORG"]]

# Tweet simulation
def simulate_tweet(include_keyword=False):
    if include_keyword:
        text = f"{fake.sentence()} {random.choice(keywords)} {fake.sentence()}"
    else:
        text = fake.sentence(nb_words=random.randint(8, 15))
    return {
        "text": text,
        "created_at": fake.iso8601()
    }

# Message generation loop
total_messages = 1000
sent_count = 0

# Decide the target range of keyword-injected messages
min_keywords = int(0.2 * total_messages)
max_keywords = int(0.5 * total_messages)
target_keyword_count = random.randint(min_keywords, max_keywords)

keyword_count = 0

print(f"Target keyword-containing tweets: {target_keyword_count}/{total_messages}")

while sent_count < total_messages:
    # Decide whether to include a keyword
    include_keyword = keyword_count < target_keyword_count and random.random() < 0.5

    tweet = simulate_tweet(include_keyword=include_keyword)
    sentiment = analyze_sentiment(tweet["text"])
    entities = extract_entities(tweet["text"])
    
    message = {
        "text": tweet["text"],
        "created_at": tweet["created_at"],
        "sentiment": sentiment,
        "entities": entities
    }

    producer.send(TOPIC_NAME, message)
    sent_count += 1
    if include_keyword:
        keyword_count += 1

    if sent_count % 100 == 0:
        print(f"Sent {sent_count} tweets...")

producer.flush()
print("✅ Done sending 1000 tweets.")
print(f"Keyword-based tweets: {keyword_count}")
