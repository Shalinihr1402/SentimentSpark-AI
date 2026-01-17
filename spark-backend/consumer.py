from kafka import KafkaConsumer
import requests
from datetime import datetime

# ----------------------------
# KAFKA CONSUMER CONFIG
# ----------------------------
consumer = KafkaConsumer(
    "sentiment-topic",
    bootstrap_servers="localhost:9092",
    group_id="sentiment-live-group",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: x.decode("utf-8", errors="ignore")
)

print("\n🔥 Kafka Live Consumer Started Successfully...")
print("📡 Waiting for live messages...\n")

# ----------------------------
# LIVE COUNTERS
# ----------------------------
positive = 0
negative = 0
neutral = 0

# ----------------------------
# SIMPLE RULE-BASED SENTIMENT
# ----------------------------
def detect_sentiment(text):
    t = text.lower().strip()

    if any(word in t for word in ["good", "love", "excellent", "happy", "awesome", "best"]):
        return "positive"

    elif any(word in t for word in ["bad", "hate", "worst", "angry", "poor", "sad"]):
        return "negative"

    else:
        return "neutral"

# ----------------------------
# CONSUME LOOP
# ----------------------------
for msg in consumer:
    try:
        text = msg.value.strip()
        if not text:
            continue

        sentiment = detect_sentiment(text)

        if sentiment == "positive":
            positive += 1
        elif sentiment == "negative":
            negative += 1
        else:
            neutral += 1

        payload = {
            "positive": positive,
            "negative": negative,
            "neutral": neutral,
            "total": positive + negative + neutral,
            "last_message": text,
            "last_sentiment": sentiment,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        print(f"📩 {text}  →  {sentiment.upper()}")

        try:
            requests.post("http://127.0.0.1:5000/api/update-live", json=payload, timeout=2)
        except:
            print("⚠️ Flask server not running (data not sent)")

    except KeyboardInterrupt:
        print("\n🛑 Consumer stopped by user")
        break

    except Exception as e:
        print("❌ Error:", e)

print("✅ Kafka consumer closed.")
