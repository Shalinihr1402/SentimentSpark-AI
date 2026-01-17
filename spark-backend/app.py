from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename
from datetime import datetime
import subprocess
import auth, os, json
os.environ["PYSPARK_PYTHON"] = r"C:\Users\DELL\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\DELL\AppData\Local\Programs\Python\Python311\python.exe"


app = Flask(__name__)
CORS(app)

# ======================
# PATH CONFIG
# ======================
UPLOAD_FOLDER = "uploads"
OUTPUT_DIR = "D:/spark-output"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

#  SPARK PATH FIX (ONLY CHANGE)
SPARK_SUBMIT = r"C:\spark\bin\spark-submit.cmd"
SPARK_SCRIPT = os.path.abspath("offline_sentiment.py")

# ======================
# LIVE DATA STORAGE
# ======================
live_data = {
    "positive": 0,
    "negative": 0,
    "neutral": 0,
    "total": 0,
    "timestamp": ""
}

# 🔥 continuous live message feed
live_feed = []

# ======================
# KAFKA → FLASK
# ======================
@app.route("/api/update-live", methods=["POST"])
def update_live():
    global live_data, live_feed
    data = request.json

    data["total"] = data["positive"] + data["negative"] + data["neutral"]
    data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    live_data = data

    live_feed.append({
        "text": data.get("last_message"),
        "sentiment": data.get("last_sentiment"),
        "time": data["timestamp"]
    })

    if len(live_feed) > 40:
        live_feed.pop(0)

    print("🔥 Live update:", live_data)
    return jsonify({"status": "ok"})


@app.route("/api/live", methods=["GET"])
def get_live():
    return jsonify(live_data)


@app.route("/api/feed", methods=["GET"])
def get_feed():
    return jsonify(live_feed)

# ======================
# LOGIN / REGISTER
# ======================
@app.route("/api/login", methods=["POST"])
def login():
    data = request.json
    if auth.validate_user(data["email"], data["password"]):
        return jsonify({"status": "success", "user": data["email"]})
    return jsonify({"status": "error"}), 401


@app.route("/api/register", methods=["POST"])
def register():
    data = request.json
    if auth.register_user(data["name"], data["email"], data["password"]):
        return jsonify({"status": "success"})
    return jsonify({"status": "exists"}), 400

# ======================
# 🔥 UPLOAD + RUN SPARK (FIXED)
# ======================
@app.route("/api/upload-dataset", methods=["POST"])
def upload_dataset():
    file = request.files.get("file")
    dataset_name = request.form.get("dataset_name")

    if not file or not dataset_name:
        return jsonify({"status": "error", "message": "Missing file or dataset name"}), 400

    filename = secure_filename(file.filename)
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    file.save(filepath)

    try:
        print("📂 Uploaded file:", filepath)
        print("🚀 Running Spark job...")

        subprocess.run([
            SPARK_SUBMIT,
            SPARK_SCRIPT,
            filepath,
            dataset_name
        ], check=True)

        return jsonify({"status": "success"})

    except Exception as e:
        print("❌ Spark error:", e)
        return jsonify({"status": "error", "message": str(e)}), 500

# ======================
# OFFLINE RESULT APIs
# ======================
@app.route("/api/offline-result", methods=["GET"])
def offline_result():
    path = os.path.join(OUTPUT_DIR, "stats.json")
    if os.path.exists(path):
        with open(path) as f:
            return jsonify(json.load(f))
    return jsonify({"status": "no_data"})


@app.route("/api/preview", methods=["GET"])
def preview():
    path = os.path.join(OUTPUT_DIR, "preview.json")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return jsonify(json.load(f))
    return jsonify([])


@app.route("/api/trend", methods=["GET"])
def trend():
    path = os.path.join(OUTPUT_DIR, "trend.json")
    if os.path.exists(path):
        with open(path) as f:
            return jsonify(json.load(f))
    return jsonify([])

# ======================
# TEST
# ======================
@app.route("/")
def home():
    return "🚀 SentimentSpark-AI Backend Running"

# ======================
# START SERVER
# ======================
if __name__ == "__main__":
    app.run(debug=True)
