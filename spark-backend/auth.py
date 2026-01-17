import json, os

USERS_FILE = "users.json"

# Create users file if not exists
if not os.path.exists(USERS_FILE):
    with open(USERS_FILE, "w") as f:
        json.dump({
            "shalinidvg16@gmail.com": {
                "password": "7411156526",
                "name": "Shalini"
            }
        }, f, indent=4)


def load_users():
    with open(USERS_FILE, "r") as f:
        return json.load(f)


def save_users(users):
    with open(USERS_FILE, "w") as f:
        json.dump(users, f, indent=4)


# ✅ USED BY LOGIN
def validate_user(email, password):
    users = load_users()
    return email in users and users[email]["password"] == password


# ✅ USED BY REGISTER
def register_user(name, email, password):
    users = load_users()

    if email in users:
        return False

    users[email] = {
        "name": name,
        "password": password
    }

    save_users(users)
    return True
