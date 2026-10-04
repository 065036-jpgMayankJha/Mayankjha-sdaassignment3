# Streaming Data Analytics — Assignment 2
## E-commerce / Online Retail

This project continues Assignment 1. It simulates four continuously generated e-commerce data sources and streams them to four Kafka topics:

- `topic_clickstream`
- `topic_orders`
- `topic_inventory`
- `topic_logistics`

The consumer reads all four topics and stores the events in MongoDB Atlas collections.

## 1. Prerequisites
- Python 3.10+ installed
- Docker Desktop installed and running
- MongoDB Atlas account
- VS Code (recommended)

## 2. Project setup — Windows PowerShell

Open the project folder in VS Code, then:

```powershell
python -m venv venv
.env\Scripts\Activate.ps1
pip install -r requirements.txt
```

If PowerShell blocks activation, run:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.env\Scripts\Activate.ps1
```

## 3. Configure MongoDB Atlas

Copy `.env.example` to `.env`:

```powershell
Copy-Item .env.example .env
```

Open `.env` and replace `MONGO_URI` with your MongoDB Atlas connection string.

Example format only:

```text
MONGO_URI=mongodb+srv://USERNAME:PASSWORD@cluster.xxxxx.mongodb.net/?retryWrites=true&w=majority
```

Do NOT commit `.env` to GitHub.

## 4. Start Kafka

Make sure Docker Desktop is running.

```powershell
docker compose up -d
```

Check:

```powershell
docker compose ps
```

Kafka should show as running.

## 5. Run the producer

Keep the virtual environment activated.

```powershell
python producer.py
```

The producer continuously generates realistic synthetic events and sends them to all four A1 topics.

You should see messages such as:

```text
[topic_orders] sent: {...}
[topic_clickstream] sent: {...}
```

Leave this terminal running.

## 6. Run the consumer

Open a SECOND VS Code terminal.

Activate the venv:

```powershell
.env\Scripts\Activate.ps1
```

Then:

```powershell
python consumer.py
```

The consumer reads Kafka events and inserts them into MongoDB Atlas.

Expected collections:

- `clickstream`
- `orders`
- `inventory`
- `logistics`

Leave both producer and consumer running for at least 1–2 minutes so that MongoDB gets well above 5 documents.

## 7. Verify MongoDB Atlas

MongoDB Atlas → your cluster → Browse Collections.

Database name:

```text
ecommerce_sda
```

You should see collections such as:

```text
clickstream
orders
inventory
logistics
```

Open `orders` (or any collection) and show 5+ documents for the Assignment 2 screenshot.

## 8. Stop the project

Stop Python programs with:

```text
Ctrl + C
```

Stop Kafka:

```powershell
docker compose down
```

## 9. GitHub

Do NOT upload:
- `venv/`
- `.env`
- `__pycache__/`

The `.gitignore` file is already configured for this.

Suggested submission tag:

```text
[A2][SDA-1]
```

or use the tag corresponding to your section.

## Architecture

```text
Synthetic E-commerce Events
          |
          v
      producer.py
          |
          v
     Apache Kafka
   /      |       |       click   orders inventory logistics
stream
   \      |       |       /
          v
      consumer.py
          |
          v
     MongoDB Atlas
   /      |       |       click   orders inventory logistics
stream
```

This matches the four-source/two-stage streaming design described in Assignment 1, while keeping Assignment 2 simple enough to run and demonstrate reliably.
