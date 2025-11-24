# 🚀 Complete Pipeline Startup Guide

## Current Status: Pipeline NOT Running

Your dashboard won't work yet because the **full pipeline is not running**. This guide will help you start everything from scratch.

---

## 📋 Prerequisites (Install First!)

### 1. Docker Desktop for Windows

**Required**: All databases run in Docker containers.

**Download**: https://www.docker.com/products/docker-desktop/

**Installation Steps**:
1. Download Docker Desktop installer
2. Run installer (requires admin rights)
3. Restart computer
4. Start Docker Desktop from Start menu
5. Wait for Docker to fully start (whale icon in system tray)

**Verify Docker is running**:
```bash
docker --version
docker ps
```

If these commands work, Docker is ready!

### 2. Python Dependencies

```bash
pip install -r requirements.txt
```

---

## 🎯 Complete 3-Stage Pipeline Architecture

```
STAGE 1: DATA COLLECTION (Local Scripts)
├─ reddit_sentiment.py      → LMDB (data/reddit/)
├─ twitter_sentiment.py     → LMDB (data/twitter/)
├─ onchain_data.py         → LMDB (data/onchain/)
└─ websocket3.py           → QuestDB (Docker)

STAGE 2: PROCESSING (Docker Container)
└─ spark-analytics container → Runs spark_analytics.py every 5 min

STAGE 3: STORAGE (Docker Containers)
├─ Cassandra (13 analytics tables)
├─ Neo4j (graph relationships)
└─ MongoDB (document analytics)
```

---

## 🚀 Step-by-Step Startup

### Step 1: Start All Docker Services

**Option A: Using the startup script (Recommended)**
```bash
cd D:\Financewebsocket-for-bigdatapractice251124
start_pipeline.bat
```

**Option B: Manual Docker Compose**
```bash
cd D:\Financewebsocket-for-bigdatapractice251124
docker-compose up -d
```

**What this starts**:
- ✅ QuestDB (time-series database)
- ✅ Cassandra (analytics storage)
- ✅ Neo4j (graph database)
- ✅ MongoDB (document storage)
- ✅ Spark Analytics (processing pipeline - auto-runs every 5 min)
- ✅ Mongo Express (MongoDB web UI)
- ✅ Cassandra Web (Cassandra web UI)

**Wait time**: ~60 seconds for all services to be ready

**Verify services are running**:
```bash
docker ps
```

You should see 7 containers running:
- questdb
- cassandra
- neo4j
- mongodb
- mongo-express
- cassandra-web
- spark-analytics

---

### Step 2: Start Data Collection Scripts (Stage 1)

You need to run these scripts **locally** (not in Docker) to collect data into LMDB.

**Option A: Run everything with main orchestrator**
```bash
python main.py
```

This starts:
1. Crypto + Stock streaming (websocket3.py) → QuestDB
2. Reddit sentiment collection (reddit_sentiment.py) → LMDB
3. Analytics scheduler (runs spark_analytics.py every 5 min)

**Option B: Run scripts individually in separate terminals**

Terminal 1: Reddit sentiment
```bash
python reddit_sentiment.py
```

Terminal 2: Twitter sentiment
```bash
python twitter_sentiment.py
```

Terminal 3: On-chain data
```bash
python onchain_data.py
```

Terminal 4: Crypto + Stock streaming
```bash
python websocket3.py
```

---

### Step 3: Wait for Data to Flow Through Pipeline

**Timeline**:

| Time | What's Happening |
|------|------------------|
| 0 min | Data collection starts (Stage 1) |
| 1 min | LMDB databases start filling with Reddit/Twitter/On-chain data |
| 1 min | QuestDB starts receiving orderbook ticks |
| 5 min | **First Spark analytics run** (Stage 2) |
| 5 min | Cassandra/Neo4j/MongoDB start filling with enriched analytics (Stage 3) |
| 10 min | **Dashboard ready!** All 3 stages have data |

**How to monitor progress**:

1. **Check Docker logs**:
```bash
docker-compose logs -f spark-analytics
```

Look for: `[Spark Analytics] ✓ Pipeline completed successfully!`

2. **Check LMDB data**:
```bash
python view_lmdb.py
```

3. **Check Cassandra tables**:
```bash
python query_all_data.py
```

---

### Step 4: Launch the Dashboard

After **10 minutes** of data collection:

```bash
streamlit run live_dashboard.py
```

Dashboard opens at: **http://localhost:8501**

**What you'll see**:
- ✅ Stage 1: Reddit, Twitter, On-Chain counts
- ✅ Stage 2: Processing status (HEALTHY)
- ✅ Stage 3: Arbitrage, divergences, predictions, word correlations

---

## 🔍 Verification Checklist

### ✅ Stage 1: Raw Collection Working?

**Check LMDB databases exist**:
```bash
dir data\reddit\reddit_sentiment_lmdb.db
dir data\twitter\twitter_sentiment.db
dir data\onchain\onchain_data.db
```

**Check QuestDB has data**:
- Open: http://localhost:9000
- Run query: `SELECT COUNT(*) FROM orderbook`
- Should see: > 0 rows

### ✅ Stage 2: Spark Processing Working?

**Check Spark container logs**:
```bash
docker logs spark-analytics --tail 50
```

Look for:
```
[Spark Analytics] ✓ Pipeline completed successfully!
```

**Check Cassandra schema created**:
```bash
docker exec -it cassandra cqlsh -e "DESCRIBE KEYSPACE financial_data"
```

Should see 13+ tables

### ✅ Stage 3: Databases Have Enriched Data?

**Check Cassandra tables**:
```bash
python query_all_data.py
```

Or open Cassandra Web: http://localhost:3000

**Check Neo4j**:
- Open: http://localhost:7474
- Login: neo4j / password
- Run: `MATCH (n) RETURN COUNT(n)`

**Check MongoDB**:
- Open: http://localhost:8081
- Login: admin / admin
- Check `financial_analytics` database

---

## 🎯 Quick Start (TL;DR)

If Docker Desktop is already installed and running:

```bash
# 1. Start databases (wait 60 seconds)
start_pipeline.bat

# 2. Start data collection
python main.py

# 3. Wait 10 minutes for data to flow through all stages

# 4. Launch dashboard
streamlit run live_dashboard.py
```

---

## 🛠️ Troubleshooting

### Problem: "docker: command not found"

**Solution**: Install Docker Desktop for Windows
- Download: https://www.docker.com/products/docker-desktop/
- Restart computer after installation

### Problem: "Docker is not running"

**Solution**:
1. Open Docker Desktop from Start menu
2. Wait for Docker to start (whale icon in system tray)
3. Try again

### Problem: "Port already in use"

**Solution**: Stop conflicting services
```bash
# Find what's using the port
netstat -ano | findstr :9000
netstat -ano | findstr :9042

# Kill the process
taskkill /F /PID <process_id>
```

### Problem: Dashboard shows "No data yet"

**Reasons**:
1. **Too early**: Wait 10 minutes for data to flow through all stages
2. **Stage 1 not running**: Check if collection scripts are running
3. **Stage 2 not running**: Check Spark container logs
4. **Databases not ready**: Check `docker ps` - all services should be "healthy"

**Solution**:
```bash
# Check what's running
docker ps

# Check Spark logs
docker logs spark-analytics --tail 100

# Check if LMDB has data
python view_lmdb.py

# Check if Cassandra has data
python query_all_data.py
```

### Problem: Spark analytics failing

**Common causes**:
1. Not enough data in LMDB yet (wait 5 minutes)
2. QuestDB empty (websocket3.py not running)
3. Cassandra schema not created

**Solution**:
```bash
# Check Spark logs
docker logs spark-analytics --follow

# Create Cassandra schema manually
python create_cassandra_table.py

# Restart Spark container
docker restart spark-analytics
```

---

## 📊 Web UIs Available

Once everything is running, you can access:

| Service | URL | Login |
|---------|-----|-------|
| **Dashboard** | http://localhost:8501 | None |
| QuestDB | http://localhost:9000 | None |
| Neo4j Browser | http://localhost:7474 | neo4j / password |
| Mongo Express | http://localhost:8081 | admin / admin |
| Cassandra Web | http://localhost:3000 | None |

---

## 🔄 Daily Operations

### Start Everything:
```bash
# Start Docker services
start_pipeline.bat

# Start data collection
python main.py

# Start dashboard (in separate terminal)
streamlit run live_dashboard.py
```

### Stop Everything:
```bash
# Stop data collection (Ctrl+C in terminals)

# Stop Docker services
docker-compose down
```

### Check Status:
```bash
# Check Docker services
docker ps

# Check Spark processing
docker logs spark-analytics --tail 20

# Check LMDB data
python view_lmdb.py
```

### View Logs:
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f spark-analytics
docker-compose logs -f cassandra
docker-compose logs -f questdb
```

---

## 🎯 Expected Resource Usage

| Component | CPU | Memory | Disk |
|-----------|-----|--------|------|
| Docker Desktop | 2-4% | 2-3 GB | - |
| QuestDB | 1-2% | 500 MB | Growing |
| Cassandra | 5-10% | 1-2 GB | Growing |
| Neo4j | 2-5% | 500 MB | Growing |
| MongoDB | 1-2% | 200 MB | Growing |
| Spark Analytics | 50-80% (during runs) | 1-2 GB | - |
| Data Collection | 1-2% each | 100 MB each | - |
| Dashboard | 1-2% | 200 MB | - |

**Total**: ~6-8 GB RAM, 4 CPU cores recommended

---

## ✅ Success Indicators

You'll know everything is working when:

1. **Docker**: 7 containers running and healthy
2. **LMDB**: Growing .mdb files in data/reddit, data/twitter, data/onchain
3. **QuestDB**: Orderbook table has >1000 rows
4. **Cassandra**: 13 tables exist with data
5. **Neo4j**: Nodes and relationships exist
6. **Spark Logs**: "Pipeline completed successfully" every 5 minutes
7. **Dashboard**: All 3 stages show data, no "No data yet" messages

---

## 🚀 Next Steps After Startup

Once the dashboard is showing data:

1. **Watch for divergences** (Tab 2: Sentiment Analytics)
   - Bearish divergence = potential short opportunity
   - Bullish divergence = potential buy opportunity

2. **Monitor arbitrage** (Tab 1: Trading Signals)
   - Look for spreads > 10 BPS

3. **Check word correlations** (Tab 3: Text Analytics)
   - See which words predict price movements

4. **Watch flash events** (Tab 4: Technical Indicators)
   - Rapid price movements for quick trades

5. **Explore graph relationships** (Tab 5: Graph Insights)
   - Sentiment → Price causality networks

---

**Need Help?**
- Check logs: `docker-compose logs -f`
- Check service status: `docker ps`
- Verify data: `python view_lmdb.py` and `python query_all_data.py`

**Good luck! 🚀📊💰**
