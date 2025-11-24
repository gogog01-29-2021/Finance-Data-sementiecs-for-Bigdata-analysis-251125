# Quick Start Guide

Get the financial data pipeline running in 5 minutes!

## Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

## Step 2: Start Databases (Docker)

Open a terminal and run:

```bash
# Start all databases at once
docker run -d --name questdb -p 9000:9000 -p 8812:8812 questdb/questdb
docker run -d --name cassandra -p 9042:9042 cassandra:latest
docker run -d --name neo4j -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/password neo4j:latest
docker run -d --name mongodb -p 27017:27017 mongo:latest
```

Wait ~30 seconds for databases to initialize.

## Step 3: Verify .env File

Make sure your `.env` file exists with the FMP API key:

```bash
# Check if .env exists
cat .env
```

Should show:
```
FMP_API_KEY=qsmqIY80Q3Q5EkXquszcRkU5r11FHW0v
...
```

## Step 4: Run the Pipeline

```bash
python main.py
```

That's it! The pipeline will:
- ✅ Stream crypto orderbooks from 7 exchanges
- ✅ Poll stock prices for 7 symbols
- ✅ Collect Reddit sentiment from 9 subreddits
- ✅ Run analytics every 5 minutes

## Verify It's Working

### Check QuestDB
Open http://localhost:9000 in your browser and run:
```sql
SELECT * FROM orderbook LIMIT 10;
```

### Check Streaming Console
You should see output like:
```
[UPBIT:BTC-KRW] Δt=2.134s bid=65432100.00 ask=65433000.00
[reddit:CryptoCurrency] New posts: 3, New comments: 12
[spark] Analytics completed successfully
```

## Stop the Pipeline

Press `Ctrl+C` in the terminal. The pipeline will shutdown gracefully.

## Troubleshooting

**Problem:** Import errors
```bash
pip install -r requirements.txt --upgrade
```

**Problem:** Can't connect to QuestDB
```bash
# Check if QuestDB is running
curl http://localhost:9000
```

**Problem:** FMP API not working
- Verify your API key in `.env`
- Check rate limits at https://financialmodelingprep.com/

## Next Steps

- View full documentation in `README.md`
- Customize configuration in `.env`
- Run specific components: `python main.py --component streaming`
- Adjust analytics interval: `python main.py --analytics-interval 600`

## Quick Commands

```bash
# Run everything
python main.py

# Run only crypto/stock streaming
python main.py --component streaming

# Run only Reddit sentiment
python main.py --component reddit

# Run analytics once
python main.py --component analytics

# Change analytics to every 10 minutes
python main.py --analytics-interval 600
```
