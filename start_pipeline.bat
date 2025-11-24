@echo off
REM One-command startup script for Financial Analytics Pipeline (Windows)

echo ================================================================
echo   FINANCIAL ANALYTICS PIPELINE - DOCKER STARTUP
echo ================================================================
echo.

REM Check if Docker is running
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Docker is not running!
    echo Please start Docker Desktop and try again.
    exit /b 1
)

echo [OK] Docker is running
echo.

REM Check if data exists
if not exist "data\reddit" (
    echo [WARNING] data\reddit directory not found
    echo Creating directory... You'll need to run data collectors first.
    mkdir data\reddit
)

if exist "data\reddit\reddit_sentiment_lmdb.db\data.mdb" (
    echo [OK] LMDB sentiment data found
) else (
    echo [WARNING] LMDB sentiment data not found
    echo Run reddit_sentiment.py first to collect data
)

echo.
echo Starting all services...
echo   - QuestDB (time-series)
echo   - Cassandra (analytics storage)
echo   - MongoDB (document storage)
echo   - Neo4j (graph database)
echo   - Spark Analytics (processing pipeline)
echo.

REM Build and start services
docker-compose up --build -d

echo.
echo ================================================================
echo   [OK] All services started!
echo ================================================================
echo.
echo Access the following UIs:
echo   - QuestDB:        http://localhost:9000
echo   - Neo4j Browser:  http://localhost:7474 (neo4j/password)
echo   - Mongo Express:  http://localhost:8081 (admin/admin)
echo   - Cassandra Web:  http://localhost:3000
echo.
echo View logs:
echo   docker-compose logs -f
echo   docker-compose logs -f spark-analytics
echo.
echo Stop services:
echo   docker-compose down
echo.
echo Pipeline will start analytics processing in ~60 seconds...
echo ================================================================
