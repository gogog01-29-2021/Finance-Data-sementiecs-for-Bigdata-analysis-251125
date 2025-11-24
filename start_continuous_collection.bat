@echo off
REM Continuous Data Collection - Never Stops
REM Runs websocket and Reddit collection in background

echo ================================================================================
echo STARTING CONTINUOUS DATA COLLECTION
echo ================================================================================

echo.
echo [1] Starting websocket orderbook collection...
start "Websocket Collection" python main.py

echo.
echo [2] Waiting 5 seconds before starting Reddit collection...
timeout /t 5 /nobreak > nul

echo.
echo [3] Starting Reddit sentiment collection...
start "Reddit Sentiment" python reddit_sentiment.py

echo.
echo ================================================================================
echo COLLECTION STARTED!
echo ================================================================================
echo.
echo Two windows opened:
echo   - "Websocket Collection" - Collecting orderbook data
echo   - "Reddit Sentiment" - Collecting Reddit posts
echo.
echo These will run continuously until you close the windows.
echo.
echo To check data:
echo   - QuestDB: http://localhost:9000
echo   - LMDB: python view_lmdb.py
echo.
echo Press any key to exit this window (collection will continue)...
pause > nul
