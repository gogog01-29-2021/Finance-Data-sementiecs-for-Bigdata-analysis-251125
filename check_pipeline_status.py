#!/usr/bin/env python3
"""
PIPELINE STATUS CHECKER
Quick script to verify what's running and what needs to be started
"""

import subprocess
import sys
from pathlib import Path
import psycopg2
from cassandra.cluster import Cluster
from neo4j import GraphDatabase
from pymongo import MongoClient
import lmdb


def print_section(title):
    """Print section header"""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def check_docker():
    """Check if Docker is installed and running"""
    print_section("1. DOCKER STATUS")

    try:
        result = subprocess.run(
            ["docker", "--version"],
            capture_output=True,
            text=True,
            timeout=5
        )

        if result.returncode == 0:
            print(f"✅ Docker installed: {result.stdout.strip()}")
        else:
            print("❌ Docker not found or not working")
            return False

    except FileNotFoundError:
        print("❌ Docker not installed")
        print("   → Install Docker Desktop: https://www.docker.com/products/docker-desktop/")
        return False
    except subprocess.TimeoutExpired:
        print("❌ Docker command timeout")
        return False

    # Check if Docker daemon is running
    try:
        result = subprocess.run(
            ["docker", "ps"],
            capture_output=True,
            text=True,
            timeout=5
        )

        if result.returncode == 0:
            print("✅ Docker daemon running")
            return True
        else:
            print("❌ Docker daemon not running")
            print("   → Start Docker Desktop from Start menu")
            return False

    except Exception as e:
        print(f"❌ Docker check failed: {e}")
        return False


def check_docker_services():
    """Check which Docker services are running"""
    print_section("2. DOCKER SERVICES STATUS")

    try:
        result = subprocess.run(
            ["docker-compose", "ps"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            if len(lines) > 1:
                print("Running containers:")
                print(result.stdout)

                # Count running containers
                running = sum(1 for line in lines[2:] if 'Up' in line)
                print(f"\n{running} containers running")

                if running == 0:
                    print("\n⚠️  No containers running!")
                    print("   → Run: start_pipeline.bat")
                elif running < 7:
                    print("\n⚠️  Some containers missing (expected 7)")
                    print("   → Run: docker-compose up -d")
                else:
                    print("\n✅ All containers running!")
            else:
                print("❌ No containers found")
                print("   → Run: start_pipeline.bat")
        else:
            print("❌ docker-compose not available")

    except FileNotFoundError:
        print("⚠️  docker-compose not found, trying 'docker compose'...")
        try:
            result = subprocess.run(
                ["docker", "compose", "ps"],
                capture_output=True,
                text=True,
                timeout=10
            )
            print(result.stdout if result.returncode == 0 else "❌ docker compose failed")
        except:
            print("❌ Docker Compose not available")

    except Exception as e:
        print(f"❌ Error checking services: {e}")


def check_databases():
    """Check database connections"""
    print_section("3. DATABASE CONNECTIONS")

    # QuestDB
    print("\n[QuestDB]")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=8812,
            user="admin",
            password="quest",
            database="qdb",
            connect_timeout=3
        )

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM orderbook")
        count = cursor.fetchone()[0]

        print(f"✅ QuestDB connected: {count:,} orderbook records")
        conn.close()

    except Exception as e:
        print(f"❌ QuestDB not accessible: {e}")
        print("   → Check if questdb container is running")

    # Cassandra
    print("\n[Cassandra]")
    try:
        cluster = Cluster(["localhost"], port=9042, connect_timeout=3)
        session = cluster.connect("financial_data")

        # Check key tables
        tables = ['arbitrage_opportunities', 'sentiment_correlations', 'price_predictions']
        table_counts = {}

        for table in tables:
            try:
                rows = session.execute(f"SELECT COUNT(*) FROM {table}")
                count = rows.one()[0]
                table_counts[table] = count
            except:
                table_counts[table] = "N/A"

        print(f"✅ Cassandra connected")
        for table, count in table_counts.items():
            print(f"   - {table}: {count}")

        session.shutdown()
        cluster.shutdown()

    except Exception as e:
        print(f"❌ Cassandra not accessible: {e}")
        print("   → Check if cassandra container is running")

    # Neo4j
    print("\n[Neo4j]")
    try:
        driver = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", "password"),
            connection_timeout=3
        )

        with driver.session() as session:
            result = session.run("MATCH (n) RETURN COUNT(n) as count")
            count = result.single()["count"]
            print(f"✅ Neo4j connected: {count} nodes")

        driver.close()

    except Exception as e:
        print(f"❌ Neo4j not accessible: {e}")
        print("   → Check if neo4j container is running")

    # MongoDB
    print("\n[MongoDB]")
    try:
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=3000)
        db = client["financial_analytics"]

        collections = db.list_collection_names()
        print(f"✅ MongoDB connected: {len(collections)} collections")

        for col in collections[:3]:
            count = db[col].count_documents({})
            print(f"   - {col}: {count} documents")

        client.close()

    except Exception as e:
        print(f"❌ MongoDB not accessible: {e}")
        print("   → Check if mongodb container is running")


def check_lmdb_data():
    """Check LMDB data files"""
    print_section("4. STAGE 1: RAW DATA COLLECTION (LMDB)")

    data_dir = Path("data")

    lmdb_paths = [
        ("Reddit", data_dir / "reddit" / "reddit_sentiment_lmdb.db"),
        ("Twitter", data_dir / "twitter" / "twitter_sentiment.db"),
        ("On-Chain", data_dir / "onchain" / "onchain_data.db")
    ]

    for name, path in lmdb_paths:
        print(f"\n[{name}]")

        if not path.exists():
            print(f"❌ Database not found: {path}")
            print(f"   → Run: python {name.lower()}_sentiment.py")
            continue

        try:
            env = lmdb.open(str(path), readonly=True)

            with env.begin() as txn:
                count = txn.stat()['entries']
                print(f"✅ {name} LMDB: {count:,} records")

            env.close()

        except Exception as e:
            print(f"⚠️  {name} LMDB exists but can't read: {e}")


def check_spark_logs():
    """Check Spark analytics container logs"""
    print_section("5. STAGE 2: SPARK PROCESSING STATUS")

    try:
        result = subprocess.run(
            ["docker", "logs", "spark-analytics", "--tail", "50"],
            capture_output=True,
            text=True,
            timeout=5
        )

        if result.returncode == 0:
            logs = result.stdout

            # Check for success indicator
            if "Pipeline completed successfully" in logs:
                print("✅ Spark analytics running successfully")

                # Find last success timestamp
                lines = logs.split('\n')
                for line in reversed(lines):
                    if "Pipeline completed successfully" in line:
                        print(f"   Last successful run: {line[:50]}...")
                        break
            elif "Pipeline failed" in logs:
                print("❌ Spark analytics failing")
                print("   → Check logs: docker logs spark-analytics --tail 100")
            elif "Waiting for databases" in logs:
                print("⏳ Spark analytics waiting for databases to be ready")
            else:
                print("⚠️  Spark analytics status unclear")
                print("   → Check logs: docker logs spark-analytics")

        else:
            print("❌ spark-analytics container not found")
            print("   → Run: start_pipeline.bat")

    except FileNotFoundError:
        print("❌ Docker not available")
    except Exception as e:
        print(f"❌ Error checking Spark logs: {e}")


def print_summary():
    """Print summary and next steps"""
    print_section("SUMMARY & NEXT STEPS")

    print("""
If you see ❌ errors above, follow these steps:

1. Install Docker Desktop (if not installed)
   → https://www.docker.com/products/docker-desktop/

2. Start Docker Desktop (if not running)
   → Open from Start menu, wait for whale icon

3. Start all database services
   → Run: start_pipeline.bat
   → Wait 60 seconds for services to be ready

4. Start data collection scripts
   → Run: python main.py
   → Or run individually: reddit_sentiment.py, twitter_sentiment.py, etc.

5. Wait 10 minutes for data to flow through pipeline
   → Stage 1: Data collection fills LMDB
   → Stage 2: Spark processes data every 5 minutes
   → Stage 3: Enriched analytics fill Cassandra/Neo4j/MongoDB

6. Launch dashboard
   → Run: streamlit run live_dashboard.py
   → Open: http://localhost:8501

For detailed instructions, see: STARTUP_GUIDE.md
    """)

    print("=" * 80)


def main():
    """Main status check"""
    print("\n🔍 FINANCIAL ANALYTICS PIPELINE - STATUS CHECK")
    print("=" * 80)

    # Install check
    try:
        import streamlit
        print("✅ Dashboard dependencies installed")
    except ImportError:
        print("⚠️  Dashboard dependencies missing")
        print("   → Run: pip install -r dashboard_requirements.txt")

    # Run all checks
    docker_ok = check_docker()

    if docker_ok:
        check_docker_services()
        check_databases()

    check_lmdb_data()

    if docker_ok:
        check_spark_logs()

    print_summary()


if __name__ == "__main__":
    main()
