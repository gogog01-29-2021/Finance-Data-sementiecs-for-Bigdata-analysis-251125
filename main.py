#!/usr/bin/env python3
"""
FINANCIAL DATA PIPELINE - Main Orchestrator
Runs all pipeline components: crypto/stock streaming, Reddit sentiment, and analytics
"""

import asyncio
import argparse
import signal
import sys
import os
from pathlib import Path
from dotenv import load_dotenv
import subprocess
import time
from datetime import datetime

# Load environment variables
load_dotenv()


class PipelineOrchestrator:
    """Orchestrates all components of the financial data pipeline"""

    def __init__(self):
        self.processes = {}
        self.running = True
        self.analytics_interval = 300  # Run analytics every 5 minutes

    def print_banner(self):
        """Print startup banner"""
        print("=" * 80)
        print("FINANCIAL DATA PIPELINE - MAIN ORCHESTRATOR")
        print("=" * 80)
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("\nComponents:")
        print("  1. Crypto + Stock Streaming (websocket3.py)")
        print("  2. Reddit Sentiment Collection (reddit_sentiment.py)")
        print("  3. PySpark Analytics (spark_analytics.py)")
        print("=" * 80)
        print()

    async def run_streaming(self):
        """Run crypto + stock streaming"""
        print("[main] Starting crypto + stock streaming...")

        # Import and run websocket3
        try:
            import websocket3
            await websocket3.main()
        except Exception as e:
            print(f"[main] Streaming error: {e}")
            raise

    async def run_reddit_sentiment(self):
        """Run Reddit sentiment collection"""
        print("[main] Starting Reddit sentiment collection...")

        # Import and run reddit_sentiment
        try:
            import reddit_sentiment
            await reddit_sentiment.main()
        except Exception as e:
            print(f"[main] Reddit sentiment error: {e}")
            raise

    async def run_analytics_periodic(self):
        """Run PySpark analytics periodically"""
        # Skip analytics if running in Docker (handled by spark-analytics container)
        if os.getenv("SKIP_ANALYTICS", "false").lower() == "true":
            print("[main] Analytics skipped (running in Docker, handled by spark-analytics container)")
            return

        print(f"[main] Starting analytics (runs every {self.analytics_interval}s)...")

        while self.running:
            try:
                print(f"\n[main] Running analytics at {datetime.now().strftime('%H:%M:%S')}...")

                # Run analytics in subprocess to avoid PySpark session conflicts
                process = subprocess.Popen(
                    [sys.executable, "spark_analytics.py"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )

                # Wait for completion with timeout
                try:
                    stdout, stderr = process.communicate(timeout=300)  # 5 min timeout

                    if process.returncode == 0:
                        print(f"[main] Analytics completed successfully")
                        if stdout:
                            print(f"[main] Analytics output:\n{stdout}")
                    else:
                        print(f"[main] Analytics failed with code {process.returncode}")
                        if stderr:
                            print(f"[main] Analytics errors:\n{stderr}")

                except subprocess.TimeoutExpired:
                    process.kill()
                    print("[main] Analytics timeout - process killed")

            except Exception as e:
                print(f"[main] Analytics error: {e}")

            # Wait for next interval
            await asyncio.sleep(self.analytics_interval)

    async def run_all(self):
        """Run all components concurrently"""
        self.print_banner()

        # Create tasks for all components
        tasks = [
            asyncio.create_task(self.run_streaming(), name="streaming"),
            asyncio.create_task(self.run_reddit_sentiment(), name="reddit"),
            asyncio.create_task(self.run_analytics_periodic(), name="analytics"),
        ]

        try:
            # Run all tasks concurrently
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            print("\n[main] Shutting down gracefully...")
            self.running = False
        except KeyboardInterrupt:
            print("\n[main] Keyboard interrupt received...")
            self.running = False
        except Exception as e:
            print(f"\n[main] Fatal error: {e}")
            self.running = False
        finally:
            # Cancel all tasks
            for task in tasks:
                if not task.done():
                    task.cancel()

            # Wait for tasks to complete cancellation
            await asyncio.gather(*tasks, return_exceptions=True)
            print("[main] All components stopped")

    async def run_component(self, component: str):
        """Run a single component"""
        self.print_banner()
        print(f"[main] Running only: {component}\n")

        if component == "streaming":
            await self.run_streaming()
        elif component == "reddit":
            await self.run_reddit_sentiment()
        elif component == "analytics":
            # For analytics, run once instead of periodically
            print("[main] Running analytics once...")
            process = subprocess.Popen(
                [sys.executable, "spark_analytics.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate()

            if process.returncode == 0:
                print("[main] Analytics completed successfully")
                if stdout:
                    print(stdout)
            else:
                print(f"[main] Analytics failed")
                if stderr:
                    print(stderr)
        else:
            print(f"[main] Unknown component: {component}")
            sys.exit(1)


def setup_signal_handlers(loop, orchestrator):
    """Setup signal handlers for graceful shutdown"""
    def signal_handler(signum, frame):
        print(f"\n[main] Received signal {signum}")
        orchestrator.running = False

        # Cancel all tasks
        for task in asyncio.all_tasks(loop):
            task.cancel()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Financial Data Pipeline Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                    # Run all components
  python main.py --component streaming      # Run only crypto/stock streaming
  python main.py --component reddit         # Run only Reddit sentiment
  python main.py --component analytics      # Run analytics once
  python main.py --analytics-interval 600   # Run analytics every 10 minutes
        """
    )

    parser.add_argument(
        "--component",
        choices=["streaming", "reddit", "analytics"],
        help="Run only a specific component (default: run all)"
    )

    parser.add_argument(
        "--analytics-interval",
        type=int,
        default=300,
        help="Analytics run interval in seconds (default: 300 = 5 minutes)"
    )

    parser.add_argument(
        "--no-analytics",
        action="store_true",
        help="Disable periodic analytics when running all components"
    )

    args = parser.parse_args()

    # Create orchestrator
    orchestrator = PipelineOrchestrator()
    orchestrator.analytics_interval = args.analytics_interval

    # Check if .env file exists
    if not Path(".env").exists():
        print("ERROR: .env file not found!")
        print("Please create .env file with your configuration.")
        print("See .env.example for template.")
        sys.exit(1)

    # Verify required environment variables
    required_vars = ["FMP_API_KEY"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"ERROR: Missing required environment variables: {', '.join(missing_vars)}")
        print("Please check your .env file.")
        sys.exit(1)

    # Run the pipeline
    try:
        if args.component:
            # Run single component
            asyncio.run(orchestrator.run_component(args.component))
        else:
            # Run all components
            if args.no_analytics:
                print("[main] Analytics disabled")
                # Create modified orchestrator without analytics
                async def run_without_analytics():
                    orchestrator.print_banner()
                    tasks = [
                        asyncio.create_task(orchestrator.run_streaming(), name="streaming"),
                        asyncio.create_task(orchestrator.run_reddit_sentiment(), name="reddit"),
                    ]
                    try:
                        await asyncio.gather(*tasks)
                    except (asyncio.CancelledError, KeyboardInterrupt):
                        print("\n[main] Shutting down...")
                        for task in tasks:
                            if not task.done():
                                task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)

                asyncio.run(run_without_analytics())
            else:
                # Run all including analytics
                asyncio.run(orchestrator.run_all())

    except KeyboardInterrupt:
        print("\n[main] Stopped by user")
    except Exception as e:
        print(f"\n[main] Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print("\n[main] Pipeline shutdown complete")


if __name__ == "__main__":
    main()
