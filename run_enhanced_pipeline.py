#!/usr/bin/env python3
"""
MASTER ENHANCED SEMANTIC ANALYTICS PIPELINE
Runs all components in the correct order and generates visualizations
"""

import asyncio
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime
import os


class EnhancedPipelineRunner:
    """Orchestrates the complete enhanced semantic analytics pipeline"""

    def __init__(self):
        self.root_dir = Path(__file__).parent
        self.data_collected = False

    def print_banner(self, title: str):
        """Print a formatted banner"""
        print("\n" + "=" * 80)
        print(title.center(80))
        print("=" * 80 + "\n")

    def run_python_script(self, script_name: str, duration_seconds: int = None):
        """Run a Python script"""
        script_path = self.root_dir / script_name

        if not script_path.exists():
            print(f"[FAIL] Script not found: {script_name}")
            return False

        print(f"> Running: {script_name}")

        try:
            if duration_seconds:
                # Run for limited time (data collectors)
                process = subprocess.Popen(
                    [sys.executable, str(script_path)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )

                print(f"  Running for {duration_seconds} seconds...")

                # Let it run
                time.sleep(duration_seconds)

                # Terminate
                process.terminate()

                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()

                print(f"  [OK] Completed {script_name}")
                return True

            else:
                # Run to completion (analytics, visualization)
                result = subprocess.run(
                    [sys.executable, str(script_path)],
                    capture_output=True,
                    text=True,
                    timeout=600  # 10 minute timeout
                )

                if result.returncode == 0:
                    print(f"  [OK] {script_name} completed successfully")
                    if result.stdout:
                        # Print last 20 lines of output
                        lines = result.stdout.split('\n')[-20:]
                        for line in lines:
                            if line.strip():
                                print(f"    {line}")
                    return True
                else:
                    print(f"  [FAIL] {script_name} failed with code {result.returncode}")
                    if result.stderr:
                        print(f"    Error: {result.stderr[-500:]}")  # Last 500 chars
                    return False

        except subprocess.TimeoutExpired:
            print(f"  [FAIL] {script_name} timed out")
            return False
        except Exception as e:
            print(f"  [FAIL] {script_name} error: {e}")
            return False

    async def collect_data_parallel(self, duration_seconds: int):
        """Run all data collectors in parallel"""
        self.print_banner("PHASE 1: DATA COLLECTION")

        print(f"Collecting data for {duration_seconds} seconds from:")
        print("  1. Reddit (enhanced sentiment)")
        print("  2. Twitter/X (simulated)")
        print("  3. On-chain data (simulated)")
        print()

        # Run collectors in parallel
        collectors = [
            ('reddit_sentiment.py', duration_seconds),
            ('twitter_sentiment.py', duration_seconds),
            ('onchain_data.py', duration_seconds)
        ]

        tasks = []
        for script, duration in collectors:
            task = asyncio.create_task(
                asyncio.to_thread(self.run_python_script, script, duration)
            )
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        success_count = sum(1 for r in results if r is True)
        print(f"\n[OK] Data collection completed: {success_count}/3 collectors succeeded")

        self.data_collected = True

    def run_analytics(self):
        """Run enhanced semantic analytics"""
        self.print_banner("PHASE 2: SEMANTIC ANALYTICS")

        print("Running enhanced semantic analytics pipeline...")
        print("This will analyze:")
        print("  1. Entity-specific sentiment (per crypto)")
        print("  2. Topic-price correlations")
        print("  3. Sentiment vs on-chain divergence (KEY INSIGHT!)")
        print("  4. Influence-weighted sentiment")
        print()

        success = self.run_python_script('spark_enhanced_semantic_analytics.py')

        if success:
            print("\n[OK] Analytics completed successfully")
        else:
            print("\n[FAIL] Analytics failed")

        return success

    def generate_visualizations(self):
        """Generate visualizations"""
        self.print_banner("PHASE 3: VISUALIZATION")

        print("Generating visualizations...")
        print("Creating:")
        print("  0. Summary dashboard")
        print("  1. Entity sentiment analysis")
        print("  2. Topic correlation")
        print("  3. Sentiment-onchain divergence")
        print("  4. Influence-weighted sentiment")
        print()

        success = self.run_python_script('visualize_semantic_analytics.py')

        if success:
            print("\n[OK] Visualizations generated successfully")
            print(f"\nView visualizations in: {self.root_dir / 'visualizations'}")
        else:
            print("\n[FAIL] Visualization failed")

        return success

    def show_results(self):
        """Show summary of results"""
        self.print_banner("PIPELINE COMPLETE!")

        print(" RESULTS SUMMARY")
        print()

        # Check MongoDB
        try:
            from pymongo import MongoClient
            client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
            client.server_info()

            db = client['financial_analytics']
            collection = db['enhanced_semantic_analytics']
            count = collection.count_documents({})

            print(f"[OK] MongoDB: {count} analytics records stored")

            # Count by type
            types = collection.distinct('analytics_type')
            print(f"  Analytics types: {len(types)}")
            for atype in types:
                type_count = collection.count_documents({'analytics_type': atype})
                print(f"    - {atype}: {type_count} records")

            client.close()

        except Exception as e:
            print(f"[FAIL] MongoDB check failed: {e}")

        print()

        # Check visualizations
        viz_dir = self.root_dir / "visualizations"
        if viz_dir.exists():
            viz_files = list(viz_dir.glob("*.png"))
            print(f"[OK] Visualizations: {len(viz_files)} PNG files generated")

            for viz_file in sorted(viz_files):
                size_kb = viz_file.stat().st_size / 1024
                print(f"    - {viz_file.name} ({size_kb:.1f} KB)")
        else:
            print("[FAIL] No visualizations directory found")

        print()
        print("=" * 80)
        print("NEXT STEPS:")
        print("=" * 80)
        print("1. View visualizations in: visualizations/")
        print("2. Query MongoDB: financial_analytics.enhanced_semantic_analytics")
        print("3. Compare old vs new analytics approach")
        print()
        print("KEY INSIGHTS TO LOOK FOR:")
        print("  - Divergences between sentiment and on-chain behavior")
        print("  - Entity-specific sentiment (not just overall)")
        print("  - Influence-weighted sentiment differences")
        print("  - Topic correlations with price movements")
        print("=" * 80)

    async def run_full_pipeline(self, collection_duration: int = 60):
        """Run the complete pipeline"""
        start_time = datetime.now()

        self.print_banner("ENHANCED SEMANTIC ANALYTICS PIPELINE")
        print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Data collection duration: {collection_duration} seconds")
        print()

        # Phase 1: Data Collection (parallel)
        await self.collect_data_parallel(collection_duration)

        # Phase 2: Analytics
        if self.data_collected:
            analytics_success = self.run_analytics()

            # Phase 3: Visualization
            if analytics_success:
                self.generate_visualizations()

        # Show results
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        self.show_results()

        print(f"\nTotal execution time: {duration:.1f} seconds")
        print(f"Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Enhanced Semantic Analytics Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_enhanced_pipeline.py                 # Run with 60 second collection
  python run_enhanced_pipeline.py --duration 120  # Run with 2 minute collection
  python run_enhanced_pipeline.py --quick         # Quick test (30 seconds)
        """
    )

    parser.add_argument(
        '--duration',
        type=int,
        default=60,
        help='Data collection duration in seconds (default: 60)'
    )

    parser.add_argument(
        '--quick',
        action='store_true',
        help='Quick test mode (30 seconds collection)'
    )

    args = parser.parse_args()

    duration = 30 if args.quick else args.duration

    print("""
================================================================================

           ENHANCED SEMANTIC ANALYTICS PIPELINE
           From Syntactic to Semantic Data Analysis

  Features:
  - Enhanced Reddit sentiment (entities, topics, urgency)
  - Twitter/X sentiment collection
  - On-chain data (whale movements, exchange flows)
  - Entity-specific sentiment analysis
  - Topic-price correlations
  - Sentiment vs on-chain divergence detection
  - Influence-weighted sentiment
  - Comprehensive visualizations

================================================================================
    """)

    runner = EnhancedPipelineRunner()

    try:
        asyncio.run(runner.run_full_pipeline(duration))
    except KeyboardInterrupt:
        print("\n\n[FAIL] Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n[FAIL] Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
