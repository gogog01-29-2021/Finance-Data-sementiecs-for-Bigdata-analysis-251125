 #!/usr/bin/env python3
"""Quick LMDB Database Viewer"""

import lmdb
import json
from pathlib import Path

DB_PATH = Path("data/reddit/reddit_sentiment_lmdb.db")

def view_lmdb():
    if not DB_PATH.exists():
        print(f"Database not found at: {DB_PATH}")
        print("Make sure reddit_sentiment.py has run and created data")
        return

    env = lmdb.open(str(DB_PATH), readonly=True)

    with env.begin() as txn:
        cursor = txn.cursor()

        print("=" * 80)
        print("LMDB DATABASE CONTENTS")
        print("=" * 80)

        # Count items
        posts = comments = sentiments = 0
        for key, _ in cursor:
            key_str = key.decode()
            if key_str.startswith("post:"):
                posts += 1
            elif key_str.startswith("comment:"):
                comments += 1
            elif key_str.startswith("sentiment:"):
                sentiments += 1

        print(f"\nStatistics:")
        print(f"  Posts: {posts}")
        print(f"  Comments: {comments}")
        print(f"  Sentiments: {sentiments}")
        print(f"  Total: {posts + comments + sentiments}")

        # Show sample data
        print("\nSample Data (first 10 items):")
        print("-" * 80)

        cursor.first()
        for i, (key, value) in enumerate(cursor):
            if i >= 10:
                break

            key_str = key.decode()
            data = json.loads(value.decode())

            print(f"\n{i+1}. Key: {key_str}")
            print(f"   Data: {str(data)[:100]}...")

    env.close()
    print("\n" + "=" * 80)

if __name__ == "__main__":
    view_lmdb()
