"""Test Turso database connection with proper fallback handling."""

import asyncio
import os
from dotenv import load_dotenv
from turso_db import TursoDatabase

load_dotenv()

async def main():
    # Get credentials from environment
    db_url = os.getenv("TURSO_DATABASE_URL")
    db_token = os.getenv("TURSO_AUTH_TOKEN")

    if not db_url or not db_token:
        print("ERROR: Missing TURSO_DATABASE_URL or TURSO_AUTH_TOKEN in .env")
        return

    print(f"Testing connection to: {db_url}")
    print(f"Token (first 20 chars): {db_token[:20]}...")

    # Initialize database with fallback handling
    db = TursoDatabase(db_url, db_token)

    try:
        await db.connect()
        print(f"✓ Successfully connected to Turso database!")
        print(f"  Final URL used: {db.url}")

        # Test creating a conversation
        conv_id = "test-conv-001"
        conv = await db.create_conversation(
            conversation_id=conv_id,
            title="Test Conversation",
            model="claude-3-5-sonnet"
        )
        print(f"✓ Created conversation: {conv['id']}")

        # Test adding a message
        msg = await db.add_message(
            conversation_id=conv_id,
            role="user",
            content="Hello, Turso!"
        )
        print(f"✓ Added message with ID: {msg['id']}")

        # Test retrieving messages
        messages = await db.get_conversation_messages(conv_id)
        print(f"✓ Retrieved {len(messages)} message(s)")
        for m in messages:
            print(f"  - [{m['role']}]: {m['content']}")

        # Clean up
        await db.delete_conversation(conv_id)
        print(f"✓ Deleted test conversation")

        print("\n✅ All tests passed! Database is working correctly.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())
