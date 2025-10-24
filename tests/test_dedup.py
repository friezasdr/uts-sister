"""
Unit tests untuk deduplication functionality
"""
import pytest
import asyncio
import os
import tempfile
from src.database import Database 

@pytest.fixture
async def database():  
    """Fixture untuk temporary database"""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp:
        db_path = tmp.name
    
    db = Database(db_path)  
    yield db
    
    # Cleanup
    if os.path.exists(db_path):
        os.unlink(db_path)

@pytest.mark.asyncio
async def test_dedup_mark_and_check(database): 
    """Test: Mark event as processed dan check deduplication"""
    topic = "test.topic"
    event_id = "test_event_123"
    
    # First mark should succeed
    result = await database.mark_processed(topic, event_id)
    assert result == True, "First mark should succeed"
    
    # Check should return True (processed)
    is_processed = await database.is_processed(topic, event_id)
    assert is_processed == True, "Event should be marked as processed"
    
    # Second mark should fail (duplicate)
    result = await database.mark_processed(topic, event_id)
    assert result == False, "Second mark should fail (duplicate)"

@pytest.mark.asyncio
async def test_dedup_different_topics(database):  
    """Test: Same event_id pada different topics should be treated as different"""
    event_id = "same_id"
    topic1 = "topic.one"
    topic2 = "topic.two"
    
    # Mark di topic1
    result1 = await database.mark_processed(topic1, event_id)
    assert result1 == True
    
    # Mark di topic2 dengan event_id sama should succeed
    result2 = await database.mark_processed(topic2, event_id)
    assert result2 == True, "Same event_id on different topic should be allowed"
    
    # Check both
    assert await database.is_processed(topic1, event_id) == True
    assert await database.is_processed(topic2, event_id) == True

@pytest.mark.asyncio
async def test_dedup_persistence(database): 
    """Test: Database persistence across reconnection"""
    topic = "persist.topic"
    event_id = "persist_event"
    db_path = database.db_path
    
    # Mark event
    await database.mark_processed(topic, event_id)
    
    # Create new database instance with same db_path (simulasi restart)
    new_db = Database(db_path)
    
    # Check should still return True
    is_processed = await new_db.is_processed(topic, event_id)
    assert is_processed == True, "Dedup state should persist across restart"

@pytest.mark.asyncio
async def test_get_all_topics(database): 
    """Test: Get all unique topics"""
    await database.mark_processed("topic.a", "event1")
    await database.mark_processed("topic.b", "event2")
    await database.mark_processed("topic.a", "event3")
    
    topics = await database.get_all_topics()
    
    assert len(topics) == 2
    assert "topic.a" in topics
    assert "topic.b" in topics

@pytest.mark.asyncio
async def test_concurrent_dedup(database):  
    """Test: Concurrent marking same event (race condition)"""
    topic = "concurrent.topic"
    event_id = "concurrent_event"
    
    # Simulate concurrent marking
    results = await asyncio.gather(
        database.mark_processed(topic, event_id),
        database.mark_processed(topic, event_id),
        database.mark_processed(topic, event_id)
    )
    
    # Only one should succeed
    success_count = sum(1 for r in results if r)
    assert success_count == 1, "Only one concurrent mark should succeed"
