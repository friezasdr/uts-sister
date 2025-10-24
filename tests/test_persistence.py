"""
Unit tests untuk persistence dan crash recovery
"""
import pytest
import os
import tempfile
import asyncio
from src.database import Database
from src.processor import EventProcessor  # ← GANTI INI!
from src.models import Event
from datetime import datetime
import uuid

@pytest.mark.asyncio
async def test_restart_persistence():
    """
    Test: Database tetap efektif setelah restart.
    Simulasi crash dan restart container.
    """
    with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp:
        db_path = tmp.name
    
    try:
        # Phase 1: Initial processing
        db1 = Database(db_path)
        topic = "restart.test"
        event_id = "persistent_event"
        
        # Mark event as processed
        result = await db1.mark_processed(topic, event_id)
        assert result == True, "Initial mark should succeed"
        
        # Verify processed
        is_processed = await db1.is_processed(topic, event_id)
        assert is_processed == True
        
        # Simulate restart: create new database instance
        db2 = Database(db_path)
        
        # Verify dedup still works
        is_still_processed = await db2.is_processed(topic, event_id)
        assert is_still_processed == True, "Dedup state should persist"
        
        # Try to mark again (should fail due to dedup)
        result2 = await db2.mark_processed(topic, event_id)
        assert result2 == False, "Should detect duplicate after restart"
        
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

@pytest.mark.asyncio
async def test_processor_dedup_after_restart():  # ← GANTI nama function!
    """
    Test: Processor correctly handles duplicates after simulated restart
    """
    with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp:
        db_path = tmp.name
    
    try:
        # Setup
        db = Database(db_path)
        queue = asyncio.Queue()
        processor = EventProcessor(db, queue)  # ← GANTI INI!
        
        # Create test event
        event = Event(
            topic="processor.restart.test",
            event_id="restart_dedup_event",
            timestamp=datetime.utcnow().isoformat() + "Z",
            source="test",
            payload={}
        )
        
        # Start processor
        await processor.start(num_workers=1)
        await asyncio.sleep(0.5)  # Let worker start
        
        # Phase 1: Process event
        await queue.put(event)
        await asyncio.sleep(0.5)  # Let it process
        
        stats1 = processor.get_stats()
        assert stats1["unique_processed"] == 1
        assert stats1["duplicate_dropped"] == 0
        
        # Stop processor (simulate crash)
        await processor.stop()
        
        # Phase 2: Restart processor with same database
        new_queue = asyncio.Queue()
        new_processor = EventProcessor(db, new_queue)  # ← GANTI INI!
        await new_processor.start(num_workers=1)
        await asyncio.sleep(0.5)
        
        # Send same event (duplicate)
        await new_queue.put(event)
        await asyncio.sleep(0.5)
        
        stats2 = new_processor.get_stats()
        assert stats2["duplicate_dropped"] == 1, "Should detect duplicate after restart"
        
        await new_processor.stop()
        
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

@pytest.mark.asyncio
async def test_stress_dedup():
    """
    Test: Stress test dengan banyak event dan duplikasi
    Target: >= 5000 events dengan >= 20% duplikasi
    """
    with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp:
        db_path = tmp.name
    
    try:
        db = Database(db_path)
        queue = asyncio.Queue(maxsize=10000)
        processor = EventProcessor(db, queue)  # ← GANTI INI!
        
        await processor.start(num_workers=3)
        await asyncio.sleep(0.5)
        
        # Generate events dengan duplikasi
        total_events = 5000
        unique_events = 4000  # 20% akan duplicate
        
        event_pool = []
        for i in range(unique_events):
            event_pool.append(Event(
                topic=f"stress.topic.{i % 10}",
                event_id=f"stress_event_{i}",
                timestamp=datetime.utcnow().isoformat() + "Z",
                source="stress-test",
                payload={"index": i}
            ))
        
        # Add events ke queue (dengan duplikasi)
        import random
        for i in range(total_events):
            event = random.choice(event_pool)
            await queue.put(event)
        
        # Wait for processing
        await queue.join()
        await asyncio.sleep(1)  # Extra time untuk processing
        
        stats = processor.get_stats()
        
        # Validasi
        assert stats["received"] == total_events
        assert stats["unique_processed"] == unique_events
        duplicate_rate = stats["duplicate_dropped"] / stats["received"]
        assert duplicate_rate >= 0.18, f"Duplicate rate should be >= 18%, got {duplicate_rate:.2%}"
        
        await processor.stop()
        
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)
