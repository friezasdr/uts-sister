"""
Main application: FastAPI server untuk Pub-Sub Log Aggregator
dengan Idempotent Consumer dan Deduplication
"""
import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from src.config import Config
from src.models import Event, EventBatch, StatsResponse
from src.database import Database
from src.processor import EventProcessor  # ← GANTI INI!

# Setup logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global state
database: Optional[Database] = None
processor: Optional[EventProcessor] = None  # ← GANTI INI!
event_queue: Optional[asyncio.Queue] = None
start_time: float = 0

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle manager untuk FastAPI application.
    Inisialisasi dan cleanup resources.
    """
    global database, processor, event_queue, start_time  # ← GANTI INI!
    
    # Startup
    logger.info("Starting Pub-Sub Log Aggregator")
    start_time = time.time()
    
    # Initialize database
    database = Database(Config.DEDUP_DB_PATH)
    
    # Initialize queue
    event_queue = asyncio.Queue(maxsize=Config.QUEUE_MAX_SIZE)
    
    # Initialize and start processor
    processor = EventProcessor(database, event_queue)  # ← GANTI INI!
    await processor.start(num_workers=Config.CONSUMER_WORKERS)
    
    logger.info(f"Application started on {Config.HOST}:{Config.PORT}")
    
    yield
    
    # Shutdown
    logger.info("Shutting down application")
    if processor:  # ← GANTI INI!
        await processor.stop()
    logger.info("Application stopped")

# Create FastAPI app
app = FastAPI(
    title="Pub-Sub Log Aggregator",
    description="Log aggregator dengan idempotent consumer dan deduplication",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Pub-Sub Log Aggregator",
        "version": "1.0.0",
        "status": "running"
    }

@app.post("/publish")
async def publish_event(event: Event):
    """
    Publish single event ke aggregator.
    Event akan dimasukkan ke queue untuk diproses oleh processor.
    
    Args:
        event: Event object dengan schema yang valid
        
    Returns:
        Success message dengan event_id
    """
    try:
        # Validasi event sudah dilakukan oleh Pydantic
        await event_queue.put(event)
        
        logger.info(f"Event published: topic={event.topic}, event_id={event.event_id}")
        
        return {
            "status": "accepted",
            "event_id": event.event_id,
            "topic": event.topic,
            "message": "Event queued for processing"
        }
    
    except asyncio.QueueFull:
        logger.error("Queue full, cannot accept more events")
        raise HTTPException(
            status_code=503,
            detail="Queue full, please try again later"
        )
    except Exception as e:
        logger.error(f"Error publishing event: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/publish/batch")
async def publish_batch(batch: EventBatch):
    """
    Publish batch events ke aggregator.
    Mendukung bulk publishing untuk efisiensi.
    
    Args:
        batch: Batch of events
        
    Returns:
        Success message dengan count
    """
    accepted = 0
    rejected = 0
    
    for event in batch.events:
        try:
            await event_queue.put(event)
            accepted += 1
        except asyncio.QueueFull:
            rejected += 1
            logger.warning(f"Queue full, event rejected: {event.event_id}")
    
    logger.info(f"Batch published: accepted={accepted}, rejected={rejected}")
    
    return {
        "status": "completed",
        "accepted": accepted,
        "rejected": rejected,
        "total": len(batch.events)
    }

@app.get("/events")
async def get_events(topic: Optional[str] = Query(None, description="Filter by topic")):
    """
    Get list of processed events.
    Optional filter by topic.
    
    Args:
        topic: Optional topic filter
        
    Returns:
        List of processed events
    """
    try:
        events = await database.get_events_by_topic(topic)
        
        return {
            "topic": topic if topic else "all",
            "count": len(events),
            "events": events
        }
    
    except Exception as e:
        logger.error(f"Error fetching events: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats", response_model=StatsResponse)
async def get_stats():
    """
    Get aggregator statistics.
    
    Returns:
        - received: Total event yang diterima
        - unique_processed: Total event unik yang diproses
        - duplicate_dropped: Total duplicate yang di-drop
        - topics: List unique topics
        - uptime: Uptime dalam seconds
    """
    try:
        processor_stats = processor.get_stats()  # ← GANTI INI!
        topics = await database.get_all_topics()
        uptime = time.time() - start_time
        
        return StatsResponse(
            received=processor_stats["received"],
            unique_processed=processor_stats["unique_processed"],
            duplicate_dropped=processor_stats["duplicate_dropped"],
            topics=sorted(list(topics)),
            uptime=round(uptime, 2)
        )
    
    except Exception as e:
        logger.error(f"Error fetching stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    queue_size = event_queue.qsize() if event_queue else 0
    
    return {
        "status": "healthy",
        "queue_size": queue_size,
        "uptime": round(time.time() - start_time, 2)
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "src.main:app",
        host=Config.HOST,
        port=Config.PORT,
        log_level=Config.LOG_LEVEL.lower()
    )
