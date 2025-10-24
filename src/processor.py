"""
Event processor untuk memproses event dari queue secara idempotent
Implementasi at-least-once delivery dengan deduplication
"""
import asyncio
import logging
from typing import Dict
from src.models import Event
from src.database import Database

logger = logging.getLogger(__name__)

class EventProcessor: 
    """
    Event processor yang memproses event secara idempotent.
    Menggunakan database untuk memastikan setiap event hanya diproses sekali.
    """
    
    def __init__(self, database: Database, queue: asyncio.Queue):
        self.database = database
        self.queue = queue
        self.running = False
        self.tasks = []
        
        # Statistik
        self.stats = {
            "received": 0,
            "unique_processed": 0,
            "duplicate_dropped": 0
        }
    
    async def start(self, num_workers: int = 3):
        """
        Start processor workers.
        Multiple workers untuk meningkatkan throughput.
        
        Args:
            num_workers: Jumlah worker threads
        """
        self.running = True
        logger.info(f"Starting {num_workers} processor workers")
        
        for i in range(num_workers):
            task = asyncio.create_task(self._worker(i))
            self.tasks.append(task)
    
    async def stop(self):
        """Stop semua processor workers"""
        self.running = False
        logger.info("Stopping processor workers")
        
        # Wait for all workers to finish
        for task in self.tasks:
            task.cancel()
        
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()
    
    async def _worker(self, worker_id: int):
        """
        Worker thread untuk memproses event dari queue.
        Implementasi idempotent processing dengan lazy dedup check.
        """
        logger.info(f"Worker {worker_id} started")
        
        while self.running:
            try:
                # Ambil event dari queue dengan timeout
                event = await asyncio.wait_for(
                    self.queue.get(),
                    timeout=1.0
                )
                
                await self._process_event(event, worker_id)
                self.queue.task_done()
                
            except asyncio.TimeoutError:
                # Timeout normal, continue waiting
                continue
            except asyncio.CancelledError:
                logger.info(f"Worker {worker_id} cancelled")
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}", exc_info=True)
    
    async def _process_event(self, event: Event, worker_id: int):
        """
        Process single event dengan idempotency check.
        
        Implementasi lazy idempotency:
        1. Check apakah event sudah pernah diproses
        2. Jika sudah, drop (increment duplicate counter)
        3. Jika belum, proses dan mark sebagai processed
        
        Args:
            event: Event object to process
            worker_id: ID worker yang memproses
        """
        self.stats["received"] += 1
        
        # Idempotency check
        is_duplicate = await self.database.is_processed(event.topic, event.event_id)
        
        if is_duplicate:
            # Kalai event sudah pernah diproses, drop sebagai duplicate
            self.stats["duplicate_dropped"] += 1
            logger.warning(
                f"[Worker {worker_id}] DUPLICATE DROPPED: topic={event.topic}, "
                f"event_id={event.event_id}"
            )
            return
        
        #  processing
        try:

            logger.info(
                f"[Worker {worker_id}] PROCESSING: topic={event.topic}, "
                f"event_id={event.event_id}, source={event.source}"
            )
            
            # Mark sebagai processed di database
            marked = await self.database.mark_processed(event.topic, event.event_id)
            
            if marked:
                self.stats["unique_processed"] += 1
                logger.info(
                    f"[Worker {worker_id}] PROCESSED: topic={event.topic}, "
                    f"event_id={event.event_id}"
                )
            else:
                # Race condition: event sudah dimark oleh worker lain
                self.stats["duplicate_dropped"] += 1
                logger.warning(
                    f"[Worker {worker_id}] RACE CONDITION DUPLICATE: "
                    f"topic={event.topic}, event_id={event.event_id}"
                )
        
        except Exception as e:
            logger.error(
                f"[Worker {worker_id}] ERROR processing event: "
                f"topic={event.topic}, event_id={event.event_id}, error={e}",
                exc_info=True
            )

    
    def get_stats(self) -> Dict:
        """Get processor statistics"""
        return self.stats.copy()
