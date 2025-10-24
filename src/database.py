"""
Database module untuk deduplication store menggunakan SQLite
Implementasi idempotent repository dengan SQLite sebagai backend
"""
import sqlite3
import aiosqlite
import logging
import os
from typing import Optional, Set
from pathlib import Path

logger = logging.getLogger(__name__)

class Database:
    """
    Database untuk tracking event yang sudah diproses.
    Menggunakan composite key (topic, event_id) untuk deduplication.
    Persistent menggunakan SQLite untuk tahan terhadap restart.
    """
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_db_dir()
        self._init_db()
    
    def _ensure_db_dir(self):
        """Pastikan direktori database exists"""
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            logger.info(f"Created database directory: {db_dir}")
    
    def _init_db(self):
        """
        Inisialisasi database dan tabel.
        Menggunakan UNIQUE constraint pada (topic, event_id) untuk enforce deduplication.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Tabel untuk menyimpan event yang sudah diproses
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                event_id TEXT NOT NULL,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(topic, event_id)
            )
        ''')
        
        # Index untuk mempercepat lookup
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_topic_event 
            ON processed_events(topic, event_id)
        ''')
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.db_path}")
    
    async def is_processed(self, topic: str, event_id: str) -> bool:
        """
        Check apakah event sudah pernah diproses.
        Implementasi lazy check untuk idempotency.
        
        Args:
            topic: Topic event
            event_id: Unique identifier event
            
        Returns:
            True jika event sudah pernah diproses, False jika belum
        """
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                'SELECT 1 FROM processed_events WHERE topic = ? AND event_id = ? LIMIT 1',
                (topic, event_id)
            )
            result = await cursor.fetchone()
            return result is not None
    
    async def mark_processed(self, topic: str, event_id: str) -> bool:
        """
        Mark event sebagai sudah diproses.
        Menggunakan INSERT OR IGNORE untuk atomicity.
        
        Args:
            topic: Topic event
            event_id: Unique identifier event
            
        Returns:
            True jika berhasil mark (event baru), False jika sudah ada (duplicate)
        """
        try:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute(
                    'INSERT OR IGNORE INTO processed_events (topic, event_id) VALUES (?, ?)',
                    (topic, event_id)
                )
                await db.commit()
                
                # Jika rowcount = 0, berarti event sudah ada (duplicate)
                is_new = cursor.rowcount > 0
                
                if is_new:
                    logger.debug(f"Marked as processed: topic={topic}, event_id={event_id}")
                else:
                    logger.warning(f"Duplicate detected: topic={topic}, event_id={event_id}")
                
                return is_new
        except Exception as e:
            logger.error(f"Error marking event as processed: {e}")
            return False
    
    async def get_all_topics(self) -> Set[str]:
        """Ambil semua unique topics yang sudah diproses"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute('SELECT DISTINCT topic FROM processed_events')
            rows = await cursor.fetchall()
            return {row[0] for row in rows}
    
    async def get_events_by_topic(self, topic: Optional[str] = None) -> list:
        """
        Ambil list event yang sudah diproses, optional filter by topic
        
        Args:
            topic: Optional topic filter
            
        Returns:
            List of (topic, event_id, processed_at) tuples
        """
        async with aiosqlite.connect(self.db_path) as db:
            if topic:
                cursor = await db.execute(
                    'SELECT topic, event_id, processed_at FROM processed_events WHERE topic = ? ORDER BY processed_at DESC',
                    (topic,)
                )
            else:
                cursor = await db.execute(
                    'SELECT topic, event_id, processed_at FROM processed_events ORDER BY processed_at DESC'
                )
            
            rows = await cursor.fetchall()
            return [{"topic": row[0], "event_id": row[1], "processed_at": row[2]} for row in rows]
    
    async def get_count(self) -> int:
        """Hitung total event yang sudah diproses"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute('SELECT COUNT(*) FROM processed_events')
            result = await cursor.fetchone()
            return result[0] if result else 0
    
    async def clear(self):
        """Clear semua data (untuk testing)"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('DELETE FROM processed_events')
            await db.commit()
            logger.info("Database cleared")
