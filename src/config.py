"""
Konfigurasi aplikasi log aggregator
"""
import os

class Config:
    # Konfigurasi server
    HOST = os.getenv("HOST", "0.0.0.0")
    PORT = int(os.getenv("PORT", 8080))
    
    # Konfigurasi dedup store
    DEDUP_DB_PATH = os.getenv("DEDUP_DB_PATH", "/app/data/dedup.db")
    
    # Konfigurasi queue
    QUEUE_MAX_SIZE = int(os.getenv("QUEUE_MAX_SIZE", 10000))
    
    # Konfigurasi consumer
    CONSUMER_WORKERS = int(os.getenv("CONSUMER_WORKERS", 3))
    
    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
