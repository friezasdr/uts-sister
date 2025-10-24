"""
Model data untuk event dan validasi schema
"""
from pydantic import BaseModel, Field, validator
from typing import Dict, Any, Optional
from datetime import datetime
import uuid

class Event(BaseModel):
    """
    Model event log sesuai spesifikasi:
    - topic: kategori/topik event
    - event_id: identifier unik untuk deduplication
    - timestamp: waktu event dalam ISO8601
    - source: asal event/publisher
    - payload: data event dalam bentuk dictionary
    """
    topic: str = Field(..., min_length=1, max_length=255)
    event_id: str = Field(..., min_length=1)
    timestamp: str = Field(...)
    source: str = Field(..., min_length=1)
    payload: Dict[str, Any] = Field(default_factory=dict)
    
    @validator('timestamp')
    def validate_timestamp(cls, v):
        """Validasi format ISO8601 timestamp"""
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
            return v
        except ValueError:
            raise ValueError('Timestamp harus dalam format ISO8601')
    
    @validator('event_id')
    def validate_event_id(cls, v):
        """Validasi event_id tidak kosong"""
        if not v or not v.strip():
            raise ValueError('event_id tidak boleh kosong')
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "topic": "user.login",
                "event_id": "evt_" + str(uuid.uuid4()),
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "source": "auth-service",
                "payload": {"user_id": "123", "ip": "192.168.1.1"}
            }
        }

class EventBatch(BaseModel):
    """Model untuk batch event"""
    events: list[Event] = Field(..., min_items=1)

class StatsResponse(BaseModel):
    """Response model untuk GET /stats"""
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: list[str]
    uptime: float
