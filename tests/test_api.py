"""
Unit tests untuk FastAPI endpoints
"""
import pytest
from fastapi.testclient import TestClient
from src.main import app
import uuid
from datetime import datetime
import asyncio
import time

# Override database path untuk testing
import os
os.environ["DEDUP_DB_PATH"] = "test_api.db"

@pytest.fixture(scope="module")
def client():
    """Test client fixture"""
    # Clean up database sebelum test
    if os.path.exists("test_api.db"):
        os.remove("test_api.db")
    
    with TestClient(app) as c:
        # Wait untuk startup
        time.sleep(1)
        yield c
    
    # Cleanup setelah test
    if os.path.exists("test_api.db"):
        os.remove("test_api.db")

def test_root_endpoint(client):
    """Test: Root endpoint returns service info"""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "service" in data
    assert "version" in data

def test_health_check(client):
    """Test: Health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "uptime" in data

def test_publish_event_valid(client):
    """Test: Publish valid event"""
    event = {
        "topic": "test.publish",
        "event_id": f"test_{uuid.uuid4()}",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "test-client",
        "payload": {"key": "value"}
    }
    
    response = client.post("/publish", json=event)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "accepted"
    assert data["event_id"] == event["event_id"]
    
    # Wait untuk processing
    time.sleep(0.5)

def test_publish_event_invalid_schema(client):
    """Test: Publish event with invalid schema"""
    invalid_event = {
        "topic": "",  # Invalid: empty topic
        "event_id": "test",
        "timestamp": "invalid-timestamp",
        "source": "test"
    }
    
    response = client.post("/publish", json=invalid_event)
    assert response.status_code == 422  # Validation error

def test_get_stats(client):
    """Test: Get statistics endpoint"""
    # Publish event dulu
    event = {
        "topic": "test.stats",
        "event_id": f"stats_{uuid.uuid4()}",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "test-client",
        "payload": {}
    }
    client.post("/publish", json=event)
    time.sleep(0.5)
    
    response = client.get("/stats")
    assert response.status_code == 200
    data = response.json()
    
    assert "received" in data
    assert "unique_processed" in data
    assert "duplicate_dropped" in data
    assert "topics" in data
    assert "uptime" in data
    assert isinstance(data["topics"], list)

def test_get_events(client):
    """Test: Get events endpoint"""
    # Publish event dulu
    event = {
        "topic": "test.events",
        "event_id": f"events_{uuid.uuid4()}",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "test-client",
        "payload": {}
    }
    client.post("/publish", json=event)
    time.sleep(0.5)
    
    response = client.get("/events")
    assert response.status_code == 200
    data = response.json()
    
    assert "count" in data
    assert "events" in data
    assert isinstance(data["events"], list)

def test_get_events_with_topic_filter(client):
    """Test: Get events with topic filter"""
    topic = "test.filter"
    event = {
        "topic": topic,
        "event_id": f"filter_{uuid.uuid4()}",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "test-client",
        "payload": {}
    }
    client.post("/publish", json=event)
    time.sleep(0.5)
    
    response = client.get(f"/events?topic={topic}")
    assert response.status_code == 200
    data = response.json()
    assert data["topic"] == topic
