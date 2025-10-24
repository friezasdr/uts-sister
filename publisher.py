"""
Publisher simulator untuk testing Pub-Sub Aggregator
Digunakan untuk demo duplicate detection dan load testing
"""
import requests
import time
import uuid
from datetime import datetime
import random
import sys
import os

# URL aggregator (untuk Docker Compose)
AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://localhost:8080")

def generate_event(topic, event_id=None):
    """Generate event dengan format yang benar"""
    if event_id is None:
        event_id = f"evt_{uuid.uuid4()}"
    
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "publisher-simulator",
        "payload": {
            "message": f"Test message for {topic}",
            "random_value": random.randint(1, 1000)
        }
    }

def publish_event(event):
    """Publish event ke aggregator"""
    try:
        response = requests.post(
            f"{AGGREGATOR_URL}/publish",
            json=event,
            timeout=5
        )
        return response.status_code == 200
    except requests.exceptions.RequestException as e:
        print(f" Error publishing: {e}")
        return False

def wait_for_aggregator(max_retries=30):
    """Wait untuk aggregator ready"""
    print(f" Waiting for aggregator at {AGGREGATOR_URL}...")
    for i in range(max_retries):
        try:
            response = requests.get(f"{AGGREGATOR_URL}/health", timeout=2)
            if response.status_code == 200:
                print(f" Aggregator is ready!")
                return True
        except:
            pass
        time.sleep(1)
    
    print(" Aggregator might not be ready, but continuing anyway...")
    return False

def run_demo():
    """
    Demo publisher dengan duplicate detection
    """
    print("=" * 60)
    print("🚀 PUBLISHER DEMO - Pub-Sub Log Aggregator")
    print("=" * 60)
    
    # Wait for aggregator
    wait_for_aggregator()
    
    topics = ["user.login", "user.logout", "order.created", "payment.processed"]
    event_pool = []
    
    total_events = 100
    duplicate_rate = 0.3  # 30% duplicate
    
    print(f"\n Configuration:")
    print(f"  Total events: {total_events}")
    print(f"  Duplicate rate: {duplicate_rate * 100}%")
    print(f"  Topics: {', '.join(topics)}")
    print("\n" + "=" * 60)
    
    sent = 0
    duplicates_sent = 0
    
    for i in range(total_events):
        # Decide: new event atau duplicate?
        if event_pool and random.random() < duplicate_rate:
            # Kirim duplicate
            event = random.choice(event_pool)
            is_dup = True
            duplicates_sent += 1
        else:
            # Buat event baru
            topic = random.choice(topics)
            event = generate_event(topic)
            event_pool.append(event)
            is_dup = False
            
            # Limit pool size
            if len(event_pool) > 50:
                event_pool.pop(0)
        
        # Publish
        success = publish_event(event)
        
        if success:
            sent += 1
            status = " DUP" if is_dup else " NEW"
            print(f"[{sent:3d}] {status} | {event['topic']:20} | {event['event_id'][:25]}")
        else:
            print(f"[FAIL] Could not publish event")
        
        # Small delay
        time.sleep(0.1)
    
    # Summary
    print("\n" + "=" * 60)
    print(" PUBLISHER SUMMARY")
    print("=" * 60)
    print(f" Total sent: {sent}")
    print(f" Duplicates sent: {duplicates_sent}")
    print(f" Duplicate rate: {duplicates_sent/sent*100:.1f}%")
    
    # Get stats dari aggregator
    print("\n Fetching aggregator stats...")
    time.sleep(1)
    
    try:
        response = requests.get(f"{AGGREGATOR_URL}/stats", timeout=5)
        if response.status_code == 200:
            stats = response.json()
            print("\n AGGREGATOR STATISTICS:")
            print("=" * 60)
            print(f"Received: {stats['received']}")
            print(f"Unique processed: {stats['unique_processed']}")
            print(f"Duplicate dropped: {stats['duplicate_dropped']}")
            print(f"Topics: {', '.join(stats['topics'])}")
            print(f"Uptime: {stats['uptime']:.1f}s")
            
            # Validation
            if stats['duplicate_dropped'] > 0:
                print("\n DEDUPLICATION WORKING!")
                print(f"   {stats['duplicate_dropped']} duplicates detected and dropped")
            else:
                print("\n  No duplicates detected (might need more events)")
        else:
            print(f" Failed to get stats: {response.status_code}")
    except Exception as e:
        print(f" Error fetching stats: {e}")
    
    print("\n" + "=" * 60)
    print(" Demo completed!")
    print("=" * 60)

if __name__ == "__main__":
    run_demo()
