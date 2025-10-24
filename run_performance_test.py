"""
Performance test untuk Pub-Sub Aggregator
Test dengan >= 5000 events dan >= 20% duplicate rate
"""
import requests
import time
import uuid
from datetime import datetime
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "http://localhost:8080"

def generate_event(topic, event_id=None):
    """Generate event"""
    if event_id is None:
        event_id = f"perf_test_{uuid.uuid4()}"
    
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "performance-test",
        "payload": {"test": "data"}
    }

def publish_event(event):
    """Publish single event"""
    try:
        response = requests.post(f"{BASE_URL}/publish", json=event, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"Error: {e}")
        return False

def run_performance_test(total_events=5000, duplicate_rate=0.25, workers=10):
    """
    Run performance test
    
    Args:
        total_events: Total events to send
        duplicate_rate: Percentage of duplicates (0.0-1.0)
        workers: Number of parallel workers
    """
    print("=" * 60)
    print("PERFORMANCE TEST - Pub-Sub Log Aggregator")
    print("=" * 60)
    print(f"Total events: {total_events}")
    print(f"Duplicate rate: {duplicate_rate*100}%")
    print(f"Workers: {workers}")
    print("=" * 60)
    
    topics = ["perf.test.1", "perf.test.2", "perf.test.3"]
    
    # Generate event pool (unique events)
    unique_count = int(total_events * (1 - duplicate_rate))
    event_pool = []
    
    print(f"\nGenerating {unique_count} unique events...")
    for i in range(unique_count):
        topic = random.choice(topics)
        event = generate_event(topic)
        event_pool.append(event)
    
    # Create event list with duplicates
    events_to_send = event_pool.copy()
    duplicate_count = total_events - unique_count
    
    print(f"Adding {duplicate_count} duplicate events...")
    for _ in range(duplicate_count):
        events_to_send.append(random.choice(event_pool))
    
    # Shuffle
    random.shuffle(events_to_send)
    
    # Send events
    print(f"\nSending {len(events_to_send)} events with {workers} workers...")
    start_time = time.time()
    
    success_count = 0
    failed_count = 0
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(publish_event, event) for event in events_to_send]
        
        for i, future in enumerate(as_completed(futures)):
            if future.result():
                success_count += 1
            else:
                failed_count += 1
            
            # Progress indicator
            if (i + 1) % 500 == 0:
                print(f"  Progress: {i+1}/{len(events_to_send)} events sent")
    
    duration = time.time() - start_time
    
    # Results
    print("\n" + "=" * 60)
    print("TEST RESULTS")
    print("=" * 60)
    print(f"Duration: {duration:.2f} seconds")
    print(f"Throughput: {total_events/duration:.2f} events/second")
    print(f"Success: {success_count}")
    print(f"Failed: {failed_count}")
    
    # Wait a bit for processing
    print("\nWaiting 3 seconds for processing...")
    time.sleep(3)
    
    # Get stats
    print("\nFetching aggregator stats...")
    try:
        response = requests.get(f"{BASE_URL}/stats", timeout=10)
        if response.status_code == 200:
            stats = response.json()
            print("\nAGGREGATOR STATISTICS:")
            print(f"  Received: {stats['received']}")
            print(f"  Unique processed: {stats['unique_processed']}")
            print(f"  Duplicate dropped: {stats['duplicate_dropped']}")
            print(f"  Actual duplicate rate: {stats['duplicate_dropped']/stats['received']*100:.2f}%")
            print(f"  Topics: {', '.join(stats['topics'])}")
            
            # Validation
            print("\n" + "=" * 60)
            print("VALIDATION")
            print("=" * 60)
            
            # Check if meets requirements
            meets_total = stats['received'] >= 5000
            meets_duplicate = (stats['duplicate_dropped'] / stats['received']) >= 0.20
            
            print(f"✓ Total events >= 5000: {meets_total} ({stats['received']} events)")
            print(f"✓ Duplicate rate >= 20%: {meets_duplicate} ({stats['duplicate_dropped']/stats['received']*100:.2f}%)")
            
            if meets_total and meets_duplicate:
                print("\n🎉 TEST PASSED! Sistem memenuhi requirement minimal.")
            else:
                print("\n⚠️ TEST FAILED! Sistem belum memenuhi requirement.")
        else:
            print(f"Failed to get stats: {response.status_code}")
    except Exception as e:
        print(f"Error fetching stats: {e}")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    # Check if aggregator is running
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code != 200:
            print("Error: Aggregator is not healthy!")
            exit(1)
    except Exception as e:
        print(f"Error: Cannot connect to aggregator at {BASE_URL}")
        print(f"Make sure the aggregator is running!")
        exit(1)
    
    # Run test
    run_performance_test(
        total_events=5000,
        duplicate_rate=0.25,  # 25% duplicate
        workers=10
    )
