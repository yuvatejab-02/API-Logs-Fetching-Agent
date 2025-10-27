#!/usr/bin/env python3
"""Test continuous polling system."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.utils.logger import setup_logging, get_logger
from src.utils.config import get_settings
from src.polling.incident_poller import IncidentPoller

setup_logging()
logger = get_logger(__name__)


def main():
    """Test continuous polling for an incident."""
    
    print("\n" + "="*80)
    print("  🔄 CONTINUOUS POLLING TEST")
    print("="*80 + "\n")
    
    # Initialize poller
    poller = IncidentPoller()
    
    # Test incident
    test_payload = {
        "compslug": "companyname",
        "Session_id": "7b3f2f30-0c4b-4c42-9f8f-7e3d2b8e2a61",
        "incident_id": "INC_polling_test",
        "title": "Spike in 5xx for payments",
        "service": {
            "id": "al89asf9asdhjfaslkdjfl",
            "name": "payments"
        }
    }
    
    print(f"📋 Incident: {test_payload['incident_id']}")
    print(f"🎯 Service: {test_payload['service']['name']}")
    print(f"⏱️  Duration: {get_settings().polling_duration_minutes} minutes")
    print(f"🔄 Interval: {get_settings().polling_interval_seconds} seconds")
    print("\n⚠️  Press Ctrl+C to stop polling early\n")
    print("="*80 + "\n")
    
    try:
        # Start polling
        result = poller.start_polling(
            incident_payload=test_payload,
            initial_lookback_hours=1  # ← Changed parameter name to match new code
        )
        
        # Print detailed summary
        print("\n" + "="*80)
        print("  ✅ POLLING COMPLETED")
        print("="*80)
        print(f"Total Polls: {result['total_polls']}")
        print(f"Total Logs Fetched: {result['total_logs_fetched']}")
        print(f"Unique Logs: {len(result['all_logs'])}")
        print(f"Duration: {int((result['end_time'] - result['start_time']).total_seconds())} seconds")
        print(f"Filter: {result['filter_expression']}")
        
        # Show per-poll breakdown
        if result.get('fetch_history'):
            print("\n" + "-"*80)
            print("  📊 FETCH HISTORY (Per-Poll Breakdown)")
            print("-"*80)
            for history in result['fetch_history']:
                poll_type = "🔵 INITIAL" if history.get('is_initial_poll') else "🟢 INCREMENTAL"
                print(f"\nPoll #{history['poll_number']} {poll_type}")
                print(f"  Time Window: {history['time_range']['start'][:19]} → {history['time_range']['end'][:19]}")
                print(f"  Logs Fetched: {history['log_count']}")
        
        print("\n" + "="*80)
        print(f"\n📁 Output: output/{test_payload['incident_id']}/")
        print("="*80 + "\n")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Polling stopped by user")
    except Exception as e:
        print(f"\n\n❌ Error: {str(e)}")
        logger.error("polling_test_failed", error=str(e), exc_info=True)


if __name__ == "__main__":
    main()
