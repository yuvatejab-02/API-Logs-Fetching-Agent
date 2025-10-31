"""Script to test SigNoz API rate limits."""
import os
from datetime import datetime, timedelta, timezone
from src.signoz.fetcher import SigNozFetcher
from src.signoz.rate_limit_tester import RateLimitTester


def main():
    """Run rate limit tests."""
    # Get configuration from environment
    signoz_endpoint = os.getenv("SIGNOZ_ENDPOINT", "https://selected-starling.us.signoz.cloud")
    signoz_api_key = os.getenv("SIGNOZ_API_KEY", "")
    
    if not signoz_api_key:
        print("Error: SIGNOZ_API_KEY environment variable not set")
        return
    
    print("Initializing SigNoz fetcher...")
    fetcher = SigNozFetcher(
        api_endpoint=signoz_endpoint,
        api_key=signoz_api_key,
        timeout=30
    )
    
    # Calculate time range (last 1 hour)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)
    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)
    
    print(f"Testing rate limits for time range: {start_time} to {end_time}")
    print("This will take approximately 2-3 minutes...\n")
    
    # Run tests
    tester = RateLimitTester(fetcher)
    results = tester.run_test_suite(start_ms, end_ms)
    
    print("\n✅ Rate limit testing completed!")
    print("Check the generated files for detailed metrics and report.")


if __name__ == "__main__":
    main()


