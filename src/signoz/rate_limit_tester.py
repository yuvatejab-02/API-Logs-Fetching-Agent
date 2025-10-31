"""Rate limiting test suite for SigNoz API."""
import time
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class APICallMetric:
    """Metrics for a single API call."""
    timestamp: str
    signal_type: str
    response_time_ms: float
    status_code: int
    page_size: int
    success: bool
    error_message: Optional[str] = None


class RateLimitTester:
    """Test SigNoz API rate limits and find optimal configurations."""
    
    def __init__(self, fetcher):
        """Initialize rate limit tester.
        
        Args:
            fetcher: SigNozFetcher instance
        """
        self.fetcher = fetcher
        self.metrics: List[APICallMetric] = []
        
    def run_test_suite(self, start_ms: int, end_ms: int) -> Dict[str, Any]:
        """Run comprehensive rate limiting tests.
        
        Args:
            start_ms: Start time in epoch milliseconds
            end_ms: End time in epoch milliseconds
            
        Returns:
            Dictionary with test results
        """
        logger.info("starting_rate_limit_test_suite")
        
        test_scenarios = [
            {"concurrent": 1, "page_size": 1000, "duration": 30, "name": "baseline_single_thread"},
            {"concurrent": 3, "page_size": 1000, "duration": 30, "name": "current_config"},
            {"concurrent": 5, "page_size": 1000, "duration": 30, "name": "high_concurrency"},
            {"concurrent": 3, "page_size": 500, "duration": 30, "name": "small_pages"},
        ]
        
        results = {}
        
        for scenario in test_scenarios:
            logger.info("testing_scenario", scenario=scenario['name'])
            
            try:
                result = self._run_scenario(scenario, start_ms, end_ms)
                results[scenario['name']] = result
                
                # Save intermediate results
                self._save_metrics(scenario['name'])
                
                # Wait between scenarios to avoid rate limiting
                time.sleep(5)
                
            except Exception as e:
                logger.error("scenario_test_failed", scenario=scenario['name'], error=str(e))
                results[scenario['name']] = {"error": str(e)}
        
        # Generate final report
        report = self._generate_report(results)
        self._save_report(report)
        
        logger.info("rate_limit_test_suite_completed", total_scenarios=len(results))
        
        return results
    
    def _run_scenario(self, scenario: Dict[str, Any], start_ms: int, end_ms: int) -> Dict[str, Any]:
        """Run a single test scenario.
        
        Args:
            scenario: Test scenario configuration
            start_ms: Start time in epoch milliseconds
            end_ms: End time in epoch milliseconds
            
        Returns:
            Scenario results
        """
        concurrent = scenario['concurrent']
        page_size = scenario['page_size']
        duration = scenario['duration']
        
        start_time = time.time()
        end_time = start_time + duration
        
        successful_calls = 0
        failed_calls = 0
        total_response_time = 0
        
        with ThreadPoolExecutor(max_workers=concurrent) as executor:
            while time.time() < end_time:
                futures = []
                
                # Submit batch of concurrent requests
                for _ in range(concurrent):
                    future = executor.submit(
                        self._make_test_request,
                        start_ms,
                        end_ms,
                        page_size
                    )
                    futures.append(future)
                
                # Wait for batch to complete
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result['success']:
                            successful_calls += 1
                            total_response_time += result['response_time_ms']
                        else:
                            failed_calls += 1
                    except Exception as e:
                        logger.error("future_result_failed", error=str(e))
                        failed_calls += 1
                
                # Small delay between batches
                time.sleep(0.5)
        
        actual_duration = time.time() - start_time
        total_calls = successful_calls + failed_calls
        avg_response_time = total_response_time / successful_calls if successful_calls > 0 else 0
        
        return {
            "concurrent": concurrent,
            "page_size": page_size,
            "duration_seconds": actual_duration,
            "successful_calls": successful_calls,
            "failed_calls": failed_calls,
            "total_calls": total_calls,
            "avg_response_time_ms": round(avg_response_time, 2),
            "calls_per_second": round(total_calls / actual_duration, 2),
            "success_rate": round(successful_calls / total_calls * 100, 2) if total_calls > 0 else 0
        }
    
    def _make_test_request(self, start_ms: int, end_ms: int, page_size: int) -> Dict[str, Any]:
        """Make a single test API request.
        
        Args:
            start_ms: Start time in epoch milliseconds
            end_ms: End time in epoch milliseconds
            page_size: Page size for request
            
        Returns:
            Request result
        """
        request_start = time.time()
        
        try:
            # Use fetch_logs for testing
            response = self.fetcher.fetch_logs(
                start_ms=start_ms,
                end_ms=end_ms,
                filter_expression="",
                limit=page_size,
                incident_id="rate_test"
            )
            
            response_time = (time.time() - request_start) * 1000
            
            # Record metric
            metric = APICallMetric(
                timestamp=datetime.now().isoformat(),
                signal_type="logs",
                response_time_ms=round(response_time, 2),
                status_code=200,
                page_size=page_size,
                success=True
            )
            self.metrics.append(metric)
            
            return {"success": True, "response_time_ms": response_time}
            
        except Exception as e:
            response_time = (time.time() - request_start) * 1000
            
            # Record failed metric
            metric = APICallMetric(
                timestamp=datetime.now().isoformat(),
                signal_type="logs",
                response_time_ms=round(response_time, 2),
                status_code=500,
                page_size=page_size,
                success=False,
                error_message=str(e)
            )
            self.metrics.append(metric)
            
            return {"success": False, "response_time_ms": response_time}
    
    def _save_metrics(self, scenario_name: str):
        """Save metrics to JSON file.
        
        Args:
            scenario_name: Name of the scenario
        """
        try:
            filename = f"rate_limit_metrics_{scenario_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(filename, 'w') as f:
                json.dump({
                    "scenario": scenario_name,
                    "timestamp": datetime.now().isoformat(),
                    "total_metrics": len(self.metrics),
                    "metrics": [asdict(m) for m in self.metrics]
                }, f, indent=2)
            
            logger.info("metrics_saved", filename=filename, count=len(self.metrics))
            
        except Exception as e:
            logger.error("failed_to_save_metrics", error=str(e))
    
    def _generate_report(self, results: Dict[str, Any]) -> str:
        """Generate human-readable report.
        
        Args:
            results: Test results
            
        Returns:
            Report string
        """
        report = f"""
SigNoz API Rate Limiting Test Report
=====================================
Generated: {datetime.now().isoformat()}

TEST SCENARIOS:
---------------
"""
        
        for name, result in results.items():
            if "error" in result:
                report += f"\n{name}: FAILED - {result['error']}\n"
                continue
            
            report += f"""
{name}:
  Concurrency: {result['concurrent']}
  Page Size: {result['page_size']}
  Duration: {result['duration_seconds']:.1f}s
  Total Calls: {result['total_calls']}
  Successful: {result['successful_calls']}
  Failed: {result['failed_calls']}
  Success Rate: {result['success_rate']}%
  Avg Response Time: {result['avg_response_time_ms']:.2f}ms
  Calls/Second: {result['calls_per_second']:.2f}
"""
        
        # Calculate recommendations
        report += "\nRECOMMENDATIONS:\n----------------\n"
        
        # Find best performing scenario
        best_scenario = None
        best_score = 0
        
        for name, result in results.items():
            if "error" in result:
                continue
            
            # Score = success_rate * calls_per_second / avg_response_time
            score = (result['success_rate'] * result['calls_per_second']) / (result['avg_response_time_ms'] / 1000)
            
            if score > best_score:
                best_score = score
                best_scenario = (name, result)
        
        if best_scenario:
            name, result = best_scenario
            report += f"✅ Best Configuration: {name}\n"
            report += f"   - Concurrency: {result['concurrent']}\n"
            report += f"   - Page Size: {result['page_size']}\n"
            report += f"   - Expected Rate: {result['calls_per_second']:.2f} req/s\n"
        
        return report
    
    def _save_report(self, report: str):
        """Save report to file.
        
        Args:
            report: Report string
        """
        try:
            filename = f"rate_limit_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
            with open(filename, 'w') as f:
                f.write(report)
            
            logger.info("report_saved", filename=filename)
            print(report)  # Also print to console
            
        except Exception as e:
            logger.error("failed_to_save_report", error=str(e))


