"""Multi-signal fetcher for SigNoz (logs, metrics, traces)."""
import requests
from typing import Dict, Any, List, Optional, Literal
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from ..utils.logger import get_logger

logger = get_logger(__name__)

SignalType = Literal["logs", "metrics", "traces"]

# Constants for pagination and concurrency
DEFAULT_PAGE_SIZE = 1000  # SigNoz recommended page size
MAX_WORKERS = 3  # Number of concurrent signal fetches
MAX_RETRIES = 3  # Maximum retry attempts
RETRY_BACKOFF = 2  # Exponential backoff multiplier

# Rate limiting constants
DEFAULT_REQUESTS_PER_SECOND = 2.0  # Conservative default
MIN_REQUESTS_PER_SECOND = 0.5  # Minimum rate when throttled
MAX_REQUESTS_PER_SECOND = 5.0  # Maximum rate when performing well
RATE_LIMIT_ADJUSTMENT_FACTOR = 0.8  # Reduce rate by 20% on failure


class AdaptiveRateLimiter:
    """Adaptive rate limiter that adjusts based on API responses."""
    
    def __init__(self, initial_rps: float = DEFAULT_REQUESTS_PER_SECOND):
        """Initialize rate limiter.
        
        Args:
            initial_rps: Initial requests per second rate
        """
        self.current_rps = initial_rps
        self.min_rps = MIN_REQUESTS_PER_SECOND
        self.max_rps = MAX_REQUESTS_PER_SECOND
        self.last_request_time = 0.0
        
    def record_success(self):
        """Record successful API call and gradually increase rate."""
        self.current_rps = min(self.max_rps, self.current_rps * 1.05)
        
    def record_failure(self, is_rate_limit: bool = False):
        """Record failed API call and reduce rate.
        
        Args:
            is_rate_limit: Whether failure was due to rate limiting
        """
        if is_rate_limit:
            # Aggressive reduction for rate limit errors
            self.current_rps = max(self.min_rps, self.current_rps * RATE_LIMIT_ADJUSTMENT_FACTOR)
        else:
            # Moderate reduction for other errors
            self.current_rps = max(self.min_rps, self.current_rps * 0.95)
        
    def wait_if_needed(self):
        """Wait if necessary to respect rate limit."""
        if self.last_request_time > 0:
            delay = 1.0 / self.current_rps
            elapsed = time.time() - self.last_request_time
            
            if elapsed < delay:
                time.sleep(delay - elapsed)
        
        self.last_request_time = time.time()
    
    def get_current_rate(self) -> float:
        """Get current requests per second rate.
        
        Returns:
            Current rate in requests per second
        """
        return self.current_rps


class SigNozFetcher:
    """Unified fetcher for logs, metrics, and traces from SigNoz API."""
    
    def __init__(self, api_endpoint: str, api_key: str, timeout: int = 30, enable_rate_limiting: bool = True):
        """Initialize the multi-signal fetcher.
        
        Args:
            api_endpoint: SigNoz API endpoint URL
            api_key: SigNoz API key
            timeout: Request timeout in seconds (default: 30)
            enable_rate_limiting: Enable adaptive rate limiting (default: True)
        """
        self.api_endpoint = api_endpoint.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        
        self.headers = {
            "Content-Type": "application/json",
            "SIGNOZ-API-KEY": self.api_key
        }
        
        # Create a session with connection pooling for better performance
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Configure connection pooling
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=0  # We'll handle retries manually
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        # Initialize adaptive rate limiter
        self.rate_limiter = AdaptiveRateLimiter() if enable_rate_limiting else None
        
        logger.info(
            "signoz_fetcher_initialized",
            endpoint=self.api_endpoint,
            timeout=timeout,
            rate_limiting_enabled=enable_rate_limiting
        )
    
    def fetch_logs(
        self,
        start_ms: int,
        end_ms: int,
        filter_expression: str = "",
        limit: int = 1000,
        incident_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch logs from SigNoz (single page).
        
        Args:
            start_ms: Start time in epoch milliseconds
            end_ms: End time in epoch milliseconds
            filter_expression: SigNoz filter expression (empty for all logs)
            limit: Maximum logs to fetch (default: 1000)
            incident_id: Optional incident ID for logging context
            
        Returns:
            Raw SigNoz API response
        """
        query_payload = {
            "start": start_ms,
            "end": end_ms,
            "requestType": "raw",
            "variables": {},
            "compositeQuery": {
                "queries": [{
                    "type": "builder_query",
                    "spec": {
                        "name": "A",
                        "signal": "logs",
                        "filter": {"expression": filter_expression},
                        "order": [
                            {"key": {"name": "timestamp"}, "direction": "desc"},
                            {"key": {"name": "id"}, "direction": "desc"}
                        ],
                        "offset": 0,
                        "limit": limit
                    }
                }]
            }
        }
        
        return self._execute_query(
            query_payload=query_payload,
            signal="logs",
            incident_id=incident_id
        )
    
    def fetch_logs_paginated(
        self,
        start_ms: int,
        end_ms: int,
        filter_expression: str = "",
        page_size: int = DEFAULT_PAGE_SIZE,
        max_pages: Optional[int] = None,
        incident_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch all logs from SigNoz using pagination.
        
        Args:
            start_ms: Start time in epoch milliseconds
            end_ms: End time in epoch milliseconds
            filter_expression: SigNoz filter expression (empty for all logs)
            page_size: Number of logs per page (default: 1000)
            max_pages: Maximum pages to fetch (None = fetch all)
            incident_id: Optional incident ID for logging context
            
        Returns:
            Aggregated SigNoz API response with all logs
        """
        logger.info(
            "starting_paginated_logs_fetch",
            incident_id=incident_id,
            page_size=page_size,
            max_pages=max_pages or "unlimited"
        )
        
        all_rows = []
        offset = 0
        page = 1
        total_fetched = 0
        
        while True:
            # Check if we've reached max pages
            if max_pages and page > max_pages:
                logger.info(
                    "reached_max_pages_for_logs",
                    incident_id=incident_id,
                    pages_fetched=page - 1,
                    total_rows=total_fetched
                )
                break
            
            # Fetch current page
            query_payload = {
                "start": start_ms,
                "end": end_ms,
                "requestType": "raw",
                "variables": {},
                "compositeQuery": {
                    "queries": [{
                        "type": "builder_query",
                        "spec": {
                            "name": "A",
                            "signal": "logs",
                            "filter": {"expression": filter_expression},
                            "order": [
                                {"key": {"name": "timestamp"}, "direction": "desc"},
                                {"key": {"name": "id"}, "direction": "desc"}
                            ],
                            "offset": offset,
                            "limit": page_size
                        }
                    }]
                }
            }
            
            try:
                response = self._execute_query(
                    query_payload=query_payload,
                    signal="logs",
                    incident_id=incident_id
                )
                
                # Extract rows from response
                results = response.get('data', {}).get('data', {}).get('results', [])
                if not results or len(results) == 0:
                    break
                
                rows = results[0].get('rows', [])
                if not rows or len(rows) == 0:
                    logger.info(
                        "no_more_logs_to_fetch",
                        incident_id=incident_id,
                        pages_fetched=page,
                        total_rows=total_fetched
                    )
                    break
                
                # Add rows to aggregated result
                all_rows.extend(rows)
                total_fetched += len(rows)
                
                logger.info(
                    "logs_page_fetched",
                    incident_id=incident_id,
                    page=page,
                    rows_in_page=len(rows),
                    total_fetched=total_fetched
                )
                
                # If we got fewer rows than page_size, we've reached the end
                if len(rows) < page_size:
                    logger.info(
                        "reached_end_of_logs",
                        incident_id=incident_id,
                        pages_fetched=page,
                        total_rows=total_fetched
                    )
                    break
                
                # Move to next page
                offset += page_size
                page += 1
                
            except Exception as e:
                logger.error(
                    "error_fetching_logs_page",
                    incident_id=incident_id,
                    page=page,
                    error=str(e)
                )
                # Return what we have so far
                break
        
        # Construct aggregated response in SigNoz format
        aggregated_response = {
            "status": "success",
            "data": {
                "type": "raw",
                "data": {
                    "results": [{
                        "queryName": "A",
                        "rows": all_rows
                    }]
                }
            }
        }
        
        logger.info(
            "paginated_logs_fetch_complete",
            incident_id=incident_id,
            total_pages=page,
            total_rows=total_fetched
        )
        
        return aggregated_response
    
    def fetch_metrics(
        self,
        start_ms: int,
        end_ms: int,
        metric_name: str,
        aggregation: str = "avg",
        group_by: Optional[List[str]] = None,
        filter_expression: str = "",
        step_seconds: int = 60,
        incident_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch metrics from SigNoz using v5 API format.
        
        Args:
            start_ms: Start time in epoch milliseconds
            end_ms: End time in epoch milliseconds
            metric_name: Name of the metric to fetch
            aggregation: Aggregation function (avg, sum, rate, p50, p95, p99)
            group_by: List of attributes to group by
            filter_expression: SigNoz filter expression
            step_seconds: Time step for aggregation in seconds (default: 60)
            incident_id: Optional incident ID for logging context
            
        Returns:
            Raw SigNoz API response
        """
        group_by = group_by or []
        
        # Build groupBy array with proper structure
        group_by_list = [{"name": gb} for gb in group_by] if group_by else []
        
        query_payload = {
            "start": start_ms,
            "end": end_ms,
            "requestType": "time_series",
            "compositeQuery": {
                "queries": [{
                    "type": "builder_query",
                    "spec": {
                        "name": "A",
                        "signal": "metrics",
                        "stepInterval": step_seconds,
                        "aggregations": [{
                            "metricName": metric_name,
                            "timeAggregation": aggregation,
                            "spaceAggregation": "sum"
                        }],
                        "filter": {"expression": filter_expression} if filter_expression else {},
                        "groupBy": group_by_list,
                        "disabled": False
                    }
                }]
            }
        }
        
        return self._execute_query(
            query_payload=query_payload,
            signal="metrics",
            incident_id=incident_id
        )
    
    def fetch_traces(
        self,
        start_ms: int,
        end_ms: int,
        filter_expression: str = "",
        limit: int = 1000,
        order_by: str = "durationNano",
        incident_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch traces from SigNoz (single page).
        
        Args:
            start_ms: Start time in epoch milliseconds
            end_ms: End time in epoch milliseconds
            filter_expression: SigNoz filter expression (empty for all traces)
            limit: Maximum traces to fetch (default: 1000)
            order_by: Order by field (durationNano, timestamp)
            incident_id: Optional incident ID for logging context
            
        Returns:
            Raw SigNoz API response
        """
        query_payload = {
            "start": start_ms,
            "end": end_ms,
            "requestType": "raw",
            "variables": {},
            "compositeQuery": {
                "queries": [{
                    "type": "builder_query",
                    "spec": {
                        "name": "A",
                        "signal": "traces",
                        "filter": {"expression": filter_expression},
                        "order": [
                            {"key": {"name": order_by}, "direction": "desc"}
                        ],
                        "offset": 0,
                        "limit": limit
                    }
                }]
            }
        }
        
        return self._execute_query(
            query_payload=query_payload,
            signal="traces",
            incident_id=incident_id
        )
    
    def fetch_traces_paginated(
        self,
        start_ms: int,
        end_ms: int,
        filter_expression: str = "",
        page_size: int = DEFAULT_PAGE_SIZE,
        max_pages: Optional[int] = None,
        order_by: str = "durationNano",
        incident_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch all traces from SigNoz using pagination.
        
        Args:
            start_ms: Start time in epoch milliseconds
            end_ms: End time in epoch milliseconds
            filter_expression: SigNoz filter expression (empty for all traces)
            page_size: Number of traces per page (default: 1000)
            max_pages: Maximum pages to fetch (None = fetch all)
            order_by: Order by field (durationNano, timestamp)
            incident_id: Optional incident ID for logging context
            
        Returns:
            Aggregated SigNoz API response with all traces
        """
        logger.info(
            "starting_paginated_traces_fetch",
            incident_id=incident_id,
            page_size=page_size,
            max_pages=max_pages or "unlimited"
        )
        
        all_rows = []
        offset = 0
        page = 1
        total_fetched = 0
        
        while True:
            # Check if we've reached max pages
            if max_pages and page > max_pages:
                logger.info(
                    "reached_max_pages_for_traces",
                    incident_id=incident_id,
                    pages_fetched=page - 1,
                    total_rows=total_fetched
                )
                break
            
            # Fetch current page
            query_payload = {
                "start": start_ms,
                "end": end_ms,
                "requestType": "raw",
                "variables": {},
                "compositeQuery": {
                    "queries": [{
                        "type": "builder_query",
                        "spec": {
                            "name": "A",
                            "signal": "traces",
                            "filter": {"expression": filter_expression},
                            "order": [
                                {"key": {"name": order_by}, "direction": "desc"}
                            ],
                            "offset": offset,
                            "limit": page_size
                        }
                    }]
                }
            }
            
            try:
                response = self._execute_query(
                    query_payload=query_payload,
                    signal="traces",
                    incident_id=incident_id
                )
                
                # Extract rows from response
                results = response.get('data', {}).get('data', {}).get('results', [])
                if not results or len(results) == 0:
                    break
                
                rows = results[0].get('rows', [])
                if not rows or len(rows) == 0:
                    logger.info(
                        "no_more_traces_to_fetch",
                        incident_id=incident_id,
                        pages_fetched=page,
                        total_rows=total_fetched
                    )
                    break
                
                # Add rows to aggregated result
                all_rows.extend(rows)
                total_fetched += len(rows)
                
                logger.info(
                    "traces_page_fetched",
                    incident_id=incident_id,
                    page=page,
                    rows_in_page=len(rows),
                    total_fetched=total_fetched
                )
                
                # If we got fewer rows than page_size, we've reached the end
                if len(rows) < page_size:
                    logger.info(
                        "reached_end_of_traces",
                        incident_id=incident_id,
                        pages_fetched=page,
                        total_rows=total_fetched
                    )
                    break
                
                # Move to next page
                offset += page_size
                page += 1
                
            except Exception as e:
                logger.error(
                    "error_fetching_traces_page",
                    incident_id=incident_id,
                    page=page,
                    error=str(e)
                )
                # Return what we have so far
                break
        
        # Construct aggregated response in SigNoz format
        aggregated_response = {
            "status": "success",
            "data": {
                "type": "raw",
                "data": {
                    "results": [{
                        "queryName": "A",
                        "rows": all_rows
                    }]
                }
            }
        }
        
        logger.info(
            "paginated_traces_fetch_complete",
            incident_id=incident_id,
            total_pages=page,
            total_rows=total_fetched
        )
        
        return aggregated_response
    
    def fetch_all_signals_concurrent(
        self,
        start_ms: int,
        end_ms: int,
        logs_filter: str = "",
        traces_filter: str = "",
        metric_name: str = "signoz_calls_total",
        metric_aggregation: str = "rate",
        metric_group_by: Optional[List[str]] = None,
        use_pagination: bool = True,
        max_pages: Optional[int] = None,
        incident_id: Optional[str] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Fetch logs, metrics, and traces concurrently for maximum performance.
        
        Args:
            start_ms: Start time in epoch milliseconds
            end_ms: End time in epoch milliseconds
            logs_filter: Filter expression for logs
            traces_filter: Filter expression for traces
            metric_name: Metric name to fetch
            metric_aggregation: Metric aggregation function
            metric_group_by: Metric group by attributes
            use_pagination: Whether to use pagination (fetch all data)
            max_pages: Maximum pages to fetch per signal (None = unlimited)
            incident_id: Optional incident ID for logging context
            
        Returns:
            Dictionary with keys: logs, metrics, traces
        """
        logger.info(
            "starting_concurrent_signal_fetch",
            incident_id=incident_id,
            use_pagination=use_pagination,
            max_pages=max_pages or "unlimited"
        )
        
        results = {}
        start_time = time.time()
        
        # Define fetch functions for each signal
        def fetch_logs_task():
            try:
                if use_pagination:
                    return self.fetch_logs_paginated(
                        start_ms=start_ms,
                        end_ms=end_ms,
                        filter_expression=logs_filter,
                        max_pages=max_pages,
                        incident_id=incident_id
                    )
                else:
                    return self.fetch_logs(
                        start_ms=start_ms,
                        end_ms=end_ms,
                        filter_expression=logs_filter,
                        limit=1000,
                        incident_id=incident_id
                    )
            except Exception as e:
                logger.error("failed_to_fetch_logs", incident_id=incident_id, error=str(e))
                return {"error": str(e)}
        
        def fetch_traces_task():
            try:
                if use_pagination:
                    return self.fetch_traces_paginated(
                        start_ms=start_ms,
                        end_ms=end_ms,
                        filter_expression=traces_filter,
                        max_pages=max_pages,
                        incident_id=incident_id
                    )
                else:
                    return self.fetch_traces(
                        start_ms=start_ms,
                        end_ms=end_ms,
                        filter_expression=traces_filter,
                        limit=1000,
                        incident_id=incident_id
                    )
            except Exception as e:
                logger.error("failed_to_fetch_traces", incident_id=incident_id, error=str(e))
                return {"error": str(e)}
        
        def fetch_metrics_task():
            try:
                return self.fetch_metrics(
                    start_ms=start_ms,
                    end_ms=end_ms,
                    metric_name=metric_name,
                    aggregation=metric_aggregation,
                    group_by=metric_group_by or [],
                    filter_expression="",  # Metrics don't support complex filters
                    incident_id=incident_id
                )
            except Exception as e:
                logger.error("failed_to_fetch_metrics", incident_id=incident_id, error=str(e))
                return {"error": str(e)}
        
        # Execute all fetches concurrently using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            future_logs = executor.submit(fetch_logs_task)
            future_traces = executor.submit(fetch_traces_task)
            future_metrics = executor.submit(fetch_metrics_task)
            
            # Wait for all to complete
            results["logs"] = future_logs.result()
            results["traces"] = future_traces.result()
            results["metrics"] = future_metrics.result()
        
        elapsed_time = time.time() - start_time
        
        logger.info(
            "concurrent_signal_fetch_complete",
            incident_id=incident_id,
            elapsed_seconds=round(elapsed_time, 2),
            signals_fetched=list(results.keys())
        )
        
        return results
    
    def _execute_query(
        self,
        query_payload: Dict[str, Any],
        signal: SignalType,
        incident_id: Optional[str] = None,
        retry_count: int = 0
    ) -> Dict[str, Any]:
        """Execute a query against SigNoz API with retry logic.
        
        Args:
            query_payload: The query payload
            signal: Signal type (logs, metrics, traces)
            incident_id: Optional incident ID for logging context
            retry_count: Current retry attempt (internal use)
            
        Returns:
            Raw API response
            
        Raises:
            Exception: If API call fails after all retries
        """
        url = f"{self.api_endpoint}/api/v5/query_range"
        
        logger.info(
            "executing_signoz_query",
            incident_id=incident_id,
            signal=signal,
            url=url,
            time_range=f"{query_payload.get('start')} to {query_payload.get('end')}",
            retry_attempt=retry_count
        )
        
        try:
            # Apply rate limiting if enabled
            if self.rate_limiter:
                self.rate_limiter.wait_if_needed()
            
            # Use session for connection pooling
            response = self.session.post(
                url=url,
                json=query_payload,
                timeout=self.timeout
            )
            
            logger.info(
                "signoz_api_response",
                incident_id=incident_id,
                signal=signal,
                status_code=response.status_code,
                response_time_ms=response.elapsed.total_seconds() * 1000,
                current_rate_limit=self.rate_limiter.get_current_rate() if self.rate_limiter else None
            )
            
            response.raise_for_status()
            response_data = response.json()
            
            # Record success for rate limiter
            if self.rate_limiter:
                self.rate_limiter.record_success()
            
            # Extract count
            count = self._extract_count(response_data, signal)
            
            logger.info(
                f"{signal}_fetched_successfully",
                incident_id=incident_id,
                count=count
            )
            
            return response_data
            
        except requests.exceptions.Timeout as e:
            # Record failure for rate limiter
            if self.rate_limiter:
                self.rate_limiter.record_failure(is_rate_limit=False)
            
            logger.error(
                "signoz_api_timeout",
                incident_id=incident_id,
                signal=signal,
                timeout=self.timeout,
                retry_attempt=retry_count
            )
            
            # Retry on timeout
            if retry_count < MAX_RETRIES:
                wait_time = RETRY_BACKOFF ** retry_count
                logger.info(
                    "retrying_after_timeout",
                    incident_id=incident_id,
                    signal=signal,
                    wait_seconds=wait_time,
                    retry_attempt=retry_count + 1
                )
                time.sleep(wait_time)
                return self._execute_query(query_payload, signal, incident_id, retry_count + 1)
            
            raise Exception(f"SigNoz API request timed out after {self.timeout}s for {signal}")
            
        except requests.exceptions.HTTPError as e:
            # Check if it's a rate limit error (429)
            is_rate_limit = e.response.status_code == 429
            
            # Record failure for rate limiter
            if self.rate_limiter:
                self.rate_limiter.record_failure(is_rate_limit=is_rate_limit)
            
            logger.error(
                "signoz_api_http_error",
                incident_id=incident_id,
                signal=signal,
                status_code=e.response.status_code,
                response_body=e.response.text[:500],
                retry_attempt=retry_count,
                is_rate_limit=is_rate_limit
            )
            
            # Retry on 5xx errors or rate limiting
            if (e.response.status_code >= 500 or is_rate_limit) and retry_count < MAX_RETRIES:
                wait_time = RETRY_BACKOFF ** retry_count
                if is_rate_limit:
                    wait_time *= 2  # Extra wait for rate limiting
                
                logger.info(
                    "retrying_after_server_error",
                    incident_id=incident_id,
                    signal=signal,
                    status_code=e.response.status_code,
                    wait_seconds=wait_time,
                    retry_attempt=retry_count + 1
                )
                time.sleep(wait_time)
                return self._execute_query(query_payload, signal, incident_id, retry_count + 1)
            
            raise Exception(f"SigNoz API error ({e.response.status_code}) for {signal}: {e.response.text}")
            
        except requests.exceptions.RequestException as e:
            # Record failure for rate limiter
            if self.rate_limiter:
                self.rate_limiter.record_failure(is_rate_limit=False)
            
            logger.error(
                "signoz_api_request_failed",
                incident_id=incident_id,
                signal=signal,
                error=str(e),
                retry_attempt=retry_count
            )
            
            # Retry on connection errors
            if retry_count < MAX_RETRIES:
                wait_time = RETRY_BACKOFF ** retry_count
                logger.info(
                    "retrying_after_connection_error",
                    incident_id=incident_id,
                    signal=signal,
                    wait_seconds=wait_time,
                    retry_attempt=retry_count + 1
                )
                time.sleep(wait_time)
                return self._execute_query(query_payload, signal, incident_id, retry_count + 1)
            
            raise Exception(f"Failed to fetch {signal} from SigNoz: {str(e)}")
    
    def _extract_count(self, response_data: Dict[str, Any], signal: SignalType) -> int:
        """Extract count from SigNoz response.
        
        Args:
            response_data: The SigNoz API response
            signal: Signal type
            
        Returns:
            Number of items fetched
        """
        try:
            results = response_data.get('data', {}).get('data', {}).get('results', [])
            if results and len(results) > 0:
                if signal in ["logs", "traces"]:
                    rows = results[0].get('rows', [])
                    return len(rows) if rows else 0
                elif signal == "metrics":
                    series = results[0].get('series', [])
                    return len(series) if series else 0
            return 0
        except (KeyError, IndexError, TypeError):
            return 0

