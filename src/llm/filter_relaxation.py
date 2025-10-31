"""Filter relaxation strategies for fallback when queries return no results."""
from typing import List, Tuple
import re

from ..utils.logger import get_logger

logger = get_logger(__name__)


class FilterRelaxation:
    """Strategies to relax SigNoz filter expressions when they return no results."""
    
    @staticmethod
    def relax_filter(filter_expression: str, attempt: int) -> Tuple[str, str]:
        """Relax a filter expression based on the attempt number.
        
        Args:
            filter_expression: The original filter expression
            attempt: The relaxation attempt number (0-based)
            
        Returns:
            Tuple of (relaxed_filter, relaxation_strategy)
        """
        if attempt == 0:
            return FilterRelaxation._remove_status_code_filter(filter_expression)
        elif attempt == 1:
            return FilterRelaxation._keep_service_and_severity_only(filter_expression)
        elif attempt == 2:
            return FilterRelaxation._keep_service_only(filter_expression)
        else:
            # Final fallback: empty filter (fetch all)
            return "", "all_signals_mode"
    
    @staticmethod
    def _remove_status_code_filter(filter_expression: str) -> Tuple[str, str]:
        """Remove HTTP status code filters while keeping other conditions.
        
        Args:
            filter_expression: Original filter expression
            
        Returns:
            Tuple of (relaxed_filter, strategy_name)
        """
        try:
            # Remove patterns like: http.status_code >= 500, http.status_code < 600, etc.
            relaxed = re.sub(
                r'\s*(AND|OR)?\s*http\.status_code\s*[><=!]+\s*\d+',
                '',
                filter_expression,
                flags=re.IGNORECASE
            )
            
            # Clean up leftover AND/OR at start or end
            relaxed = re.sub(r'^\s*(AND|OR)\s+', '', relaxed, flags=re.IGNORECASE)
            relaxed = re.sub(r'\s+(AND|OR)\s*$', '', relaxed, flags=re.IGNORECASE)
            
            # Clean up double AND/OR
            relaxed = re.sub(r'\s+(AND|OR)\s+(AND|OR)\s+', r' \1 ', relaxed, flags=re.IGNORECASE)
            
            relaxed = relaxed.strip()
            
            logger.info(
                "filter_relaxation_applied",
                strategy="remove_status_code",
                original=filter_expression,
                relaxed=relaxed
            )
            
            return relaxed, "remove_status_code"
            
        except Exception as e:
            logger.error("filter_relaxation_failed", error=str(e))
            return filter_expression, "no_relaxation"
    
    @staticmethod
    def _keep_service_and_severity_only(filter_expression: str) -> Tuple[str, str]:
        """Keep only service.name and severity_text filters.
        
        Args:
            filter_expression: Original filter expression
            
        Returns:
            Tuple of (relaxed_filter, strategy_name)
        """
        try:
            parts = []
            
            # Extract service.name filter
            service_match = re.search(
                r"service\.name\s*=\s*['\"]([^'\"]+)['\"]",
                filter_expression,
                flags=re.IGNORECASE
            )
            if service_match:
                parts.append(f"service.name = '{service_match.group(1)}'")
            
            # Extract severity_text filter
            severity_match = re.search(
                r"severity_text\s*=\s*['\"]([^'\"]+)['\"]",
                filter_expression,
                flags=re.IGNORECASE
            )
            if severity_match:
                parts.append(f"severity_text = '{severity_match.group(1)}'")
            
            relaxed = " AND ".join(parts) if parts else ""
            
            logger.info(
                "filter_relaxation_applied",
                strategy="service_and_severity_only",
                original=filter_expression,
                relaxed=relaxed
            )
            
            return relaxed, "service_and_severity_only"
            
        except Exception as e:
            logger.error("filter_relaxation_failed", error=str(e))
            return filter_expression, "no_relaxation"
    
    @staticmethod
    def _keep_service_only(filter_expression: str) -> Tuple[str, str]:
        """Keep only service.name filter.
        
        Args:
            filter_expression: Original filter expression
            
        Returns:
            Tuple of (relaxed_filter, strategy_name)
        """
        try:
            # Extract service.name filter
            service_match = re.search(
                r"service\.name\s*=\s*['\"]([^'\"]+)['\"]",
                filter_expression,
                flags=re.IGNORECASE
            )
            
            if service_match:
                relaxed = f"service.name = '{service_match.group(1)}'"
            else:
                relaxed = ""
            
            logger.info(
                "filter_relaxation_applied",
                strategy="service_only",
                original=filter_expression,
                relaxed=relaxed
            )
            
            return relaxed, "service_only"
            
        except Exception as e:
            logger.error("filter_relaxation_failed", error=str(e))
            return filter_expression, "no_relaxation"
    
    @staticmethod
    def should_fallback_to_all(log_count: int, max_attempts: int, current_attempt: int) -> bool:
        """Determine if we should fallback to ALL signals mode.
        
        Args:
            log_count: Number of logs found in current attempt
            max_attempts: Maximum number of relaxation attempts
            current_attempt: Current attempt number (0-based)
            
        Returns:
            True if should fallback to ALL mode
        """
        return log_count == 0 and current_attempt >= max_attempts


