"""Structured logging configuration."""
import logging
import structlog
from typing import Any
from .config import get_settings


def human_readable_renderer(logger, method_name, event_dict):
    """Render log messages in a human-readable format."""
    # Extract key fields
    timestamp = event_dict.pop("timestamp", "")
    level = event_dict.pop("level", "info").upper()
    event = event_dict.pop("event", "")
    
    # Format level with colors (for terminals that support it)
    level_colors = {
        "INFO": "\033[36m",     # Cyan
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",    # Red
        "DEBUG": "\033[90m",    # Gray
    }
    reset_color = "\033[0m"
    colored_level = f"{level_colors.get(level, '')}{level:8}{reset_color}"
    
    # Build the message
    message_parts = []
    
    # Add timestamp (just time, not full ISO)
    if timestamp:
        time_part = timestamp.split("T")[1][:8] if "T" in timestamp else timestamp[:8]
        message_parts.append(f"[{time_part}]")
    
    # Add level
    message_parts.append(colored_level)
    
    # Add event message
    if event:
        message_parts.append(f"| {event}")
    
    # Add remaining context (but filter out noise)
    important_keys = ["incident_id", "service", "status", "error", "count", "total", "duration_ms"]
    context_parts = []
    for key, value in event_dict.items():
        if key in important_keys or (key not in ["logger", "exception"] and value):
            if isinstance(value, (int, float)):
                context_parts.append(f"{key}={value}")
            elif isinstance(value, str) and len(value) < 100:
                context_parts.append(f"{key}={value}")
    
    if context_parts:
        message_parts.append(f"({', '.join(context_parts)})")
    
    return " ".join(message_parts)


def setup_logging() -> None:
    """Configure structured logging for the application."""
    settings = get_settings()
    
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, settings.log_level.upper()),
    )
    
    # Choose renderer based on LOG_FORMAT setting
    if settings.log_format.lower() == "human":
        renderer = human_readable_renderer
    else:
        renderer = structlog.processors.JSONRenderer()
    
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            renderer
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> Any:
    """Get a structured logger instance."""
    return structlog.get_logger(name)


def print_banner(title: str, items: dict = None, width: int = 80) -> None:
    """
    Print a formatted banner for important information.
    
    Args:
        title: Banner title
        items: Dictionary of key-value pairs to display
        width: Banner width
    """
    settings = get_settings()
    
    # Only print banners in human format
    if settings.log_format.lower() != "human":
        return
    
    border = "=" * width
    print(f"\n{border}")
    print(f"  {title}")
    print(border)
    
    if items:
        for key, value in items.items():
            # Format the key nicely
            formatted_key = key.replace("_", " ").title()
            print(f"{formatted_key:.<30} {value}")
    
    print(f"{border}\n")
