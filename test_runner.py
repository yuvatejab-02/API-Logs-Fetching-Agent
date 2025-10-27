#!/usr/bin/env python3
"""Test runner for Components 1 & 2: Query Generation + Log Fetching."""
import sys
import json
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.utils.logger import setup_logging, get_logger
from src.utils.config import get_settings
from src.llm.query_generator import QueryGenerator
from src.signoz.api_client import SigNozClient
from src.signoz.log_transformer import LogTransformer
from src.storage.local_storage import LocalStorage

setup_logging()
logger = get_logger(__name__)


def print_section(title: str):
    """Print a formatted section header."""
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}\n")


def main():
    """Run complete test: Query generation -> Log fetching -> Transformation -> Storage."""
    
    print_section("🚀 INCIDENT LOG ANALYZER - COMPONENT TEST")
    
    # ============================================================
    # 1. Configuration Check
    # ============================================================
    print("Step 1: Loading configuration...")
    try:
        settings = get_settings()
        logger.info(
            "configuration_loaded",
            region=settings.aws_region,
            model_id=settings.bedrock_model_id
        )
        print("✅ Configuration loaded successfully")
    except Exception as e:
        logger.error("configuration_failed", error=str(e))
        print(f"❌ Configuration Error: {str(e)}")
        return
    
    # ============================================================
    # 2. Initialize Components
    # ============================================================
    print("\nStep 2: Initializing components...")
    try:
        query_generator = QueryGenerator()
        signoz_client = SigNozClient()
        log_transformer = LogTransformer()
        local_storage = LocalStorage(base_dir="output")
        print("✅ All components initialized")
    except Exception as e:
        logger.error("initialization_failed", error=str(e))
        print(f"❌ Initialization Error: {str(e)}")
        return
    
    # ============================================================
    # 3. Test SigNoz Connection
    # ============================================================
    print("\nStep 3: Testing SigNoz connection...")
    if signoz_client.test_connection():
        print("✅ SigNoz connection successful")
    else:
        print("⚠️  SigNoz connection test failed (will continue anyway)")
    
    # ============================================================
    # 4. Test Incident Payload
    # ============================================================
    test_payload = {
        "alert_id": "ALT_xyz789",
        "severity": "critical",
        "message": "Database connection pool exhausted",
        "application": "user-service",
        "error_pattern": "connection refused"
    }
    
    incident_id = test_payload.get("incident_id", "unknown")
    
    print_section(f"📋 INCIDENT: {incident_id}")
    print(f"Title: {test_payload.get('title')}")
    print(f"Service: {test_payload.get('service', {}).get('name')}")
    
    # ============================================================
    # 5. Generate SigNoz Query using LLM
    # ============================================================
    print_section("🤖 STEP 1: Generating SigNoz Query (AWS Bedrock)")
    try:
        result = query_generator.generate_signoz_query(
            test_payload, 
            lookback_hours=1
        )
        
        query_payload = result['query']
        metadata = result['metadata']
        
        print("✅ Query generated successfully!")
        print(f"\n📊 Filter Expression:")
        print(f"   {metadata['filter_expression']}")
        print(f"\n💡 Reasoning:")
        print(f"   {metadata['reasoning']}")
        print(f"\n🔑 Key Attributes:")
        print(f"   {', '.join(metadata['key_attributes'])}")
        
    except Exception as e:
        logger.error("query_generation_failed", error=str(e), exc_info=True)
        print(f"❌ Query generation failed: {str(e)}")
        return
    
    # ============================================================
    # 6. Fetch Logs from SigNoz
    # ============================================================
    print_section("📡 STEP 2: Fetching Logs from SigNoz")
    try:
        raw_response = signoz_client.fetch_logs(
            query_payload=query_payload,
            incident_id=incident_id
        )
        
        # Save raw response for debugging
        raw_path = local_storage.save_raw_response(raw_response, incident_id)
        
        print(f"✅ Logs fetched successfully!")
        print(f"   Raw response saved to: {raw_path}")
        
    except Exception as e:
        logger.error("log_fetch_failed", error=str(e), exc_info=True)
        print(f"❌ Log fetch failed: {str(e)}")
        return
    
    # ============================================================
    # 7. Transform Logs to Expected Format
    # ============================================================
    print_section("🔄 STEP 3: Transforming Logs")
    try:
        transformed_logs = log_transformer.transform_logs(raw_response)
        
        print(f"✅ Logs transformed successfully!")
        print(f"   Total logs: {len(transformed_logs)}")
        
        # Show sample transformed log
        if transformed_logs:
            print(f"\n📝 Sample Transformed Log:")
            print(json.dumps(transformed_logs[0], indent=2))
        else:
            print("\n⚠️  No logs found matching the filter criteria")
        
    except Exception as e:
        logger.error("log_transformation_failed", error=str(e), exc_info=True)
        print(f"❌ Log transformation failed: {str(e)}")
        return
    
    # ============================================================
    # 8. Save Transformed Logs Locally
    # ============================================================
    print_section("💾 STEP 4: Saving Transformed Logs")
    try:
        saved_path = local_storage.save_logs(
            logs=transformed_logs,
            incident_id=incident_id,
            metadata={
                "filter_expression": metadata['filter_expression'],
                "reasoning": metadata['reasoning'],
                "key_attributes": metadata['key_attributes']
            }
        )
        
        print(f"✅ Logs saved successfully!")
        print(f"   File: {saved_path}")
        
    except Exception as e:
        logger.error("log_save_failed", error=str(e), exc_info=True)
        print(f"❌ Failed to save logs: {str(e)}")
        return
    
    # ============================================================
    # 9. Summary
    # ============================================================
    print_section("✅ TEST COMPLETED SUCCESSFULLY")
    print(f"Incident ID: {incident_id}")
    print(f"Logs fetched: {len(transformed_logs)}")
    print(f"Filter used: {metadata['filter_expression']}")
    print(f"Output directory: output/{incident_id}/")
    print("\n" + "="*80 + "\n")


if __name__ == "__main__":
    main()
