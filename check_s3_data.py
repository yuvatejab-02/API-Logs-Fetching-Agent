"""Check what data is stored in S3 (LocalStack)."""
import boto3
import json
import gzip

# Configuration
LOCALSTACK_ENDPOINT = "http://localhost:14566"
BUCKET_NAME = "incident-logs-test"
REGION = "us-east-1"

# Initialize S3 client
s3 = boto3.client(
    's3',
    region_name=REGION,
    endpoint_url=LOCALSTACK_ENDPOINT
)

try:
    # List all objects in bucket
    response = s3.list_objects_v2(Bucket=BUCKET_NAME)
    
    if 'Contents' not in response:
        print(f"\n❌ No objects found in bucket '{BUCKET_NAME}'")
        print("\nBucket exists but is empty. This could mean:")
        print("1. No messages have been processed yet")
        print("2. The analyzer container exited before processing")
        print("3. Data was uploaded but then deleted")
    else:
        objects = response['Contents']
        print(f"\n✅ Found {len(objects)} objects in bucket '{BUCKET_NAME}':\n")
        print("="*100)
        
        # Group by signal type
        logs = []
        traces = []
        metrics = []
        manifests = []
        edal = []
        
        for obj in objects:
            key = obj['Key']
            size_kb = obj['Size'] / 1024
            
            if '/logs/' in key and key.endswith('.json.gz'):
                logs.append((key, size_kb))
            elif '/traces/' in key and key.endswith('.json.gz'):
                traces.append((key, size_kb))
            elif '/metrics/' in key and key.endswith('.json.gz'):
                metrics.append((key, size_kb))
            elif 'manifest.json' in key:
                manifests.append((key, size_kb))
            elif 'edal' in key:
                edal.append((key, size_kb))
        
        # Print summary
        print(f"\n📊 SUMMARY:")
        print(f"   Logs files: {len(logs)}")
        print(f"   Traces files: {len(traces)}")
        print(f"   Metrics files: {len(metrics)}")
        print(f"   Manifest files: {len(manifests)}")
        print(f"   EDAL descriptors: {len(edal)}")
        
        # Print details
        if logs:
            print(f"\n📝 LOGS ({len(logs)} files):")
            for key, size in logs:
                print(f"   - {key} ({size:.2f} KB)")
        
        if traces:
            print(f"\n🔍 TRACES ({len(traces)} files):")
            for key, size in traces:
                print(f"   - {key} ({size:.2f} KB)")
        
        if metrics:
            print(f"\n📈 METRICS ({len(metrics)} files):")
            for key, size in metrics:
                print(f"   - {key} ({size:.2f} KB)")
        
        if manifests:
            print(f"\n📋 MANIFESTS ({len(manifests)} files):")
            for key, size in manifests:
                print(f"   - {key} ({size:.2f} KB)")
        
        if edal:
            print(f"\n🔧 EDAL DESCRIPTORS ({len(edal)} files):")
            for key, size in edal:
                print(f"   - {key} ({size:.2f} KB)")
        
        print("\n" + "="*100)
        
        # Offer to download and inspect a file
        if logs:
            print(f"\n💡 To inspect the actual data, you can download a file:")
            print(f"   Key: {logs[0][0]}")
            
            choice = input("\nDownload and inspect this file? (y/n): ")
            if choice.lower() == 'y':
                key = logs[0][0]
                print(f"\nDownloading {key}...")
                
                response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
                compressed_data = response['Body'].read()
                
                # Decompress
                data = gzip.decompress(compressed_data)
                json_data = json.loads(data)
                
                print(f"\n📄 File contents (first 500 chars):")
                print(json.dumps(json_data, indent=2)[:500])
                print("...")

except Exception as e:
    print(f"\n❌ Error checking S3: {str(e)}")
    print("\nPossible reasons:")
    print("1. LocalStack is not running (start with: docker-compose -f docker-compose.test.yml up)")
    print("2. Bucket doesn't exist yet (send a message to trigger processing)")
    print("3. Network connectivity issue")


