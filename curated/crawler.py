import boto3
import json
from datetime import datetime

class MetadataScanner:
    def __init__(self, database_name):
        self.glue_client = boto3.client('glue')
        self.s3_client = boto3.client('s3')
        self.database_name = database_name
        
    def create_crawler(self, crawler_name, s3_path, table_prefix):
        """Create Glue Crawler for metadata extraction"""
        try:
            self.glue_client.create_crawler(
                Name=crawler_name,
                Role='AWSGlueServiceRole-DataLake',
                DatabaseName=self.database_name,
                Targets={
                    'S3Targets': [
                        {
                            'Path': s3_path,
                            'Exclusions': []
                        }
                    ]
                },
                TablePrefix=table_prefix,
                SchemaChangePolicy={
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'LOG'
                },
                Configuration=json.dumps({
                    'Version': 1.0,
                    'CrawlerOutput': {
                        'Partitions': {'AddOrUpdateBehavior': 'InheritFromTable'}
                    }
                })
            )
            print(f"Crawler {crawler_name} created successfully")
        except self.glue_client.exceptions.AlreadyExistsException:
            print(f"Crawler {crawler_name} already exists")
    
    def run_crawler(self, crawler_name):
        """Execute crawler to scan metadata"""
        try:
            self.glue_client.start_crawler(Name=crawler_name)
            print(f"Crawler {crawler_name} started")
        except Exception as e:
            print(f"Error starting crawler: {e}")
    
    def extract_custom_metadata(self, s3_bucket, s3_prefix):
        """Extract custom metadata beyond schema"""
        metadata = {
            'scan_timestamp': datetime.now().isoformat(),
            'files': [],
            'total_size_bytes': 0,
            'record_count_estimate': 0
        }
        
        # List objects in S3 prefix
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)
        
        for page in pages:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                if obj['Key'].endswith('.parquet'):
                    file_info = {
                        'key': obj['Key'],
                        'size_bytes': obj['Size'],
                        'last_modified': obj['LastModified'].isoformat()
                    }
                    metadata['files'].append(file_info)
                    metadata['total_size_bytes'] += obj['Size']
        
        metadata['file_count'] = len(metadata['files'])
        
        # Store metadata in S3
        metadata_key = f"{s3_prefix}/_metadata/scan_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        self.s3_client.put_object(
            Bucket=s3_bucket,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2),
            ContentType='application/json'
        )
        
        return metadata

# Usage
scanner = MetadataScanner(database_name='data_lake_inbound')

# Create crawlers for each topic
scanner.create_crawler(
    crawler_name='orders_inbound_crawler',
    s3_path='s3://vrams-data-lake-inbound/topic_orders/',
    table_prefix='inbound_'
)

scanner.create_crawler(
    crawler_name='customers_inbound_crawler',
    s3_path='s3://vrams-data-lake-inbound/topic_customers/',
    table_prefix='inbound_'
)

# Run crawlers
scanner.run_crawler('orders_inbound_crawler')
scanner.run_crawler('customers_inbound_crawler')

# Extract custom metadata
metadata = scanner.extract_custom_metadata(
    s3_bucket='vrams-data-lake-inbound',
    s3_prefix='topic_orders/date=2025-10-19'
)
print(f"Scanned {metadata['file_count']} files, total size: {metadata['total_size_bytes']} bytes")