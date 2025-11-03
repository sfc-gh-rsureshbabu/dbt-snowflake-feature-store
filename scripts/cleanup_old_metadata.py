"""
Cleanup script for feature views with old metadata format.

This script finds and optionally drops feature views that have the old
metadata format (entities as strings) which causes UI errors.

Usage:
    python scripts/cleanup_old_metadata.py --check  # Just check, don't drop
    python scripts/cleanup_old_metadata.py --drop   # Drop bad feature views
"""

import snowflake.connector
import os
import json
import argparse

def get_connection():
    """Create Snowflake connection from environment variables."""
    return snowflake.connector.connect(
        account=os.environ.get('SNOWFLAKE_ACCOUNT'),
        user=os.environ.get('SNOWFLAKE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        role=os.environ.get('SNOWFLAKE_ROLE'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
        database=os.environ.get('SNOWFLAKE_DATABASE', 'rsureshbabu'),
    )

def find_bad_feature_views(cursor):
    """Find all feature views with old metadata format."""
    print('\n🔍 Searching for feature views with old metadata...')
    print('=' * 80)
    
    # Find all objects with Feature Store metadata tag
    cursor.execute('''
        SELECT DISTINCT
            OBJECT_DATABASE,
            OBJECT_SCHEMA,
            OBJECT_NAME,
            DOMAIN
        FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
        WHERE TAG_NAME = 'SNOWML_FEATURE_VIEW_METADATA'
        ORDER BY OBJECT_DATABASE, OBJECT_SCHEMA, OBJECT_NAME
    ''')
    
    objects = cursor.fetchall()
    print(f'\nFound {len(objects)} feature views total')
    
    bad_objects = []
    
    for obj_db, obj_schema, obj_name, obj_domain in objects:
        try:
            # Query metadata for this object
            cursor.execute(f'''
                SELECT TAG_VALUE
                FROM TABLE(
                    INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
                        '{obj_db}.{obj_schema}.{obj_name}', 
                        'TABLE'
                    )
                )
                WHERE TAG_NAME = 'SNOWML_FEATURE_VIEW_METADATA'
                LIMIT 1
            ''')
            
            rows = cursor.fetchall()
            if rows:
                metadata = json.loads(rows[0][0])
                entities = metadata.get('entities', [])
                
                if entities and isinstance(entities, list):
                    first_entity = entities[0]
                    if isinstance(first_entity, str):
                        # OLD FORMAT - entities as strings
                        print(f'\n❌ BAD: {obj_db}.{obj_schema}.{obj_name}')
                        print(f'   Type: {obj_domain}')
                        print(f'   Metadata: {json.dumps(metadata)}')
                        bad_objects.append({
                            'database': obj_db,
                            'schema': obj_schema,
                            'name': obj_name,
                            'type': obj_domain
                        })
                    elif isinstance(first_entity, dict) and 'joinKeys' in first_entity:
                        # NEW FORMAT - entities as objects (OK)
                        pass
        except Exception as e:
            print(f'\n⚠️  Error checking {obj_db}.{obj_schema}.{obj_name}: {e}')
    
    return bad_objects

def drop_feature_views(cursor, bad_objects):
    """Drop feature views with old metadata."""
    print('\n🗑️  Dropping feature views with bad metadata...')
    print('=' * 80)
    
    for obj in bad_objects:
        full_name = f"{obj['database']}.{obj['schema']}.{obj['name']}"
        obj_type = 'DYNAMIC TABLE' if obj['type'] == 'TABLE' else 'VIEW'
        
        print(f'\nDropping {obj_type}: {full_name}')
        try:
            cursor.execute(f'DROP {obj_type} IF EXISTS {full_name}')
            print(f'  ✅ Dropped successfully')
        except Exception as e:
            print(f'  ⚠️  Error: {e}')

def main():
    parser = argparse.ArgumentParser(description='Cleanup feature views with old metadata')
    parser.add_argument('--check', action='store_true', help='Only check, do not drop')
    parser.add_argument('--drop', action='store_true', help='Drop bad feature views')
    args = parser.parse_args()
    
    if not args.check and not args.drop:
        parser.error('Please specify --check or --drop')
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        bad_objects = find_bad_feature_views(cursor)
        
        print('\n' + '=' * 80)
        print(f'\n📊 Summary: Found {len(bad_objects)} feature views with OLD metadata')
        
        if bad_objects:
            print('\nThese feature views will cause UI errors:')
            print('  "TypeError: Cannot read properties of undefined (reading \'joinKeys\')"')
            
            if args.drop:
                print('\n⚠️  Proceeding with DROP...')
                drop_feature_views(cursor, bad_objects)
                print('\n✅ Cleanup complete!')
                print('\n💡 Refresh the Snowsight UI to see the fix.')
            else:
                print('\n💡 Run with --drop to remove these feature views')
                print('   Or recreate them with the updated dbt package')
        else:
            print('\n✅ All feature views have correct metadata!')
    
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()

