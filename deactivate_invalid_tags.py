#!/usr/bin/env python3
"""
Script to deactivate invalid tags from the database
"""

import asyncio
import sys
import os
import re

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import get_plant_db
from queries.tag_queries import get_all_tags, update_tag
from utils.log import setup_logger

logger = setup_logger(__name__)

def validate_opcua_connection_string(connection_string: str) -> bool:
    """Validate OPC UA connection string format"""
    if not connection_string:
        return False
    pattern = r'^ns=\d+;[isgb]=[^;]+$'
    return bool(re.match(pattern, connection_string))

async def deactivate_invalid_tags(plant_id: str, dry_run: bool = True):
    """Deactivate invalid tags from the database
    
    Args:
        plant_id (str): The plant ID to clean up
        dry_run (bool): If True, only show what would be deactivated without actually doing it
    """
    try:
        logger.info(f"Starting deactivation of invalid tags for plant {plant_id} (dry_run={dry_run})")
        
        # Get database session for the plant
        async for session in get_plant_db(plant_id):
            # Get all tags
            tags = await get_all_tags(session, plant_id, limit=1000, offset=0)
            
            invalid_tags = []
            valid_tags = []
            
            for tag in tags:
                if not validate_opcua_connection_string(tag.connection_string):
                    invalid_tags.append(tag)
                else:
                    valid_tags.append(tag)
            
            logger.info(f"Found {len(tags)} total tags in plant {plant_id}")
            logger.info(f"  - Valid tags: {len(valid_tags)}")
            logger.info(f"  - Invalid tags: {len(invalid_tags)}")
            
            if invalid_tags:
                logger.info("Invalid tags found:")
                for tag in invalid_tags:
                    logger.info(f"  - ID: {tag.id}, Name: {tag.name}, Connection String: {tag.connection_string}, Active: {tag.is_active}")
                
                if not dry_run:
                    logger.info("Deactivating invalid tags...")
                    deactivated_count = 0
                    for tag in invalid_tags:
                        try:
                            # Deactivate the tag and add a prefix to indicate it's invalid
                            success = await update_tag(
                                session, 
                                tag.id, 
                                plant_id, 
                                is_active=False,
                                connection_string=f"INVALID_{tag.connection_string or tag.name}"
                            )
                            if success:
                                deactivated_count += 1
                                logger.info(f"  ✅ Deactivated tag {tag.id} ({tag.name})")
                            else:
                                logger.error(f"  ❌ Failed to deactivate tag {tag.id} ({tag.name})")
                        except Exception as e:
                            logger.error(f"  ❌ Error deactivating tag {tag.id} ({tag.name}): {e}")
                    
                    logger.info(f"Successfully deactivated {deactivated_count}/{len(invalid_tags)} invalid tags")
                else:
                    logger.info("DRY RUN: Would deactivate the above invalid tags")
            else:
                logger.info("No invalid tags found - database is clean!")
            
            break  # Only use the first session
            
    except Exception as e:
        logger.error(f"Error during deactivation: {e}")
        import traceback
        logger.error(traceback.format_exc())

async def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Deactivate invalid tags from the database")
    parser.add_argument("--plant-id", default="2", help="Plant ID to clean up (default: 2)")
    parser.add_argument("--execute", action="store_true", help="Actually deactivate invalid tags (default is dry run)")
    
    args = parser.parse_args()
    
    dry_run = not args.execute
    
    if dry_run:
        print("🔍 DRY RUN MODE - No tags will be deactivated")
        print("Use --execute to actually deactivate invalid tags")
        print()
    
    await deactivate_invalid_tags(args.plant_id, dry_run=dry_run)

if __name__ == "__main__":
    asyncio.run(main()) 