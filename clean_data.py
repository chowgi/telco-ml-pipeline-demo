#!/usr/bin/env python3
"""
Telco ODS Data Cleaner
Removes data from collections without dropping collections or indexes
"""

import os
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class TelcoODSCleaner:
    def __init__(self):
        """Initialize the data cleaner with MongoDB connection"""
        self.mongodb_uri = os.getenv('MONGODB_URI')
        self.db_name = 'ods_demo_db'
        
        # Initialize MongoDB connection
        try:
            self.client = MongoClient(self.mongodb_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')  # Test connection
            self.db = self.client[self.db_name]
            print(f"✅ Connected to MongoDB Atlas: {self.db_name}")
        except ServerSelectionTimeoutError:
            print("❌ Failed to connect to MongoDB Atlas. Please check your URI.")
            raise

    def clean_collections(self):
        """Remove all data from Telco ODS collections"""
        print("🧹 Cleaning Telco ODS collections...")
        
        # Collections to clean
        collections_to_clean = [
            'customers',
            'cells', 
            'service_plans',
            'ran_network_metrics',
            'core_network_metrics',
            'mobile_service_metrics'
        ]
        
        total_deleted = 0
        
        for collection_name in collections_to_clean:
            try:
                collection = self.db[collection_name]
                # Count documents before deletion
                count_before = collection.count_documents({})
                
                if count_before > 0:
                    # Delete all documents but keep the collection
                    result = collection.delete_many({})
                    print(f"✅ Cleaned {collection_name}: {result.deleted_count} documents removed")
                    total_deleted += result.deleted_count
                else:
                    print(f"ℹ️ {collection_name}: Already empty")
                    
            except Exception as e:
                print(f"⚠️ Error cleaning {collection_name}: {e}")
        
        print(f"\n🎉 Data cleaning completed!")
        print(f"📊 Total documents removed: {total_deleted}")
        print(f"📋 Collections cleaned: {len(collections_to_clean)}")
        print(f"💾 Collections and indexes preserved")

    def show_collection_stats(self):
        """Show current collection statistics"""
        print("\n📊 Current collection statistics:")
        print("-" * 50)
        
        collections_to_check = [
            'customers',
            'cells', 
            'service_plans',
            'ran_network_metrics',
            'core_network_metrics',
            'mobile_service_metrics'
        ]
        
        for collection_name in collections_to_check:
            try:
                collection = self.db[collection_name]
                count = collection.count_documents({})
                print(f"{collection_name:25} | {count:8} documents")
            except Exception as e:
                print(f"{collection_name:25} | Error: {e}")

def main():
    """Main function to run the data cleaning"""
    try:
        cleaner = TelcoODSCleaner()
        
        # Show current stats
        cleaner.show_collection_stats()
        
        # Ask for confirmation
        print("\n⚠️  This will remove ALL data from Telco ODS collections!")
        print("   Collections and indexes will be preserved.")
        response = input("   Continue? (y/N): ").strip().lower()
        
        if response in ['y', 'yes']:
            cleaner.clean_collections()
            print("\n📊 Final collection statistics:")
            cleaner.show_collection_stats()
        else:
            print("❌ Data cleaning cancelled.")
            
    except Exception as e:
        print(f"❌ Error during data cleaning: {e}")
        raise

if __name__ == "__main__":
    main()
