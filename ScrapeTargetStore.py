import pymongo
from datetime import datetime


###################
# 1. MongoDB as a Central Source of Truth
# Profiles are stored in a structured, queryable format.
#
# Supports metadata like added_by, active, platform, and timestamps.
#
# Enables audit trails, versioning, and analytics.
######################

class ScrapeTargetStore:
    def __init__(self, mongo_uri="mongodb://localhost:27017", db_name="social_scraper"):
        # Add timeouts to prevent hanging
        self.client = pymongo.MongoClient(
            mongo_uri,
            serverSelectionTimeoutMS=2000,  # 2 second timeout for server selection
            connectTimeoutMS=2000,  # 2 second timeout for connection
            socketTimeoutMS=2000  # 2 second timeout for socket operations
        )

        # Test connection immediately
        try:
            self.client.server_info()
            print("✓ MongoDB connection verified")
        except Exception as e:
            print(f"✗ MongoDB connection test failed: {e}")
            raise

        self.db = self.client[db_name]
        self.collection = self.db["scrape_targets"]

        # Create unique compound index
        self.collection.create_index(
            [("platform", pymongo.ASCENDING), ("target_type", pymongo.ASCENDING), ("value", pymongo.ASCENDING)],
            unique=True
        )

    def add_target(self, platform: str, target_type: str, value: str, added_by: str = "admin"):
        """
        Add a new scrape target to the database.
        Uses upsert to avoid duplicates.
        """
        doc = {
            "platform": platform,
            "target_type": target_type,
            "value": value,
            "active": True,
            "added_by": added_by,
            "added_at": datetime.utcnow(),
            "last_scraped": None
        }
        result = self.collection.update_one(
            {"platform": platform, "target_type": target_type, "value": value},
            {"$setOnInsert": doc},
            upsert=True
        )
        return result

    def get_active_targets(self, platform: str = None, target_type: str = None):
        """
        Get all active targets, optionally filtered by platform and target_type.
        """
        query = {"active": True}
        if platform:
            query["platform"] = platform
        if target_type:
            query["target_type"] = target_type

        return list(self.collection.find(query))

    def get_all_targets(self, platform: str = None):
        """
        Get all targets (active and inactive), optionally filtered by platform.
        """
        query = {}
        if platform:
            query["platform"] = platform

        return list(self.collection.find(query))

    def mark_scraped(self, platform: str, target_type: str, value: str):
        """
        Update the last_scraped timestamp for a target.
        """
        result = self.collection.update_one(
            {"platform": platform, "target_type": target_type, "value": value},
            {"$set": {"last_scraped": datetime.utcnow()}}
        )
        return result

    def toggle_active(self, platform: str, target_type: str, value: str):
        """
        Toggle the active status of a target.
        """
        target = self.collection.find_one({
            "platform": platform,
            "target_type": target_type,
            "value": value
        })

        if target:
            new_status = not target.get("active", True)
            result = self.collection.update_one(
                {"platform": platform, "target_type": target_type, "value": value},
                {"$set": {"active": new_status}}
            )
            return result
        return None

    def delete_target(self, platform: str, target_type: str, value: str):
        """
        Permanently delete a target from the database.
        """
        result = self.collection.delete_one({
            "platform": platform,
            "target_type": target_type,
            "value": value
        })
        return result

    def deactivate_target(self, platform: str, target_type: str, value: str):
        """
        Soft delete - set active to False instead of removing.
        """
        result = self.collection.update_one(
            {"platform": platform, "target_type": target_type, "value": value},
            {"$set": {"active": False}}
        )
        return result

    def get_stats(self):
        """
        Get statistics about targets in the database.
        """
        total = self.collection.count_documents({})
        active = self.collection.count_documents({"active": True})
        inactive = self.collection.count_documents({"active": False})

        # Count by platform
        pipeline = [
            {"$group": {"_id": "$platform", "count": {"$sum": 1}}}
        ]
        by_platform = list(self.collection.aggregate(pipeline))

        return {
            "total": total,
            "active": active,
            "inactive": inactive,
            "by_platform": {item["_id"]: item["count"] for item in by_platform}
        }