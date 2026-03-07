from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta
from typing import Optional
import pymongo
import os
from collections import defaultdict

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])

# Global service instance
dashboard_service = None


class DashboardService:
    def __init__(self, mongo_uri=os.getenv("MONGODB_URI")):
        self.connected = False
        try:
            self.client = pymongo.MongoClient(
                mongo_uri,
                serverSelectionTimeoutMS=2000,
                connectTimeoutMS=2000,
                socketTimeoutMS=2000
            )

            # Test connection
            self.client.server_info()

            self.social_scraper_db = self.client["social_scraper"]
            self.offer_insights_db = self.client["offer_insights"]

            # Collections
            self.raw_data_collection = self.social_scraper_db["raw_social_data"]
            self.offers_collection = self.offer_insights_db["offers"]
            self.targets_collection = self.social_scraper_db["scrape_targets"]
            self.system_config_collection = self.social_scraper_db["system_config"]  # ← NEW

            self.connected = True
            print("✓ Dashboard MongoDB connection successful")

        except Exception as e:
            print(f"✗ Dashboard MongoDB connection failed: {e}")
            print("  Dashboard will use mock data")
            self.connected = False

    def get_time_filter(self, time_range: str):
        """Generate MongoDB time filter based on range"""
        now = datetime.utcnow()
        if time_range == "24h":
            start_time = now - timedelta(hours=24)
        elif time_range == "7d":
            start_time = now - timedelta(days=7)
        elif time_range == "30d":
            start_time = now - timedelta(days=30)
        else:
            start_time = now - timedelta(hours=24)

        return {"scraped_at": {"$gte": start_time}}

    def get_mock_stats(self):
        """Return mock data when MongoDB is not available"""
        return {
            "totalPosts": 156,
            "totalOffers": 89,
            "activeProfiles": 12,
            "lastScraped": datetime.utcnow().isoformat(),
            "successRate": 57.05,
            "byPlatform": {
                "facebook": 89,
                "instagram": 45,
                "tiktok": 22
            },
            "byProfile": {
                "promoofficiel": 45,
                "dealsalgeria": 32,
                "promodz": 28,
                "bestoffers_dz": 25,
                "others": 26
            },
            "offersByCategory": {
                "shoes": 34,
                "fashion": 28,
                "electronics": 15,
                "beauty": 12
            },
            "recentActivity": [
                {
                    "time": "2m ago",
                    "profile": "promoofficiel",
                    "platform": "facebook",
                    "posts": 3,
                    "status": "success"
                },
                {
                    "time": "15m ago",
                    "profile": "dealsalgeria",
                    "platform": "instagram",
                    "posts": 5,
                    "status": "success"
                },
                {
                    "time": "1h ago",
                    "profile": "promodz",
                    "platform": "facebook",
                    "posts": 2,
                    "status": "success"
                },
                {
                    "time": "2h ago",
                    "profile": "bestoffers_dz",
                    "platform": "tiktok",
                    "posts": 0,
                    "status": "warning"
                }
            ]
        }

    def get_stats(self, time_range: str = "24h"):
        """Get overall statistics"""
        if not self.connected:
            return self.get_mock_stats()

        try:
            time_filter = self.get_time_filter(time_range)

            # Total posts in time range from raw_social_data
            total_posts = self.raw_data_collection.count_documents(time_filter)
            print(f"total_posts: {total_posts}")

            # Total VALID offers in time range from offers collection
            offer_filter = {
                **time_filter,
                "brand_name": {"$ne": None, "$exists": True},
                "confidence_score": {"$gt": 0.8}
            }
            total_offers = self.offers_collection.count_documents(offer_filter)
            print(f"total_offers (valid): {total_offers}")

            # Active profiles (from scrape_targets)
            active_profiles = self.targets_collection.count_documents({"active": True})

            # Last scraped time
            last_post = self.raw_data_collection.find_one(
                sort=[("scraped_at", pymongo.DESCENDING)]
            )
            last_scraped = last_post["scraped_at"] if last_post else None

            # Success rate (posts with valid offers / total posts)
            success_rate = (total_offers / total_posts * 100) if total_posts > 0 else 0

            return {
                "totalPosts": total_posts,
                "totalOffers": total_offers,
                "activeProfiles": active_profiles,
                "lastScraped": last_scraped.isoformat() if last_scraped else None,
                "successRate": round(success_rate, 2)
            }
        except Exception as e:
            print(f"Error in get_stats: {e}")
            import traceback
            traceback.print_exc()
            return self.get_mock_stats()

    def get_by_platform(self, time_range: str = "24h"):
        """Get post counts by platform"""
        if not self.connected:
            return self.get_mock_stats()["byPlatform"]

        try:
            time_filter = self.get_time_filter(time_range)

            pipeline = [
                {"$match": time_filter},
                {"$group": {
                    "_id": "$platform",
                    "count": {"$sum": 1}
                }}
            ]

            results = self.raw_data_collection.aggregate(pipeline)
            return {doc["_id"]: doc["count"] for doc in results}
        except Exception as e:
            print(f"Error in get_by_platform: {e}")
            return self.get_mock_stats()["byPlatform"]

    def get_by_profile(self, time_range: str = "24h", limit: int = 10):
        """Get post counts by profile"""
        if not self.connected:
            return self.get_mock_stats()["byProfile"]

        try:
            time_filter = self.get_time_filter(time_range)

            pipeline = [
                {"$match": time_filter},
                {"$group": {
                    "_id": "$profile",
                    "count": {"$sum": 1}
                }},
                {"$sort": {"count": -1}},
                {"$limit": limit}
            ]

            results = self.raw_data_collection.aggregate(pipeline)
            return {doc["_id"]: doc["count"] for doc in results}
        except Exception as e:
            print(f"Error in get_by_profile: {e}")
            return self.get_mock_stats()["byProfile"]

    def get_offers_by_category(self, time_range: str = "24h"):
        """Get offers grouped by product category"""
        if not self.connected:
            return self.get_mock_stats()["offersByCategory"]

        try:
            time_filter = self.get_time_filter(time_range)

            offer_filter = {
                **time_filter,
                "brand_name": {"$ne": None, "$exists": True},
                "confidence_score": {"$gt": 0.8}
            }

            pipeline = [
                {"$match": offer_filter},
                {"$group": {
                    "_id": "$product_category",
                    "count": {"$sum": 1}
                }},
                {"$sort": {"count": -1}}
            ]

            results = self.offers_collection.aggregate(pipeline)
            return {doc["_id"]: doc["count"] for doc in results if doc["_id"]}
        except Exception as e:
            print(f"Error in get_offers_by_category: {e}")
            return self.get_mock_stats()["offersByCategory"]

    def get_recent_activity(self, limit: int = 10):
        """Get recent scraping activity"""
        if not self.connected:
            return self.get_mock_stats()["recentActivity"]

        try:
            pipeline = [
                {"$sort": {"scraped_at": -1}},
                {"$group": {
                    "_id": {
                        "profile": "$profile",
                        "platform": "$platform"
                    },
                    "count": {"$sum": 1},
                    "latest": {"$max": "$scraped_at"}
                }},
                {"$sort": {"latest": -1}},
                {"$limit": limit}
            ]

            results = list(self.raw_data_collection.aggregate(pipeline))
            activities = []

            for doc in results:
                time_diff = datetime.utcnow() - doc["latest"]
                total_seconds = time_diff.total_seconds()

                if total_seconds < 60:
                    time_str = f"{int(total_seconds)}s ago"
                elif total_seconds < 3600:
                    time_str = f"{int(total_seconds // 60)}m ago"
                elif time_diff.days == 0:
                    time_str = f"{int(total_seconds // 3600)}h ago"
                else:
                    time_str = f"{time_diff.days}d ago"

                activities.append({
                    "profile": doc["_id"]["profile"],
                    "platform": doc["_id"]["platform"],
                    "posts": doc["count"],
                    "time": time_str,
                    "status": "success" if doc["count"] > 0 else "warning"
                })

            return activities
        except Exception as e:
            print(f"Error in get_recent_activity: {e}")
            import traceback
            traceback.print_exc()
            return self.get_mock_stats()["recentActivity"]

    def get_incomplete_posts(self, time_range: str = "24h"):
        """Get posts with only one type of content (text only, image only, video only)"""
        if not self.connected:
            return {"text_only": [], "image_only": [], "video_only": [], "total": 0}

        try:
            time_filter = self.get_time_filter(time_range)
            start_time = time_filter["scraped_at"]["$gte"]

            def has(field):
                return {"$and": [
                    {field: {"$nin": [None, "", [], ""]}},
                    {field: {"$exists": True}}
                ]}

            def has_not(field):
                return {"$or": [
                    {field: {"$in": [None, "", []]}},
                    {field: {"$exists": False}}
                ]}

            # Text only: has text, no image, no video
            text_only_filter = {
                "$and": [
                    {"scraped_at": {"$gte": start_time}},
                    has("post_text"),
                    has_not("post_images"),
                    has_not("post_video")
                ]
            }

            # Image only: has images, no text, no video
            image_only_filter = {
                "$and": [
                    {"scraped_at": {"$gte": start_time}},
                    has("post_images"),
                    has_not("post_text"),
                    has_not("post_video")
                ]
            }

            # Video only: has video, no text, no image
            video_only_filter = {
                "$and": [
                    {"scraped_at": {"$gte": start_time}},
                    has("post_video"),
                    has_not("post_text"),
                    has_not("post_images")
                ]
            }

            projection = {"_id": 1, "post_id": 1, "platform": 1, "profile": 1, "scraped_at": 1}

            def fetch(f):
                docs = list(self.raw_data_collection.find(f, projection).limit(50))
                for doc in docs:
                    doc["_id"] = str(doc["_id"])
                    if "scraped_at" in doc and hasattr(doc["scraped_at"], "isoformat"):
                        doc["scraped_at"] = doc["scraped_at"].isoformat()
                return docs

            text_only = fetch(text_only_filter)
            image_only = fetch(image_only_filter)
            video_only = fetch(video_only_filter)

            return {
                "text_only": text_only,
                "image_only": image_only,
                "video_only": video_only,
                "total": len(text_only) + len(image_only) + len(video_only)
            }

        except Exception as e:
            print(f"Error in get_incomplete_posts: {e}")
            import traceback
            traceback.print_exc()
            return {"text_only": [], "image_only": [], "video_only": [], "total": 0}

        except Exception as e:
            print(f"Error in get_incomplete_posts: {e}")
            import traceback
            traceback.print_exc()
            return {"missing_images": [], "missing_video": [], "total": 0}

    def get_profile_success_rate(self, time_range: str = "24h", limit: int = 10):
        """Get success rate by profile (% of posts that yielded valid offers)"""
        if not self.connected:
            return {}

        try:
            time_filter = self.get_time_filter(time_range)

            posts_pipeline = [
                {"$match": time_filter},
                {"$group": {
                    "_id": "$profile",
                    "total_posts": {"$sum": 1}
                }},
                {"$sort": {"total_posts": -1}},
                {"$limit": limit}
            ]

            posts_by_profile = {
                doc["_id"]: doc["total_posts"]
                for doc in self.raw_data_collection.aggregate(posts_pipeline)
            }

            offer_filter = {
                **time_filter,
                "brand_name": {"$ne": None, "$exists": True},
                "confidence_score": {"$gt": 0.8}
            }

            offers_pipeline = [
                {"$match": offer_filter},
                {"$group": {
                    "_id": "$profile",
                    "offers": {"$sum": 1}
                }}
            ]

            offers_by_profile = {
                doc["_id"]: doc["offers"]
                for doc in self.offers_collection.aggregate(offers_pipeline)
            }

            success_rates = {}
            for profile, posts in posts_by_profile.items():
                offers = offers_by_profile.get(profile, 0)
                rate = (offers / posts * 100) if posts > 0 else 0
                success_rates[profile] = {
                    "rate": round(rate, 2),
                    "posts": posts,
                    "offers": offers
                }

            return success_rates

        except Exception as e:
            print(f"Error in get_profile_success_rate: {e}")
            import traceback
            traceback.print_exc()
            return {}

    def get_top_brands(self, time_range: str = "24h", limit: int = 50):
        """Get top brands by offer count"""
        if not self.connected:
            return {}

        try:
            time_filter = self.get_time_filter(time_range)

            offer_filter = {
                **time_filter,
                "brand_name": {"$ne": None, "$exists": True},
                "confidence_score": {"$gt": 0.8}
            }

            pipeline = [
                {"$match": offer_filter},
                {"$group": {
                    "_id": "$brand_name",
                    "count": {"$sum": 1}
                }},
                {"$sort": {"count": -1}},
                {"$limit": limit}
            ]

            results = self.offers_collection.aggregate(pipeline)
            return {doc["_id"]: doc["count"] for doc in results if doc["_id"]}

        except Exception as e:
            print(f"Error in get_top_brands: {e}")
            import traceback
            traceback.print_exc()
            return {}

    def get_offers_by_country(self, time_range: str = "24h"):
        """Get offers grouped by country using NORMALIZED fields"""
        if not self.connected:
            return {}

        try:
            time_filter = self.get_time_filter(time_range)

            offer_filter = {
                **time_filter,
                "brand_name": {"$ne": None, "$exists": True},
                "confidence_score": {"$gt": 0.8},
                "is_normalized": True,
                "normalized_fields.location.country": {"$exists": True, "$ne": None}
            }

            pipeline = [
                {"$match": offer_filter},
                {"$group": {
                    "_id": "$normalized_fields.location.country",
                    "count": {"$sum": 1}
                }},
                {"$sort": {"count": -1}}
            ]

            results = self.offers_collection.aggregate(pipeline)
            breakdown = {doc["_id"]: doc["count"] for doc in results if doc["_id"]}

            print(f"Country breakdown (normalized): {breakdown}")
            return breakdown

        except Exception as e:
            print(f"Error in get_offers_by_country: {e}")
            import traceback
            traceback.print_exc()
            return {}

    def get_discount_types_distribution(self, time_range: str = "24h"):
        """Get distribution of discount types using NORMALIZED fields"""
        if not self.connected:
            return {}

        try:
            time_filter = self.get_time_filter(time_range)

            offer_filter = {
                **time_filter,
                "brand_name": {"$ne": None, "$exists": True},
                "confidence_score": {"$gt": 0.8},
                "is_normalized": True,
                "normalized_fields.discounts": {"$exists": True, "$ne": []}
            }

            pipeline = [
                {"$match": offer_filter},
                {"$unwind": "$normalized_fields.discounts"},
                {"$group": {
                    "_id": "$normalized_fields.discounts.discount_type",
                    "count": {"$sum": 1}
                }},
                {"$sort": {"count": -1}}
            ]

            results = self.offers_collection.aggregate(pipeline)
            return {doc["_id"]: doc["count"] for doc in results if doc["_id"]}

        except Exception as e:
            print(f"Error in get_discount_types_distribution: {e}")
            import traceback
            traceback.print_exc()
            return {}

    def get_promo_code_usage(self, time_range: str = "24h"):
        """Get stats on promo code usage"""
        if not self.connected:
            return {"with_promo_code": 0, "without_promo_code": 0, "percentage_with_code": 0}

        try:
            time_filter = self.get_time_filter(time_range)

            offer_filter = {
                **time_filter,
                "brand_name": {"$ne": None, "$exists": True},
                "confidence_score": {"$gt": 0.8}
            }

            pipeline = [
                {"$match": offer_filter},
                {"$group": {
                    "_id": {
                        "$cond": [
                            {"$and": [
                                {"$ne": ["$promo_code", None]},
                                {"$ne": ["$promo_code", ""]}
                            ]},
                            "with_code",
                            "without_code"
                        ]
                    },
                    "count": {"$sum": 1}
                }}
            ]

            results = list(self.offers_collection.aggregate(pipeline))
            stats = {"with_promo_code": 0, "without_promo_code": 0}

            for doc in results:
                if doc["_id"] == "with_code":
                    stats["with_promo_code"] = doc["count"]
                else:
                    stats["without_promo_code"] = doc["count"]

            total = stats["with_promo_code"] + stats["without_promo_code"]
            stats["percentage_with_code"] = round((stats["with_promo_code"] / total * 100) if total > 0 else 0, 2)

            return stats

        except Exception as e:
            print(f"Error in get_promo_code_usage: {e}")
            import traceback
            traceback.print_exc()
            return {"with_promo_code": 0, "without_promo_code": 0, "percentage_with_code": 0}

    def get_average_discount_value(self, time_range: str = "24h"):
        """Get average discount value by currency using NORMALIZED fields"""
        if not self.connected:
            return {"overall": 0, "by_currency": {}}

        try:
            time_filter = self.get_time_filter(time_range)

            offer_filter = {
                **time_filter,
                "brand_name": {"$ne": None, "$exists": True},
                "confidence_score": {"$gt": 0.8},
                "is_normalized": True,
                "normalized_fields.discounts": {"$exists": True, "$ne": []}
            }

            pipeline = [
                {"$match": offer_filter},
                {"$unwind": "$normalized_fields.discounts"},
                {"$match": {
                    "normalized_fields.discounts.discount_amount": {"$ne": None, "$exists": True, "$gt": 0}
                }},
                {"$group": {
                    "_id": "$normalized_fields.discounts.discount_currency",
                    "avg_discount": {"$avg": "$normalized_fields.discounts.discount_amount"},
                    "count": {"$sum": 1}
                }},
                {"$sort": {"count": -1}}
            ]

            results = list(self.offers_collection.aggregate(pipeline))
            by_currency = {}
            total_discount = 0
            total_count = 0

            for doc in results:
                if doc["_id"]:
                    by_currency[doc["_id"]] = round(doc["avg_discount"], 2)
                    total_discount += doc["avg_discount"] * doc["count"]
                    total_count += doc["count"]

            overall = round(total_discount / total_count, 2) if total_count > 0 else 0

            return {
                "overall": overall,
                "by_currency": by_currency
            }

        except Exception as e:
            print(f"Error in get_average_discount_value: {e}")
            import traceback
            traceback.print_exc()
            return {"overall": 0, "by_currency": {}}

    def get_offer_type_breakdown(self, time_range: str = "24h"):
        """Get breakdown of offer types"""
        if not self.connected:
            return {}

        try:
            time_filter = self.get_time_filter(time_range)

            offer_filter = {
                **time_filter,
                "brand_name": {"$ne": None, "$exists": True},
                "confidence_score": {"$gt": 0.8}
            }

            pipeline = [
                {"$match": offer_filter},
                {"$group": {
                    "_id": "$offer_type",
                    "count": {"$sum": 1}
                }},
                {"$sort": {"count": -1}}
            ]

            results = self.offers_collection.aggregate(pipeline)
            breakdown = {doc["_id"]: doc["count"] for doc in results if doc["_id"]}

            print(f"Offer type breakdown: {breakdown}")
            return breakdown

        except Exception as e:
            print(f"Error in get_offer_type_breakdown: {e}")
            import traceback
            traceback.print_exc()
            return {}

    def get_total_profiles_count(self):
        """Get total number of profiles"""
        if not self.connected:
            return 50

        try:
            return self.targets_collection.count_documents({})
        except Exception as e:
            print(f"Error in get_total_profiles_count: {e}")
            return 50

    def get_database_stats(self):
        """Get database and collection statistics"""
        if not self.connected:
            return {
                "raw_social_data": {"count": 0, "size": "0 B", "avgObjSize": "0 B"},
                "offers": {"count": 0, "validCount": 0, "size": "0 B", "avgObjSize": "0 B"}
            }

        try:
            stats = {}

            raw_stats = self.social_scraper_db.command("collStats", "raw_social_data")
            stats["raw_social_data"] = {
                "count": raw_stats.get("count", 0),
                "size": self._format_bytes(raw_stats.get("size", 0)),
                "avgObjSize": self._format_bytes(raw_stats.get("avgObjSize", 0))
            }

            offers_stats = self.offer_insights_db.command("collStats", "offers")
            valid_offers = self.offers_collection.count_documents({
                "brand_name": {"$ne": None, "$exists": True},
                "confidence_score": {"$gt": 0.8}
            })

            stats["offers"] = {
                "count": offers_stats.get("count", 0),
                "validCount": valid_offers,
                "size": self._format_bytes(offers_stats.get("size", 0)),
                "avgObjSize": self._format_bytes(offers_stats.get("avgObjSize", 0))
            }

            return stats

        except Exception as e:
            print(f"Error in get_database_stats: {e}")
            import traceback
            traceback.print_exc()
            return {
                "raw_social_data": {"count": 0, "size": "0 B", "avgObjSize": "0 B"},
                "offers": {"count": 0, "validCount": 0, "size": "0 B", "avgObjSize": "0 B"}
            }

    def get_profile_performance(self, time_range: str = "24h", limit: int = 10):
        """Get profile performance: all profiles sorted by valid offers (highest first)"""
        if not self.connected:
            return []

        try:
            time_filter = self.get_time_filter(time_range)

            posts_pipeline = [
                {"$match": time_filter},
                {"$group": {
                    "_id": "$profile",
                    "total_posts": {"$sum": 1}
                }},
                {"$sort": {"total_posts": -1}}
            ]

            posts_by_profile = {
                doc["_id"]: doc["total_posts"]
                for doc in self.raw_data_collection.aggregate(posts_pipeline)
            }

            offer_filter = {
                **time_filter,
                "brand_name": {"$ne": None, "$exists": True},
                "confidence_score": {"$gt": 0.8}
            }

            offers_pipeline = [
                {"$match": offer_filter},
                {"$group": {
                    "_id": "$profile",
                    "valid_offers": {"$sum": 1}
                }}
            ]

            offers_by_profile = {
                doc["_id"]: doc["valid_offers"]
                for doc in self.offers_collection.aggregate(offers_pipeline)
            }

            all_profiles = []
            for profile, total_posts in posts_by_profile.items():
                valid_offers = offers_by_profile.get(profile, 0)
                all_profiles.append({
                    "profile": profile,
                    "total_posts": total_posts,
                    "valid_offers": valid_offers,
                    "success_rate": round((valid_offers / total_posts * 100) if total_posts > 0 else 0, 2)
                })

            all_profiles.sort(key=lambda x: (x["valid_offers"], x["total_posts"]), reverse=True)
            return all_profiles[:limit]

        except Exception as e:
            print(f"Error in get_profile_performance: {e}")
            import traceback
            traceback.print_exc()
            return []

    def get_total_profiles_in_range(self, time_range: str = "24h"):
        """Get total number of unique profiles that posted in time range"""
        if not self.connected:
            return 50

        try:
            time_filter = self.get_time_filter(time_range)

            pipeline = [
                {"$match": time_filter},
                {"$group": {"_id": "$profile"}},
                {"$count": "total"}
            ]

            result = list(self.raw_data_collection.aggregate(pipeline))
            return result[0]["total"] if result else 0

        except Exception as e:
            print(f"Error in get_total_profiles_in_range: {e}")
            return 50

    def get_failed_scrapes_count(self, time_range: str = "24h"):
        """Count posts with scraping issues (missing text, img or video)"""
        if not self.connected:
            return {"total": 0, "by_platform": {}, "by_profile": {}}

        try:
            time_filter = self.get_time_filter(time_range)
            start_time = time_filter["scraped_at"]["$gte"]

            failed_filter = {
                "$and": [
                    {"scraped_at": {"$gte": start_time}},
                    {"$or": [
                        {"post_text": {"$in": [None, ""]}},
                        {"post_text": {"$exists": False}}
                    ]},
                    {"$or": [
                        {"post_images": {"$in": [[], None, ""]}},
                        {"post_images": {"$exists": False}}
                    ]},
                    {"$or": [
                        {"post_video": {"$in": [[], None, ""]}},  # <-- added ""
                        {"post_video": {"$exists": False}}
                    ]}
                ]
            }

            total_failed = self.raw_data_collection.count_documents(failed_filter)

            platform_pipeline = [
                {"$match": failed_filter},
                {"$group": {"_id": "$platform", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]

            by_platform = {
                doc["_id"]: doc["count"]
                for doc in self.raw_data_collection.aggregate(platform_pipeline)
            }

            profile_pipeline = [
                {"$match": failed_filter},
                {"$group": {"_id": "$profile", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]

            by_profile = {
                doc["_id"]: doc["count"]
                for doc in self.raw_data_collection.aggregate(profile_pipeline)
            }

            return {
                "total": total_failed,
                "by_platform": by_platform,
                "by_profile": by_profile
            }

        except Exception as e:
            print(f"Error in get_failed_scrapes_count: {e}")
            import traceback
            traceback.print_exc()
            return {"total": 0, "by_platform": {}, "by_profile": {}}



    def get_inactive_offers_count(self, time_range: str = "24h"):
        """Count offers that are no longer active based on valid_until date"""
        if not self.connected:
            return {"total": 0, "by_brand": {}, "by_category": {}}

        try:
            time_filter = self.get_time_filter(time_range)
            current_date = datetime.now()

            inactive_filter = {
                **time_filter,
                "brand_name": {"$ne": None, "$exists": True},
                "confidence_score": {"$gt": 0.8},
                "$or": [
                    {
                        "normalized_fields.valid_until": {
                            "$exists": True,
                            "$ne": None,
                            "$lt": current_date.strftime("%Y-%m-%d")
                        }
                    },
                    {
                        "is_active": False,
                        "$or": [
                            {"normalized_fields.valid_until": {"$exists": False}},
                            {"normalized_fields.valid_until": None}
                        ]
                    }
                ]
            }

            total_inactive = self.offers_collection.count_documents(inactive_filter)

            brand_pipeline = [
                {"$match": inactive_filter},
                {"$group": {"_id": "$brand_name", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]

            by_brand = {
                doc["_id"]: doc["count"]
                for doc in self.offers_collection.aggregate(brand_pipeline)
            }

            category_pipeline = [
                {"$match": inactive_filter},
                {"$group": {"_id": "$product_category", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]

            by_category = {
                doc["_id"]: doc["count"]
                for doc in self.offers_collection.aggregate(category_pipeline)
                if doc["_id"]
            }

            return {
                "total": total_inactive,
                "by_brand": by_brand,
                "by_category": by_category
            }

        except Exception as e:
            print(f"Error in get_inactive_offers_count: {e}")
            import traceback
            traceback.print_exc()
            return {"total": 0, "by_brand": {}, "by_category": {}}

    def get_stale_profiles(self, hours: int = 24):
        """Get active profiles not scraped in X hours"""
        if not self.connected:
            return {"count": 0, "profiles": []}

        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)

            stale_query = {
                "target_type": "profile",
                "active": True,
                "$or": [
                    {"last_scraped": {"$lt": cutoff_time}},
                    {"last_scraped": {"$exists": False}},
                    {"last_scraped": None}
                ]
            }

            projection = {"_id": 0, "value": 1, "platform": 1, "last_scraped": 1}

            stale_targets = list(
                self.targets_collection.find(stale_query, projection)
                .sort("last_scraped", pymongo.ASCENDING)
                .limit(20)
            )

            profiles = []
            now = datetime.utcnow()

            for t in stale_targets:
                if t.get("last_scraped"):
                    hours_ago = int((now - t["last_scraped"]).total_seconds() / 3600)
                    last_scraped = t["last_scraped"].isoformat()
                else:
                    hours_ago = None
                    last_scraped = None

                profiles.append({
                    "profile": t["value"],
                    "platform": t.get("platform"),
                    "last_scraped": last_scraped,
                    "hours_ago": hours_ago
                })

            total_count = self.targets_collection.count_documents(stale_query)

            return {"count": total_count}

        except Exception as e:
            print(f"Error in get_stale_profiles: {e}")
            import traceback
            traceback.print_exc()
            return {"count": 0, "profiles": []}

    def get_ai_extraction_metrics(self, time_range: str = "24h"):
        """Get AI extraction performance metrics"""
        if not self.connected:
            return {
                "extraction_success_rate": 85.5,
                "average_confidence": 0.87,
                "low_confidence_count": 15,
                "medium_confidence_count": 45,
                "high_confidence_count": 120,
                "confidence_distribution": {
                    "0.0-0.3": 2,
                    "0.3-0.5": 5,
                    "0.5-0.7": 8,
                    "0.7-0.8": 15,
                    "0.8-0.9": 45,
                    "0.9-1.0": 120
                },
                "extraction_over_time": []
            }

        try:
            time_filter = self.get_time_filter(time_range)

            total_filter = {**time_filter, "extracted_by_llm": True}
            total_extracted = self.offers_collection.count_documents(total_filter)

            success_filter = {
                **time_filter,
                "extracted_by_llm": True,
                "brand_name": {"$ne": None, "$exists": True},
                "confidence_score": {"$gt": 0.7}
            }
            successful_extractions = self.offers_collection.count_documents(success_filter)

            extraction_success_rate = (
                successful_extractions / total_extracted * 100
            ) if total_extracted > 0 else 0

            avg_confidence_pipeline = [
                {"$match": {
                    **time_filter,
                    "extracted_by_llm": True,
                    "confidence_score": {"$exists": True, "$ne": None}
                }},
                {"$group": {"_id": None, "avg_confidence": {"$avg": "$confidence_score"}}}
            ]

            avg_result = list(self.offers_collection.aggregate(avg_confidence_pipeline))
            average_confidence = avg_result[0]["avg_confidence"] if avg_result else 0

            low_confidence = self.offers_collection.count_documents({
                **time_filter, "confidence_score": {"$gte": 0.7, "$lt": 0.8}
            })
            medium_confidence = self.offers_collection.count_documents({
                **time_filter, "confidence_score": {"$gte": 0.8, "$lt": 0.9}
            })
            high_confidence = self.offers_collection.count_documents({
                **time_filter, "confidence_score": {"$gte": 0.9}
            })

            confidence_ranges = [
                ("0.0-0.3", 0.0, 0.3),
                ("0.3-0.5", 0.3, 0.5),
                ("0.5-0.7", 0.5, 0.7),
                ("0.7-0.8", 0.7, 0.8),
                ("0.8-0.9", 0.8, 0.9),
                ("0.9-1.0", 0.9, 1.0)
            ]

            confidence_distribution = {}
            for label, min_score, max_score in confidence_ranges:
                count = self.offers_collection.count_documents({
                    **time_filter,
                    "confidence_score": {"$gte": min_score, "$lt": max_score}
                })
                confidence_distribution[label] = count

            if time_range == "24h":
                group_id = {
                    "year": {"$year": "$scraped_at"},
                    "month": {"$month": "$scraped_at"},
                    "day": {"$dayOfMonth": "$scraped_at"},
                    "hour": {"$hour": "$scraped_at"}
                }
            else:
                group_id = {
                    "year": {"$year": "$scraped_at"},
                    "month": {"$month": "$scraped_at"},
                    "day": {"$dayOfMonth": "$scraped_at"}
                }

            extraction_timeline_pipeline = [
                {"$match": {**time_filter, "extracted_by_llm": True}},
                {"$group": {
                    "_id": group_id,
                    "total": {"$sum": 1},
                    "successful": {
                        "$sum": {
                            "$cond": [
                                {"$and": [
                                    {"$ne": ["$brand_name", None]},
                                    {"$gt": ["$confidence_score", 0.7]}
                                ]},
                                1, 0
                            ]
                        }
                    },
                    "avg_confidence": {"$avg": "$confidence_score"},
                    "timestamp": {"$first": "$scraped_at"}
                }},
                {"$sort": {"timestamp": 1}},
                {"$project": {
                    "_id": 0,
                    "timestamp": 1,
                    "total": 1,
                    "successful": 1,
                    "success_rate": {
                        "$multiply": [{"$divide": ["$successful", "$total"]}, 100]
                    },
                    "avg_confidence": {"$round": ["$avg_confidence", 2]}
                }}
            ]

            extraction_over_time = list(
                self.offers_collection.aggregate(extraction_timeline_pipeline)
            )

            return {
                "extraction_success_rate": round(extraction_success_rate, 1),
                "average_confidence": round(average_confidence, 2),
                "low_confidence_count": low_confidence,
                "medium_confidence_count": medium_confidence,
                "high_confidence_count": high_confidence,
                "confidence_distribution": confidence_distribution,
                "extraction_over_time": extraction_over_time
            }

        except Exception as e:
            print(f"Error in get_ai_extraction_metrics: {e}")
            import traceback
            traceback.print_exc()
            return {
                "extraction_success_rate": 0,
                "average_confidence": 0,
                "low_confidence_count": 0,
                "medium_confidence_count": 0,
                "high_confidence_count": 0,
                "confidence_distribution": {},
                "extraction_over_time": []
            }

    # =========================================================================
    # System Configuration
    # =========================================================================

    def get_system_config(self):
        """
        Fetch all active config documents from social_scraper.system_config.
        Returns a list of config dicts with ObjectId and datetime fields
        serialized to strings so they are JSON-safe.
        """
        if not self.connected:
            return self._get_mock_config()

        try:
            docs = list(
                self.system_config_collection.find(
                    {},                                          # all config types
                    {"_id": 1, "type": 1, "version": 1,
                     "is_active": 1, "data": 1, "updated_at": 1}
                ).sort("type", pymongo.ASCENDING)
            )

            result = []
            for doc in docs:
                result.append({
                    "id": str(doc["_id"]),
                    "type": doc.get("type"),
                    "version": doc.get("version", 1),
                    "is_active": doc.get("is_active", True),
                    "data": doc.get("data", {}),
                    # updated_at may be a datetime object from Mongo
                    "updated_at": (
                        doc["updated_at"].isoformat()
                        if isinstance(doc.get("updated_at"), datetime)
                        else str(doc.get("updated_at", ""))
                    )
                })

            print(f"✓ Loaded {len(result)} system_config document(s)")
            return result

        except Exception as e:
            print(f"Error in get_system_config: {e}")
            import traceback
            traceback.print_exc()
            return self._get_mock_config()

    @staticmethod
    def _get_mock_config():
        """Fallback mock config — mirrors the real schema so the UI always renders."""
        now = datetime.utcnow().isoformat()
        return [
            {
                "id": "mock-scraper-config",
                "type": "scraper_config",
                "version": 1,
                "is_active": True,
                "updated_at": now,
                "data": {
                    "global_settings": {"default_results_limit": 20},
                    "providers": {
                        "apify": {
                            "platforms": {
                                "tiktok": {
                                    "actor_id": "clockworks/tiktok-profile-scraper",
                                    "url_template": "https://www.tiktok.com/@{username}",
                                    "results_limit": 5
                                },
                                "instagram": {
                                    "actor_id": "apify/instagram-profile-scraper",
                                    "url_template": "https://www.instagram.com/{username}/",
                                    "results_limit": 5
                                },
                                "facebook": {
                                    "actor_id": "apify/facebook-posts-scraper",
                                    "url_template": "https://www.facebook.com/{username}/",
                                    "results_limit": 5
                                }
                            }
                        },
                        "brightdata": {
                            "platforms": {
                                "tiktok": {
                                    "dataset_id": "gd_l1villgoiiidt09ci",
                                    "url_template": "https://www.tiktok.com/@{username}",
                                    "results_limit": 15
                                },
                                "instagram": {
                                    "dataset_id": "gd_l1vikfch901nx3by4",
                                    "url_template": "https://www.instagram.com/{username}/",
                                    "results_limit": 20
                                },
                                "facebook": {
                                    "dataset_id": "gd_lkaxegm826bjpoo9m5",
                                    "url_template": "https://www.facebook.com/{username}/",
                                    "results_limit": 25
                                }
                            }
                        }
                    }
                }
            },
            {
                "id": "mock-schedule-config",
                "type": "schedule_config",
                "version": 1,
                "is_active": True,
                "updated_at": now,
                "data": {
                    "global_scrape_interval_hours": 6,
                    "per_platform_overrides": {
                        "tiktok": 6,
                        "instagram": 6,
                        "facebook": 6
                    }
                }
            },
            {
                "id": "mock-llm-config",
                "type": "llm_config",
                "version": 1,
                "is_active": True,
                "updated_at": now,
                "data": {
                    "default_provider": "openai",
                    "providers": {
                        "openai": {
                            "model": "gpt-4o",
                            "generation_config": {
                                "temperature": 0.2,
                                "top_p": 1,
                                "max_output_tokens": 800
                            },
                            "normalizer": {
                                "model": "gpt-4o-mini",
                                "generation_config": {
                                    "temperature": 0.1,
                                    "top_p": 1,
                                    "max_output_tokens": 600
                                }
                            }
                        },
                        "groq": {
                            "model": "llama-3.3-70b-versatile",
                            "generation_config": {
                                "temperature": 0.2,
                                "top_p": 1,
                                "max_output_tokens": 700
                            },
                            "normalizer": {
                                "model": "llama-3.3-70b-versatile",
                                "generation_config": {
                                    "temperature": 0.1,
                                    "top_p": 1,
                                    "max_output_tokens": 800
                                }
                            }
                        }
                    }
                }
            }
        ]





    # =========================================================================

    @staticmethod
    def _format_bytes(bytes_value):
        """Format bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.2f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.2f} TB"


# Initialize service on module import
try:
    dashboard_service = DashboardService()
except Exception as e:
    print(f"Failed to initialize dashboard service: {e}")
    dashboard_service = None


# ============================================================================
# API ENDPOINTS - RESTRUCTURED FOR TAB-BASED DASHBOARD
# ============================================================================

@router.get("/stats/overview")
async def get_overview_stats(
        time_range: str = Query("24h", pattern="^(24h|7d|30d)$"),
        profile_limit: int = Query(10, ge=1, le=200)
):
    """Get high-level overview statistics"""
    try:
        if dashboard_service is None:
            mock = DashboardService(None)
            return mock.get_mock_stats()

        stats = dashboard_service.get_stats(time_range)
        recent_activity = dashboard_service.get_recent_activity()
        profile_performance = dashboard_service.get_profile_performance(time_range, limit=profile_limit)
        total_profiles = dashboard_service.get_total_profiles_in_range(time_range)

        failed_scrapes = dashboard_service.get_failed_scrapes_count(time_range)
        inactive_offers = dashboard_service.get_inactive_offers_count(time_range)
        stale_profiles = dashboard_service.get_stale_profiles(hours=24)

        ai_metrics = dashboard_service.get_ai_extraction_metrics(time_range)

        return {
            **stats,
            "recentActivity": recent_activity,
            "profilePerformance": profile_performance,
            "totalProfiles": total_profiles,
            "alerts": {
                "failedScrapesCount": failed_scrapes["total"],
                "inactiveOffersCount": inactive_offers["total"],
                "staleProfilesCount": stale_profiles["count"]
            },
            "aiMetrics": ai_metrics
        }
    except Exception as e:
        print(f"Error in get_overview_stats: {e}")
        import traceback
        traceback.print_exc()
        mock = DashboardService(None)
        return mock.get_mock_stats()

@router.get("/stats/scraping")
async def get_scraping_stats(
        time_range: str = Query("24h", pattern="^(24h|7d|30d)$"),
        profile_limit: int = Query(10, ge=1)
):
    """Get scraping-related statistics and metrics — Scraping Analytics Tab"""
    try:
        if dashboard_service is None:
            mock = DashboardService(None)
            return {
                "byPlatform": mock.get_mock_stats()["byPlatform"],
                "byProfile": mock.get_mock_stats()["byProfile"],
                "recentActivity": mock.get_mock_stats()["recentActivity"],
                "profileSuccessRate": {},
                "incompletePosts": {"missing_images": [], "missing_video": [], "total": 0}
            }

        by_platform = dashboard_service.get_by_platform(time_range)
        by_profile = dashboard_service.get_by_profile(time_range, limit=profile_limit)
        recent_activity = dashboard_service.get_recent_activity()
        profile_success_rate = dashboard_service.get_profile_success_rate(time_range, limit=profile_limit)
        incomplete_posts = dashboard_service.get_incomplete_posts(time_range)

        return {
            "byPlatform": by_platform,
            "byProfile": by_profile,
            "recentActivity": recent_activity,
            "profileSuccessRate": profile_success_rate,
            "incompletePosts": incomplete_posts
        }
    except Exception as e:
        print(f"Error in get_scraping_stats: {e}")
        import traceback
        traceback.print_exc()
        return {
            "byPlatform": {},
            "byProfile": {},
            "recentActivity": [],
            "profileSuccessRate": {},
            "incompletePosts": {"text_only": [], "image_only": [], "video_only": [], "total": 0}
        }


@router.get("/stats/offers")
async def get_offers_stats(
        time_range: str = Query("24h", pattern="^(24h|7d|30d)$"),
):
    """Get offer extraction and analysis statistics — Offer Intelligence Tab"""
    try:
        if dashboard_service is None:
            mock = DashboardService(None)
            return {
                "offersByCategory": mock.get_mock_stats()["offersByCategory"],
                "topBrands": {},
                "offersByCountry": {},
                "discountTypesDistribution": {},
                "promoCodeUsage": {"with_promo_code": 0, "without_promo_code": 0, "percentage_with_code": 0},
                "avgDiscountValue": {"overall": 0, "by_currency": {}},
                "offerTypeBreakdown": {},
            }

        return {
            "offersByCategory": dashboard_service.get_offers_by_category(time_range),
            "topBrands": dashboard_service.get_top_brands(time_range, limit=50),
            "offersByCountry": dashboard_service.get_offers_by_country(time_range),
            "discountTypesDistribution": dashboard_service.get_discount_types_distribution(time_range),
            "promoCodeUsage": dashboard_service.get_promo_code_usage(time_range),
            "avgDiscountValue": dashboard_service.get_average_discount_value(time_range),
            "offerTypeBreakdown": dashboard_service.get_offer_type_breakdown(time_range),
        }
    except Exception as e:
        print(f"Error in get_offers_stats: {e}")
        import traceback
        traceback.print_exc()
        return {
            "offersByCategory": {},
            "topBrands": {},
            "offersByCountry": {},
            "discountTypesDistribution": {},
            "promoCodeUsage": {"with_promo_code": 0, "without_promo_code": 0, "percentage_with_code": 0},
            "avgDiscountValue": {"overall": 0, "by_currency": {}},
            "offerTypeBreakdown": {},
        }


# ============================================================================
# NEW ENDPOINT: /stats/config
# ============================================================================

@router.get("/stats/config")
async def get_config_stats():
    """
    Fetch all documents from social_scraper.system_config.

    Returns a JSON array of config objects, each with:
      - id        : string (Mongo ObjectId)
      - type      : scraper_config | schedule_config | llm_config
      - version   : int
      - is_active : bool
      - data      : the nested config payload
      - updated_at: ISO-8601 string

    Falls back to built-in mock data when MongoDB is unavailable
    so the Config tab always renders correctly.

    Used by: Configuration Tab
    """
    try:
        if dashboard_service is None:
            return DashboardService._get_mock_config()

        return dashboard_service.get_system_config()

    except Exception as e:
        print(f"Error in get_config_stats: {e}")
        import traceback
        traceback.print_exc()
        return DashboardService._get_mock_config()


# ============================================================================

@router.get("/max-profiles")
async def get_max_profiles():
    """Get the total number of profiles for dynamic limit"""
    try:
        if dashboard_service is None:
            return {"maxProfiles": 50}

        max_profiles = dashboard_service.get_total_profiles_count()
        return {"maxProfiles": max_profiles}
    except Exception as e:
        print(f"Error in get_max_profiles: {e}")
        return {"maxProfiles": 50}


@router.get("/database-stats")
async def get_database_statistics():
    """Get MongoDB database and collection statistics"""
    try:
        if dashboard_service is None:
            return {
                "raw_social_data": {"count": 0, "size": "0 B", "avgObjSize": "0 B"},
                "offers": {"count": 0, "validCount": 0, "size": "0 B", "avgObjSize": "0 B"}
            }

        return dashboard_service.get_database_stats()
    except Exception as e:
        print(f"Error in get_database_statistics: {e}")
        return {
            "raw_social_data": {"count": 0, "size": "0 B", "avgObjSize": "0 B"},
            "offers": {"count": 0, "validCount": 0, "size": "0 B", "avgObjSize": "0 B"}
        }


@router.get("/health")
async def get_system_health():
    """Check system health status"""
    try:
        if dashboard_service is None or not dashboard_service.connected:
            return {
                "mongodb": {"status": "disconnected", "healthy": False},
                "collections": {"status": "unavailable", "healthy": False},
                "lastSync": None,
                "timestamp": datetime.utcnow().isoformat(),
                "mode": "mock_data"
            }

        dashboard_service.client.server_info()

        collections = dashboard_service.social_scraper_db.list_collection_names()
        has_collections = "raw_social_data" in collections

        last_post = dashboard_service.raw_data_collection.find_one(
            sort=[("scraped_at", pymongo.DESCENDING)]
        )

        last_sync = last_post["scraped_at"].isoformat() if last_post else None

        return {
            "mongodb": {"status": "connected", "healthy": True},
            "collections": {
                "status": "ready" if has_collections else "missing",
                "healthy": has_collections
            },
            "lastSync": last_sync,
            "timestamp": datetime.utcnow().isoformat(),
            "mode": "live_data"
        }
    except Exception as e:
        print(f"Health check error: {e}")
        return {
            "mongodb": {"status": "error", "healthy": False},
            "collections": {"status": "error", "healthy": False},
            "lastSync": None,
            "timestamp": datetime.utcnow().isoformat(),
            "mode": "error",
            "error": str(e)
        }