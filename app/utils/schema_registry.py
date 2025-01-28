from pyspark.sql.types import *
from typing import Dict

# Schema registry class --> In base al social utilizza uno schema diverso 
class SchemaRegistry:
    """Registry class to manage schemas for different social media platforms"""
    
    @staticmethod
    def get_tiktok_video_schema() -> StructType:
        """Schema definition for TikTok video data"""
        author_schema = StructType([
            StructField("author", StringType(), True),
            StructField("author_id", StringType(), True)
        ])
        
        challenge_schema = StructType([
            StructField("id", StringType(), True),
            StructField("title", StringType(), True)
        ])
        
        music_schema = StructType([
            StructField("coverMediumUrl", StringType(), True),
            StructField("musicAlbum", StringType(), True),
            StructField("musicAuthor", StringType(), True),
            StructField("musicId", StringType(), True),
            StructField("musicName", StringType(), True),
            StructField("musicOriginal", BooleanType(), True),
            StructField("playUrl", StringType(), True)
        ])
        
        stats_schema = StructType([
            StructField("collectCount", LongType(), True),
            StructField("commentCount", LongType(), True),
            StructField("diggCount", LongType(), True),
            StructField("playCount", LongType(), True),
            StructField("shareCount", LongType(), True)
        ])
        
        subtitle_links_schema = StructType([
            StructField("downloadLink", StringType(), True),
            StructField("language", StringType(), True),
            StructField("tiktokLink", StringType(), True)
        ])
        
        source_schema = StructType([
            StructField("author", StringType(), True),
            StructField("author_id", StringType(), True),
            StructField("challenges", ArrayType(challenge_schema), True),
            StructField("collected_by", StringType(), True),
            StructField("coverUrl", StringType(), True),
            StructField("createTime", LongType(), True),
            StructField("datatype", StringType(), True),
            StructField("definition", StringType(), True),
            StructField("desc", StringType(), True),
            StructField("duetEnabled", BooleanType(), True),
            StructField("duration", IntegerType(), True),
            StructField("format", StringType(), True),
            StructField("id", StringType(), True),
            StructField("isAd", BooleanType(), True),
            StructField("itemCommentStatus", StringType(), True),
            StructField("itemMute", BooleanType(), True),
            StructField("lastseents", StringType(), True),
            StructField("music", music_schema, True),
            StructField("officialItem", BooleanType(), True),
            StructField("originalCoverUrl", StringType(), True),
            StructField("originalItem", BooleanType(), True),
            StructField("privateobj", BooleanType(), True),
            StructField("secret", BooleanType(), True),
            StructField("shareEnabled", BooleanType(), True),
            StructField("showNotPass", BooleanType(), True),
            StructField("stats", stats_schema, True),
            StructField("stitchEnabled", BooleanType(), True),
            StructField("subtitleLinks", ArrayType(subtitle_links_schema), True)
        ])
        
        hit_schema = StructType([
            StructField("_index", StringType(), True),
            StructField("_id", StringType(), True),
            StructField("_score", StringType(), True),
            StructField("_source", source_schema, True),
            StructField("sort", ArrayType(LongType()), True)
        ])
        
        return StructType([
            StructField("hits", StructType([
                StructField("total", StructType([
                    StructField("value", LongType(), True),
                    StructField("relation", StringType(), True)
                ]), True),
                StructField("max_score", StringType(), True),
                StructField("hits", ArrayType(hit_schema), True)
            ]), True)
        ])
    
    @staticmethod
    def get_truth_social_schema() -> StructType:
        """Schema definition for Truth Social data"""
        source_schema = StructType([
            StructField("account", StructType([
                StructField("acct", StringType(), True),
                StructField("display_name", StringType(), True),
                StructField("id", StringType(), True),
                StructField("username", StringType(), True)
            ]), True),
            StructField("card", StructType([
                StructField("author_name", StringType(), True),
                StructField("author_url", StringType(), True),
                StructField("blurhash", StringType(), True),
                StructField("description", StringType(), True),
                StructField("title", StringType(), True),
                StructField("url", StringType(), True),
                StructField("image", StringType(), True),
                StructField("embed_url", StringType(), True),
                StructField("html", StringType(), True),
                StructField("provider_name", StringType(), True),
                StructField("provider_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("height", IntegerType(), True),
                StructField("width", IntegerType(), True),
                StructField("group", StringType(), True),
                StructField("id", StringType(), True),
                StructField("links", StringType(), True)
            ]), True),
            StructField("content_cleaned", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("id", StringType(), True),
            StructField("language", StringType(), True),
            StructField("favourites_count", LongType(), True),
            StructField("reblogs_count", LongType(), True),
            StructField("replies_count", LongType(), True),
            StructField("visibility", StringType(), True),
            StructField("uri", StringType(), True),
            StructField("url", StringType(), True),
            StructField("lastseents", StringType(), True),
            StructField("tags", ArrayType(StructType([
                StructField("name", StringType(), True),
                StructField("url", StringType(), True)
            ]), True), True),
            StructField("mentions", ArrayType(StructType([
                StructField("acct", StringType(), True),
                StructField("id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("username", StringType(), True)
            ]), True), True),
            StructField("media_attachments", ArrayType(StructType([
                StructField("url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("preview_url", StringType(), True),
                StructField("remote_url", StringType(), True),
                StructField("text_url", StringType(), True),
                StructField("description", StringType(), True),
                StructField("blurhash", StringType(), True),
                StructField("meta", StructType([
                    StructField("aspect", DoubleType(), True),
                    StructField("height", IntegerType(), True),
                    StructField("width", IntegerType(), True),
                    StructField("size", StringType(), True),
                    StructField("original", StructType([
                        StructField("aspect", DoubleType(), True),
                        StructField("height", IntegerType(), True),
                        StructField("width", IntegerType(), True),
                        StructField("size", StringType(), True)
                    ]), True),
                    StructField("small", StructType([
                        StructField("aspect", DoubleType(), True),
                        StructField("height", IntegerType(), True),
                        StructField("width", IntegerType(), True),
                        StructField("size", StringType(), True)
                    ]), True)
                ]), True),
                StructField("id", StringType(), True)
            ]), True), True),
            StructField("collected_by", StringType(), True),
            StructField("datatype", StringType(), True),
            StructField("in_reply_to_account_id", StringType(), True),
            StructField("in_reply_to_id", StringType(), True),
            StructField("bookmarked", BooleanType(), True),
            StructField("favourited", BooleanType(), True),
            StructField("muted", BooleanType(), True),
            StructField("pinned", BooleanType(), True),
            StructField("reblogged", BooleanType(), True),
            StructField("sensitive", BooleanType(), True),
            StructField("sponsored", BooleanType(), True),
            StructField("spoiler_text", StringType(), True),
            StructField("group", StructType([
                StructField("display_name", StringType(), True),
                StructField("id", StringType(), True),
                StructField("slug", StringType(), True)
            ]), True)
        ])
        
        hit_schema = StructType([
            StructField("_index", StringType(), True),
            StructField("_id", StringType(), True),
            StructField("_source", source_schema, True)
        ])
        
        return StructType([
            StructField("hits", StructType([
                StructField("hits", ArrayType(hit_schema), True)
            ]), True)
        ])

    @staticmethod
    def get_vk_schema() -> StructType:
        """Schema definition for VK data"""
        size_schema = StructType([
            StructField("width", IntegerType(), True),
            StructField("type", StringType(), True),
            StructField("url", StringType(), True),
            StructField("height", IntegerType(), True)
        ])

        # Schema per attachments
        attachment_schema = StructType([
            StructField("type", StringType(), True),
            StructField("link", StructType([
                StructField("url", StringType(), True),
                StructField("title", StringType(), True),
                StructField("description", StringType(), True),
                StructField("photo", StructType([
                    StructField("sizes", ArrayType(size_schema), True)
                ]), True)
            ]), True)
        ])

        # Schema principale per i dati source di VK
        source_schema = StructType([
            StructField("attachments", ArrayType(attachment_schema), True),
            StructField("author", StringType(), True),
            StructField("collected_by", StringType(), True),
            StructField("comments", StructType([
                StructField("count", LongType(), True)
            ]), True),
            StructField("datatype", StringType(), True),
            StructField("from_id", StringType(), True),
            StructField("date", LongType(), True),
            StructField("id", StringType(), True),
            StructField("likes", StructType([
                StructField("count", LongType(), True)
            ]), True),
            StructField("post_source", StructType([
                StructField("type", LongType(), True)
            ]), True),
            StructField("marked_as_ads", IntegerType(), True),
            StructField("reposts", StructType([
                StructField("count", LongType(), True)
            ]), True),
            StructField("text", StringType(), True),
            StructField("type", StringType(), True),
            StructField("views", StructType([
                StructField("count", LongType(), True)
            ]), True),
            StructField("wall_owner", StringType(), True),
            StructField("lastseents", StringType(), True)
        ])

        hit_schema = StructType([
            StructField("_index", StringType(), True),
            StructField("_id", StringType(), True),
            StructField("_source", source_schema, True)
        ])
        
        return StructType([
            StructField("hits", StructType([
                StructField("hits", ArrayType(hit_schema), True)
            ]), True)
        ])

    # Mapping tra social media e schema
    SCHEMA_REGISTRY: Dict[str, callable] = {
        "truth_social": get_truth_social_schema,
        "tiktok_video": get_tiktok_video_schema,
        "vk": get_vk_schema
    }
    
    @classmethod
    def get_schema(cls, social: str) -> StructType:
        social = social.lower()
        if social not in cls.SCHEMA_REGISTRY:
            raise ValueError(f"Unsupported social media platform: {social}. "
                           f"Supported platforms are: {list(cls.SCHEMA_REGISTRY.keys())}")
        
        return cls.SCHEMA_REGISTRY[social]()