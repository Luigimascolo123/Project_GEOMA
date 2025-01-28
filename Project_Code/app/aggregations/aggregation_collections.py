import logging
from datetime import datetime
from pymongo import MongoClient

class AggregationsManager:
    def __init__(self, mongodb_host='mongodb', mongodb_port='27017'):
        self.logger = logging.getLogger(__name__)
        
        # Inizializziamo MongoDB client
        self.mongo_client = MongoClient(f"mongodb://{mongodb_host}:{mongodb_port}/")
        self.db = self.mongo_client.gdelt_db
        self.openmeasures_db = self.mongo_client.openmeasures_db

    def create_daily_insights(self):
        """Crea la collection daily_insights"""
        try:
            pipeline = [
                {
                    "$addFields": {
                        "Date": {
                            "$toDate": {
                                "$concat": [
                                    {"$substr": ["$SQLDATE", 0, 4]},
                                    "-",
                                    {"$substr": ["$SQLDATE", 4, 2]},
                                    "-",
                                    {"$substr": ["$SQLDATE", 6, 2]}
                                ]
                            }
                        }
                    }
                },
                {
                    "$group": {
                        "_id": "$Date",
                        "AvgImpact": {"$avg": "$GoldsteinScale"},
                        "AvgTone": {"$avg": "$AvgTone"},
                        "NumMentions": {"$sum": 1}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "Date": {
                            "$dateToString": {
                                "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                                "date": "$_id"
                            }
                        },
                        "AvgImpact": 1,
                        "AvgTone": 1,
                        "NumMentions": 1
                    }
                },
                {
                    "$sort": {
                        "Date": 1
                    }
                }
            ]

            # Rimuoviamo la collezione esistente se presente
            if "daily_insights" in self.db.list_collection_names():
                self.db.daily_insights.drop()

            # Eseguiamo l'aggregazione e inserisci i risultati
            daily_insights = self.db.events.aggregate(pipeline)
            self.db.daily_insights.insert_many(daily_insights)
            
            # Creiamo un indice sulla data per ottimizzare le query
            self.db.daily_insights.create_index("Date")

            self.logger.info("Daily insights collection created successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error creating daily insights: {str(e)}")
            return False

    def create_geo_analysis(self):
        """Crea la collection geo_analysis"""
        try:
            pipeline = [
                # Filtriamo i record con coordinate valide
                {
                    "$match": {
                        "ActionGeo_Lat": {"$ne": None},
                        "ActionGeo_Long": {"$ne": None}
                    }
                },
                # Aggiungiamo il campo Date formattato
                {
                    "$addFields": {
                        "Date": {
                            "$dateToString": {
                                "format": "%Y-%m-%dT00:00:00.000Z",
                                "date": {
                                    "$toDate": {
                                        "$concat": [
                                            {"$substr": ["$SQLDATE", 0, 4]},
                                            "-",
                                            {"$substr": ["$SQLDATE", 4, 2]},
                                            "-",
                                            {"$substr": ["$SQLDATE", 6, 2]}
                                        ]
                                    }
                                }
                            }
                        }
                    }
                },
                # Raggruppiamo per coordinate
                {
                    "$group": {
                        "_id": {
                            "lat": "$ActionGeo_Lat",
                            "long": "$ActionGeo_Long"
                        },
                        "AvgImpact": {"$avg": "$GoldsteinScale"},
                        "NumMentions": {"$sum": 1},
                        "EventCode": {"$first": "$EventCode"},
                        "SOURCEURL": {"$first": "$SOURCEURL"},
                        "Date": {"$first": "$Date"}  # Manteniamo la data per il filtraggio
                    }
                },
                # Proiettiamo i campi nel formato richiesto
                {
                    "$project": {
                        "_id": 0,
                        "ActionGeo_Lat": "$_id.lat",
                        "ActionGeo_Long": "$_id.long",
                        "AvgImpact": 1,
                        "NumMentions": 1,
                        "EventCode": 1,
                        "SOURCEURL": 1,
                        "Date": 1
                    }
                },
                # Ordiniamo per NumMentions in ordine decrescente
                {
                    "$sort": {
                        "NumMentions": -1
                    }
                }
            ]

            # Rimuoviamo la collezione esistente se presente
            if "geo_analysis" in self.db.list_collection_names():
                self.db.geo_analysis.drop()

            # Eseguiamo l'aggregazione e inserisci i risultati
            geo_analysis = self.db.events.aggregate(pipeline)
            result_list = list(geo_analysis)
            
            if result_list:
                self.db.geo_analysis.insert_many(result_list)
                
                # Creiamo indici per ottimizzare le query
                self.db.geo_analysis.create_index("Date")
                self.db.geo_analysis.create_index([("ActionGeo_Lat", 1), ("ActionGeo_Long", 1)])
                self.db.geo_analysis.create_index([("NumMentions", -1)])  # Indice per l'ordinamento

                self.logger.info(f"Geo analysis collection created successfully with {len(result_list)} documents")
                return True
            else:
                self.logger.warning("No data to insert in geo analysis")
                return False

        except Exception as e:
            self.logger.error(f"Error creating geo analysis: {str(e)}")
            return False

    def create_trend_analysis(self):
        """Crea la collection trend_analysis"""
        try:
            pipeline = [
                # Proiettiamo e convertiamo la data
                {
                    "$project": {
                        "Date": {
                            "$toDate": {
                                "$concat": [
                                    {"$substr": ["$SQLDATE", 0, 4]},
                                    "-",
                                    {"$substr": ["$SQLDATE", 4, 2]},
                                    "-",
                                    {"$substr": ["$SQLDATE", 6, 2]}
                                ]
                            }
                        },
                        "NumMentions": 1
                    }
                },
                # Raggruppiamo per data
                {
                    "$group": {
                        "_id": "$Date",
                        "NumMentions": {"$sum": "$NumMentions"}
                    }
                },
                # Proiettiamo il formato finale con data serializzabile
                {
                    "$project": {
                        "_id": 0,
                        "Date": {
                            "$dateToString": {
                                "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                                "date": "$_id"
                            }
                        },
                        "NumMentions": 1
                    }
                },
                # Ordiniamo per data
                {
                    "$sort": {
                        "Date": 1
                    }
                }
            ]

            # Rimuoviamo la collezione esistente se presente
            if "trend_analysis" in self.db.list_collection_names():
                self.db.trend_analysis.drop()

            # Eseguiamo l'aggregazione e inserisci i risultati
            trend_analysis = self.db.events.aggregate(pipeline)
            self.db.trend_analysis.insert_many(trend_analysis)
            
            # Creiamo un indice sulla data per ottimizzare le query
            self.db.trend_analysis.create_index("Date")

            self.logger.info("Trend analysis collection created successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error creating trend analysis: {str(e)}")
            return False

    def create_top_mentions(self, keyword_extractor):
        """Crea la collection top_mentions"""
        try:
            top_mentions_pipeline = [
                {
                    "$addFields": {
                        "TotalMentions": {
                            "$add": ["$NumMentions", "$NumSources", "$NumArticles"]
                        }
                    }
                },
                {
                    "$sort": {"TotalMentions": -1}
                },
                {
                    "$limit": 10
                },
                {
                    "$project": {
                        "_id": 0,
                        "SourceURL": "$SOURCEURL",
                        "EventRootCode": "$EventRootCode",
                        "Date": {
                            "$dateFromString": {
                                "dateString": "$SQLDATE",
                                "format": "%Y%m%d"
                            }
                        },
                        "TotalMentions": 1,
                        "Location": {
                            "Latitude": "$ActionGeo_Lat",
                            "Longitude": "$ActionGeo_Long"
                        },
                        "NumMentions": 1,
                        "NumSources": 1,
                        "NumArticles": 1
                    }
                }
            ]

            # Execute the aggregation
            top_mentions = list(self.db.events.aggregate(top_mentions_pipeline))

            # Process each top mention to extract keywords
            for mention in top_mentions:
                mention['Keywords'] = keyword_extractor.extract_keywords(mention.get('SourceURL', ''))

            # Clear existing top_mentions collection
            self.db.top_mentions.delete_many({})
            
            # Insert new top mentions
            if top_mentions:
                self.db.top_mentions.insert_many(top_mentions)
                self.logger.info(f"Top mentions collection created successfully. {len(top_mentions)} documents inserted.")
            else:
                self.logger.warning("No top mentions found to insert.")

            return True
        except Exception as e:
            self.logger.error(f"Error creating top mentions collection: {str(e)}")
            return False


    def get_top_mentions_keywords(self, keyword_extractor):
        """Estrae le keywords dai top URL senza creare una collection"""
        try:
            top_mentions_pipeline = [
                {
                    "$addFields": {
                        "TotalMentions": {
                            "$add": ["$NumMentions", "$NumSources", "$NumArticles"]
                        }
                    }
                },
                {
                    "$sort": {"TotalMentions": -1}
                },
                {
                    "$limit": 20
                },
                {
                    "$project": {
                        "_id": 0,
                        "SourceURL": "$SOURCEURL"
                    }
                }
            ]

            top_mentions = list(self.db.events.aggregate(top_mentions_pipeline))
            all_keywords = []
            
            """
            # Raccoglie tutte le keywords
            for mention in top_mentions:
                keywords = keyword_extractor.extract_keywords(mention.get('SourceURL', ''))
                all_keywords.extend(keywords)
            """
            
            # Raccogliamo tutte le keywords
            for mention in top_mentions:
                url = mention.get('SourceURL', '')
                keywords = keyword_extractor.extract_keywords(url)
                self.logger.info(f"\nURL: {url}")
                self.logger.info(f"Keywords estratte: {keywords}")
                all_keywords.extend(keywords)

            
            # Contiamo le occorrenze
            keyword_counts = {}
            for keyword in all_keywords:
                keyword_counts[keyword] = keyword_counts.get(keyword, 0) + 1
            
            # Ordiniamo per frequenza
            sorted_keywords = dict(sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True))

            
            self.logger.info("Keywords frequency:")
            for word, count in sorted_keywords.items():
                self.logger.info(f"{word}: {count}")
            
            return sorted_keywords

        except Exception as e:
            self.logger.error(f"Error getting top mentions keywords: {str(e)}")
            return []

### Collezioni Open measures

    def create_social_temporal_metrics(self):
        """Create temporal metrics for social media engagement"""
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": {
                            "date": {"$dateToString": {"format": "%Y-%m-%d", "date": {"$toDate": "$created_at"}}}
                        },
                        "totalPosts": {"$sum": 1},
                        "totalEngagement": {"$sum": "$total_engagement"},
                        "totalLikes": {"$sum": "$likes_count"},
                        "totalShares": {"$sum": "$shares_count"},
                        "totalComments": {"$sum": "$comments_count"},
                        "totalViews": {"$sum": {"$ifNull": ["$views_count", 0]}},
                        "uniqueAuthors": {"$addToSet": "$author_id"},
                        "sponsoredCount": {"$sum": {"$cond": [{"$eq": ["$is_sponsored", True]}, 1, 0]}}
                    }
                },
                {
                    "$project": {
                        "date": "$_id.date",
                        "metrics": {
                            "posts": "$totalPosts",
                            "engagement": "$totalEngagement",
                            "likes": "$totalLikes",
                            "shares": "$totalShares",
                            "comments": "$totalComments",
                            "views": "$totalViews",
                            "uniqueAuthors": {"$size": "$uniqueAuthors"},
                            "sponsoredPercentage": {
                                "$multiply": [
                                    {"$divide": ["$sponsoredCount", "$totalPosts"]},
                                    100
                                ]
                            }
                        }
                    }
                },
                {"$sort": {"date": 1}}
            ]

            if "social_temporal_metrics" in self.openmeasures_db.list_collection_names():
                self.openmeasures_db.social_temporal_metrics.drop()

            result = self.openmeasures_db.Openmeasures_data.aggregate(pipeline)
            self.openmeasures_db.social_temporal_metrics.insert_many(result)
            self.logger.info("Successfully created social_temporal_metrics collection")
            return True
        except Exception as e:
            self.logger.error(f"Error creating social_temporal_metrics: {str(e)}")
            return False

    def create_top_authors_engagement(self):
        """Crea la collection top_authors_engagement"""
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": "$author_id",
                        "totalEngagement": {"$sum": "$total_engagement"},
                        "authorName": {"$first": "$author_name"},
                        "created_at": {"$first": "$created_at"}  # Manteniamo la data per il filtraggio
                    }
                },
                {
                    "$sort": {"totalEngagement": -1}
                },
                {
                    "$limit": 10
                },
                {
                    "$project": {
                        "_id": 0,
                        "author": "$authorName",
                        "engagement": "$totalEngagement",
                        "Date": {  
                            "$dateToString": {
                                "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                                "date": {"$toDate": "$created_at"}
                            }
                        }
                    }
                }
            ]

            # Rimuoviamo la collezione esistente se presente
            if "top_authors_engagement" in self.openmeasures_db.list_collection_names():
                self.openmeasures_db.top_authors_engagement.drop()

            # Eseguiamo l'aggregazione e inserisci i risultati
            top_authors = self.openmeasures_db.Openmeasures_data.aggregate(pipeline)
            self.openmeasures_db.top_authors_engagement.insert_many(top_authors)
            
            # Creiamo indici per ottimizzare le query
            self.openmeasures_db.top_authors_engagement.create_index("Date")
            self.openmeasures_db.top_authors_engagement.create_index([("engagement", -1)])

            self.logger.info("Top authors engagement collection created successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error creating top authors engagement: {str(e)}")
            return False
    
    def create_gdelt_social_correlation(self):
        """Crea la collection gdelt_social_correlation con il totale giornaliero di engagement e numMentions"""
        try:
            # Pipeline GDELT per il totale giornaliero di NumMentions
            gdelt_pipeline = [
                {
                    "$addFields": {
                        "Date": {
                            "$dateToString": {
                                "format": "%Y-%m-%dT00:00:00.000Z",
                                "date": {
                                    "$toDate": {
                                        "$concat": [
                                            {"$substr": ["$SQLDATE", 0, 4]}, "-",
                                            {"$substr": ["$SQLDATE", 4, 2]}, "-",
                                            {"$substr": ["$SQLDATE", 6, 2]}
                                        ]
                                    }
                                }
                            }
                        }
                    }
                },
                {
                    "$group": {
                        "_id": "$Date",
                        "total_mentions": {"$sum": "$NumMentions"}
                    }
                },
                {
                    "$sort": {"_id": 1}
                }
            ]

            # Pipeline OpenMeasures per il totale giornaliero di engagement
            openmeasures_pipeline = [
                {
                    "$addFields": {
                        "Date": {
                            "$dateToString": {
                                "format": "%Y-%m-%dT00:00:00.000Z",
                                "date": {"$toDate": "$created_at"}
                            }
                        }
                    }
                },
                {
                    "$group": {
                        "_id": "$Date",
                        "total_engagement": {"$sum": "$total_engagement"},
                    }
                },
                {
                    "$sort": {"_id": 1}
                }
            ]

            # Eseguiamo le pipeline
            gdelt_results = list(self.db.events.aggregate(gdelt_pipeline))
            openmeasures_results = list(self.openmeasures_db.Openmeasures_data.aggregate(openmeasures_pipeline))

            # Combiniamo i risultati per data
            combined_results = []
            for gdelt in gdelt_results:
                date = gdelt["_id"]
                social = next((s for s in openmeasures_results if s["_id"] == date), None)
                
                if social:
                    # Prima di inserire, controlla se esiste già un documento per questa data
                    existing_doc = self.openmeasures_db.gdelt_social_correlation.find_one({"Date": date})
                    if existing_doc:
                        # Se esiste, eliminiamolo
                        self.openmeasures_db.gdelt_social_correlation.delete_one({"Date": date})

                    # Inseriamo il nuovo documento
                    new_doc = {
                        "Date": date,
                        "mentions": gdelt["total_mentions"],
                        "engagement": social["total_engagement"]
                    }
                    combined_results.append(new_doc)

            # Se ci sono risultati, inserisci nella collezione
            if combined_results:
                self.openmeasures_db.gdelt_social_correlation.insert_many(combined_results)
                self.openmeasures_db.gdelt_social_correlation.create_index("Date")
                self.logger.info("Successfully created gdelt_social_correlation collection")
                return True
            else:
                self.logger.warning("No matching days found between GDELT and OpenMeasures data")
                return False

        except Exception as e:
            self.logger.error(f"Error creating gdelt_social_correlation: {str(e)}")
            return False
    
    def create_all_collections(self, keyword_extractor):
        """Crea tutte le collections aggregate"""
        self.logger.info("Starting creation of all aggregated collections...")
        
        results = {
            ### GDELT collections
            'daily_insights': self.create_daily_insights(),
            'geo_analysis': self.create_geo_analysis(),
            'trend_analysis': self.create_trend_analysis(),
            'top_mentions': self.create_top_mentions(keyword_extractor),
            ### Openmeasures collections
            'social_temporal_metrics': self.create_social_temporal_metrics(),
            'top_authors_engagement': self.create_top_authors_engagement(),
            ### Gdelt-socials correlation
            'gdelt_social_correlation': self.create_gdelt_social_correlation()
        }
        
        # Estrai le keywords
        keywords = self.get_top_mentions_keywords(keyword_extractor)
        self.logger.info(f"Extracted keywords: {keywords}")
        
        success = all(results.values())
        if success:
            self.logger.info("All collections created successfully")
        else:
            failed = [k for k, v in results.items() if not v]
            self.logger.warning(f"Failed to create collections: {failed}")
            
        return success, keywords

    def close(self):
        """Chiude la connessione MongoDB"""
        try:
            if self.mongo_client:
                self.mongo_client.close()
        except Exception as e:
            self.logger.error(f"Error closing MongoDB connection: {str(e)}")