from sanic import Sanic
from sanic.response import json as sanic_json
from sanic.request import Request
from sanic.response import HTTPResponse
from bson import ObjectId  
import json
from datetime import datetime
import logging
import motor.motor_asyncio
import math

# Configura il logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("grafana_api")

# Crea l'applicazione Sanic
grafana = Sanic("grafana_api")

# Configurazione MongoDB
mongo_client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://mongodb:27017") 

# Database in MongoDB
mongo_openmeasures = mongo_client["openmeasures_db"]
mongo_gdelt = mongo_client["gdelt_db"]

# Funzione per serializzare i dati di MongoDB (incluso ObjectId, datetime e valori NaN)

def mongo_to_dict(obj):
    """Converte gli oggetti BSON in dizionari serializzabili JSON."""
    if isinstance(obj, ObjectId):
        return str(obj)  
    elif isinstance(obj, datetime):
        return obj.isoformat()  
    elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None  
    if isinstance(obj, dict):
        return {key: mongo_to_dict(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [mongo_to_dict(item) for item in obj]
    return obj
    
@grafana.get('/daily_insights')
async def get_daily_insights(request):
    try:
        # Estraiamo i parametri temporali dalla query di Grafana
        from_time = request.args.get('from', None)
        to_time = request.args.get('to', None)

        # Costruiamo il filtro temporale
        filter_query = {}
        if from_time and to_time:
            filter_query = {
                "Date": {
                    "$gte": from_time,
                    "$lte": to_time
                }
            }

        # Query sulla collezione pre-calcolata
        result = await mongo_gdelt.daily_insights.find(
            filter_query,
            {'_id': 0} 
        ).sort("Date", 1).to_list(length=None)

        return sanic_json(result)
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione della query: {str(e)}")
        return sanic_json({"error": str(e)}, status=500)
    
@grafana.get('/geo_analysis')
async def get_geo_analysis(request):
    try:
        # Estraiamo i parametri temporali dalla query di Grafana
        from_time = request.args.get('from', None)
        to_time = request.args.get('to', None)

        # Costruiamo il filtro temporale
        filter_query = {}
        if from_time and to_time:
            filter_query = {
                "Date": {
                    "$gte": from_time,
                    "$lte": to_time
                }
            }

        # Query sulla collezione pre-calcolata
        result = await mongo_gdelt.geo_analysis.find(
            filter_query,
            {'_id': 0}
        ).sort("NumMentions", -1).limit(50).to_list(length=None)

        return sanic_json(result)
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione della query: {str(e)}")
        return sanic_json({"error": str(e)}, status=500)
    
@grafana.get('/trend_analysis')
async def get_trend_analysis(request):
    try:
        # Estraiamo i parametri temporali dalla query di Grafana
        from_time = request.args.get('from', None)
        to_time = request.args.get('to', None)

        # Costruiamo il filtro temporale
        filter_query = {}
        if from_time and to_time:
            filter_query = {
                "Date": {
                    "$gte": from_time,
                    "$lte": to_time
                }
            }

        # Query sulla collezione pre-calcolata
        result = await mongo_gdelt.trend_analysis.find(
            filter_query,
            {'_id': 0}
        ).sort("Date", 1).to_list(length=None)

        return sanic_json(result)
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione della query: {str(e)}")
        return sanic_json({"error": str(e)}, status=500)


@grafana.get('/social_temporal_metrics')
async def get_social_temporal_metrics(request):
    try:
        collection = mongo_openmeasures["social_temporal_metrics"]
        social_temporal_metrics_data = await collection.find({}).to_list(length=None)
        social_temporal_metrics_data = [mongo_to_dict(doc) for doc in social_temporal_metrics_data]
        return sanic_json(social_temporal_metrics_data)
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione della query su 'social_temporal_metrics': {str(e)}")
        return sanic_json({"error": str(e)}, status=500)

@grafana.get('/top_authors_engagement')
async def get_top_authors_engagement(request):
    try:
        # Estraiamo i parametri temporali dalla query di Grafana
        from_time = request.args.get('from', None)
        to_time = request.args.get('to', None)

        # Costruiamo il filtro temporale
        filter_query = {}
        if from_time and to_time:
            filter_query = {
                "Date": {
                    "$gte": from_time,
                    "$lte": to_time
                }
            }

        # Query sulla collezione pre-calcolata
        result = await mongo_openmeasures.top_authors_engagement.find(
            filter_query,
            {'_id': 0}
        ).sort("engagement", -1).limit(10).to_list(length=None)

        return sanic_json(result)
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione della query: {str(e)}")
        return sanic_json({"error": str(e)}, status=500)

@grafana.get('/platform_engagement_distribution')
async def get_platform_engagement_distribution(request):
    try:
        # Estraiamo i parametri temporali dalla query di Grafana
        from_time = request.args.get('from', None)
        to_time = request.args.get('to', None)

        # Costruiamo il match temporale
        time_match = {}
        if from_time and to_time:
            from_date = datetime.fromisoformat(from_time.replace('Z', '+00:00'))
            to_date = datetime.fromisoformat(to_time.replace('Z', '+00:00'))
            
            time_match = {
                "created_at": {
                    "$gte": from_date.isoformat(),
                    "$lte": to_date.isoformat()
                }
            }

        pipeline = [
            # Aggiungiamo il filtro temporale
            {
                "$match": time_match
            },
            {
                "$group": {
                    "_id": "$Platform",
                    "total_likes": {"$sum": "$likes_count"},
                    "total_shares": {"$sum": "$shares_count"},
                    "total_comments": {"$sum": "$comments_count"}
                }
            },
            {
                "$addFields": {
                    "total_engagement": {
                        "$add": ["$total_likes", "$total_shares", "$total_comments"]
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "platform": "$_id",
                    "likes_percentage": {
                        "$multiply": [
                            {"$divide": ["$total_likes", "$total_engagement"]},
                            100
                        ]
                    },
                    "shares_percentage": {
                        "$multiply": [
                            {"$divide": ["$total_shares", "$total_engagement"]},
                            100
                        ]
                    },
                    "comments_percentage": {
                        "$multiply": [
                            {"$divide": ["$total_comments", "$total_engagement"]},
                            100
                        ]
                    }
                }
            },
            {
                "$sort": { "platform": 1 }
            }
        ]

        result = await mongo_openmeasures.Openmeasures_data.aggregate(pipeline).to_list(length=None)
        return sanic_json(result)

    except Exception as e:
        logger.error(f"Errore durante l'esecuzione della query: {str(e)}")
        return sanic_json({"error": str(e)}, status=500)

@grafana.get('/gdelt_social_correlation')
async def get_gdelt_social_correlation(request):
    try:
        # Estraiamo i parametri temporali dalla query di Grafana
        from_time = request.args.get('from', None)
        to_time = request.args.get('to', None)

        # Costruiamo il filtro temporale
        filter_query = {}
        if from_time and to_time:
            filter_query = {
                "Date": {
                    "$gte": from_time,
                    "$lte": to_time
                }
            }

        # Query sulla collezione pre-calcolata
        result = await mongo_openmeasures.gdelt_social_correlation.find(
            filter_query,
            {'_id': 0}
        ).sort("Date", 1).to_list(length=None)

        return sanic_json(result)
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione della query: {str(e)}")
        return sanic_json({"error": str(e)}, status=500)

if __name__ == "__main__":
    grafana.run(host="0.0.0.0", port=3000, debug=True)