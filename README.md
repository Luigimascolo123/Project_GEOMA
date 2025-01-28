# Integration of Big Data infrastructure analysis and visualization of GDELT and OpenMeasures data

## Project Description
This project implements an integrated analysis system that combines data from the GDELT database (Global Database of Events, Language, and Tone) with social media data. The goal is to create a platform that allows the analysis of correlations between global events and related social media discussions.

## System Architecture
The system is structured into several main components:
- Automated GDELT data collection and processing
- Keywords extraction from event URLs 
- Social media data collection based on keywords
- Data processing using Apache Spark
- MongoDB storage with aggregated collections
- Grafana visualization dashboard

## Data Flow
1. **GDELT Processing**
   - Downloads event data from GDELT
   - Processes CSV files using Spark
   - Stores events in MongoDB

2. **Keywords Extraction**
   - Analyzes URLs from top mentioned events
   - Extracts and ranks relevant keywords
   - Uses top keywords for social media search

3. **Social Media Integration**
   - Collects data from TikTok, Truth Social and VK
   - Processes engagement metrics
   - Stores in dedicated MongoDB collections

4. **Data Analysis**
   - Creates temporal metrics
   - Analyzes platform-specific content
   - Generates engagement insights
   - Visualizes correlations in Grafana

## Technologies
- Apache Spark 3.4
- MongoDB
- Grafana
- Docker
- Python 
- HDFS

## Setup
```bash
# Clone repository and start services
git clone [repository_URL]
docker-compose up -d