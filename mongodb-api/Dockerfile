# Versione Python 3.12
FROM python:3.12-slim

# Cartella di lavoro
WORKDIR /app

# Variabili d'ambiente
ENV PYSPARK_PYTHON=/usr/local/bin/python3.12
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.12

# Requirements
COPY requirements.txt requirements.txt

# Installazione Requirements
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Imposta il comando di avvio per l'app
CMD ["python", "endpoint.py"]