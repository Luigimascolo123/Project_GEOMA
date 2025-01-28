FROM bitnami/spark:3.4

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip wget curl && \
    rm -rf /var/lib/apt/lists/*

# Variabili d'ambiente per Hadoop
ENV HADOOP_VERSION=3.2.1
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native

# Installlazione Hadoop
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar -xzf hadoop-$HADOOP_VERSION.tar.gz && \
    mv hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    rm hadoop-$HADOOP_VERSION.tar.gz

# Installazione requirements
COPY requirements.txt /
RUN pip3 install -r /requirements.txt

# Download SpaCy model 
RUN python3 -m spacy download en_core_web_sm

# Cartella di lavoro
COPY app /app
WORKDIR /app

# Variabili d'ambiente per PySpark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

CMD ["python3", "main.py"]