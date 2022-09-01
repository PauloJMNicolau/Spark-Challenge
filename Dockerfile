FROM docker.io/fedora:latest
RUN dnf upgrade -y && dnf install -y curl wget java-11-openjdk scala unzip gzip
WORKDIR /home/root/

# Instalar Spark
RUN wget https://dlcdn.apache.org/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3-scala2.13.tgz
RUN tar -xvf spark-3.3.0-bin-hadoop3-scala2.13.tgz

# Download ficheiros a realizar tratamento de dados
RUN wget  https://github.com/bdu-xpand-it/BDU-Recruitment-Challenges/raw/master/google-play-store-apps.zip
RUN mkdir files
RUN unzip google-play-store-apps.zip -d files
