FROM docker.io/fedora:latest
RUN dnf upgrade -y && dnf install -y curl wget java-latest-openjdk scala unzip

# Instalar Spark
RUN wget -P /home/root/ https://www.apache.org/dyn/closer.lua/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3-scala2.13.tgz
RUN mkdir /home/root/spark-3.3.0-bin-hadoop3
RUN ls /home/root
RUN tar -xvf /home/root/spark-3.3.0-bin-hadoop3-scala2.13.tgz -C /home/root/

# Download ficheiros a realizar tratamento de dados
RUN wget -P /home/root/ https://github.com/bdu-xpand-it/BDU-Recruitment-Challenges/raw/master/google-play-store-apps.zip
RUN mkdir /home/root/files
RUN unzip /home/root/google-play-store-apps.zip -d /home/root/files

