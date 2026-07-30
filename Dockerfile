FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

RUN apt-get update && \
    apt-get install -y default-jdk wget && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN pip install pyspark==4.0.1 python-dotenv==1.1.1

WORKDIR /app

COPY . .

RUN chmod +x run.sh

CMD ["./run.sh"]