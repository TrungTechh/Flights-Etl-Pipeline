FROM spark-base:latest

ENV SPARK_WORKER_UI_PORT=8083
ENV SPARK_WORKER_WEBUI_PORT=8083
ENV SPARK_WORKER_LOG_DIR=/var/logs/spark
ENV SPARK_MASTER=spark://spark-master:7077

COPY start-worker.sh .

RUN chmod +x start-worker.sh

EXPOSE 8083

CMD [ "./start-worker.sh" ]