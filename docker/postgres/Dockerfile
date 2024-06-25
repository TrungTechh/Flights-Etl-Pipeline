FROM postgres:11

COPY init-hive-db.sh /docker-entrypoint-initdb.d/init-hive-db.sh

RUN chmod +x /docker-entrypoint-initdb.d/init-hive-db.sh

EXPOSE 5432