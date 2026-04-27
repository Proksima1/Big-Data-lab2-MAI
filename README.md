# BigDataSpark

Анализ больших данных - лабораторная работа №2 - ETL реализованный с помощью Spark

Выполнил: Никольский Константин Германович М8О-308Б-23

Алгоритм запуска:
1. Клонировать репозиторий
2. Поднять сервисы с помощью `docker compose up -d`
3. Создать задачу для преобразования данных в снежинку (и дождаться завершения): 
```
docker compose exec jupyter bash -lc '
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.postgresql:postgresql:42.7.3 \
    --conf spark.driver.host=jupyter \
    --conf spark.driver.bindAddress=0.0.0.0 \
    /workspace/csv_to_postgres.py
'
```
4. Создать задачу для преобразования данных и загружки данных в витрины (и дождаться завершения):
```
docker compose exec jupyter bash -lc '
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.postgresql:postgresql:42.7.3 \
    --conf spark.driver.host=jupyter \
    --conf spark.driver.bindAddress=0.0.0.0 \
    /workspace/postgres_to_clickhouse.py
'
```