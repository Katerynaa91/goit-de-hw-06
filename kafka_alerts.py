import os
import uuid
from pyspark.sql.functions import (
            from_json, col, window, avg, to_json, 
            struct, current_timestamp, from_unixtime, udf)
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql import SparkSession

from configs import kafka_config

# Налаштування для роботи з Kafka через PySpark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

# Створення checkpoint директорії
checkpoint_path = os.path.abspath("./checkpoint")
kafka_checkpoint_path = os.path.abspath("./checkpoint/kafka_alerts")

os.makedirs(checkpoint_path, exist_ok=True)
os.makedirs(kafka_checkpoint_path, exist_ok=True)

# Створення Spark-сесії
spark = (
    SparkSession.builder.appName("AggFilterAlerts")
    .master("local[*]")
    .config("spark.sql.debug.maxToStringFields", "100")
    .config("spark.sql.columnNameLengthThreshold", "100")
    .config("spark.sql.streaming.checkpointLocation", checkpoint_path)
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .config("spark.hadoop.io.native.lib.available", "false")
    .getOrCreate()
)


# Завантаження CSV-файлу з умовами для алертів відповідно до трешхолдів температури та вологості
alerts_df = (spark.read.csv("./data/alerts_conditions.csv", header=True)
             .withColumn("code", col("code").cast(IntegerType()))
             .withColumn("min_temperature", col("min_temperature").cast(IntegerType()))
             .withColumn("max_temperature", col("max_temperature").cast(IntegerType()))
             .withColumn("min_humidity", col("min_humidity").cast(IntegerType()))
             .withColumn("max_humidity", col("max_humidity").cast(IntegerType())))

# Встановлення значень для вікна тривалості та ковзного інтервалу для обчислення алертів
window_duration = "1 minute"  
sliding_interval = "30 seconds" 

# Підключення до Kafka та читання потоку даних з топіку
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option(
        "kafka.sasl.jaas.config", kafka_config["kafka.sasl.jaas.config"]
    )
    .option(
        "subscribe", kafka_config["topic_building_sensors"]
    )  
    .option("startingOffsets", "earliest")  
    .option("maxOffsetsPerTrigger", "200")  # Максимум 200 повідомлень на тригер
    .load()  
)

# Створення JSON-схеми для декодування вхідних даних
json_schema = StructType(
    [
        StructField("sensor_id", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
    ]
)

# Обчислення середніх значень температури та вологості
# Додавання watermark для обробки запізнілих даних
# Групування за ковзним вікном (Sliding window) та інтервалом (sliding interval)
avg_alerts = (
    df.selectExpr(
        "CAST(key AS STRING) AS key_deserialized", 
        "CAST(value AS STRING) AS value_deserialized", 
        "*",  
    )
    .drop("key", "value")  
    .withColumnRenamed("key_deserialized", "key")  
    .withColumn(
        "value_json", from_json(col("value_deserialized"), json_schema)
    )  
    .withColumn(
        "timestamp",
        from_unixtime(col("value_json.timestamp").cast(DoubleType())).cast("timestamp"),
    )  
    .withWatermark(
        "timestamp", "10 seconds"
    )  
    .groupBy(
        window(col("timestamp"), window_duration, sliding_interval)
    )  
    .agg(
        avg("value_json.temperature").alias(
            "t_avg"
        ),  
        avg("value_json.humidity").alias("h_avg"),  
    )
)

# Об'єднання таблиць з середніми значеннями та трешхолдами 
# для подальшої фільтрації значень відповідно до умов
all_alerts = avg_alerts.crossJoin(alerts_df)

# Фільтрація для обробки алертів за температурою та вологістю відповідно до умов
# Якщо значення виходять за межі допустимих порогів -999 означає, що поріг не враховується
valid_alerts = all_alerts.where(
    "((min_temperature != -999 AND t_avg < min_temperature) OR " +
    "(max_temperature != -999 AND t_avg > max_temperature)) OR " +
    "((min_humidity != -999 AND h_avg < min_humidity) OR " +
    "(max_humidity != -999 AND h_avg > max_humidity))"
).withColumn(
    "timestamp", current_timestamp()                        
).drop("min_temperature", "max_temperature", "min_humidity", "max_humidity")

# Створення унікального ключа для кожного запису
uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

# Підготовка до відправки даних в Kafka з перетворенням у формат JSON
json_format_df = valid_alerts.withColumn("key", uuid_udf()).select(
    col("key"),
    to_json(
        struct(
            col("window"),
            col("t_avg"),
            col("h_avg"),
            col("code"),
            col("message"),
            col("timestamp"),
        )
    ).alias(
        "value"
    ),  
)

# Виведення в консоль
stream_to_console = (
    valid_alerts.writeStream.trigger(
        processingTime="10 seconds"
    )  # Тригер кожні 10 секунд
    .outputMode("update")  # Режим оновлення
    .format("console")  
    .option("truncate", "false") 
    .option("numRows", 10)  # Вивести лише 10 рядків на раз
    .start()  
)

# Відправка алертів у Kafka-топік
stream_to_kafka = (
    json_format_df.writeStream.trigger(
        processingTime="10 seconds"
    )
    .outputMode("update")  
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option("kafka.sasl.jaas.config", kafka_config["kafka.sasl.jaas.config"])
    .option("topic", "katerynaa_alerts")  # Вихідна тема для алертів
    .option("checkpointLocation", kafka_checkpoint_path)
    .start()  
)

# Очікування завершення потоків
stream_to_console.awaitTermination()



