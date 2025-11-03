package org.datapipeline.Spark;

import org.apache.hadoop.shaded.org.checkerframework.checker.units.qual.C;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.datapipeline.Config.*;
import org.datapipeline.Connector.SparkConnect;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.streaming.Trigger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class SparkConsumer {

    private static final StructType schema = new StructType(new StructField[]{
            DataTypes.createStructField("user_id", DataTypes.IntegerType, true),
            DataTypes.createStructField("login", DataTypes.StringType, true),
            DataTypes.createStructField("gravatar_id", DataTypes.StringType, true),
            DataTypes.createStructField("avatar_url", DataTypes.StringType, true),
            DataTypes.createStructField("url", DataTypes.StringType, true),
            DataTypes.createStructField("state", DataTypes.StringType, true),
            DataTypes.createStructField("log_timestamp", DataTypes.StringType, true),
    });

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {

        List<String> jars = Arrays.asList("org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0");

        Map<String, ValidateConfig> ConfigKafka;
        try {
            ConfigurationSource source = new EnvironmentSource();
            ConfigLoader loader = new ConfigLoader(source);
            ConfigKafka = loader.getKafkaConfig();
        } catch (Exception e) {
            System.err.println("LỖI KHÔNG THỂ TẢI CẤU HÌNH: Dừng ứng dụng.");
            System.err.println("Chi tiết: " + e.getMessage());
            return;
        }
        KafkaConfig kafkaConfig = (KafkaConfig) ConfigKafka.get("kafka");

        Map<String, ValidateConfig> ConfigDatabase;
        try {
            ConfigurationSource source = new EnvironmentSource();
            ConfigLoader loader = new ConfigLoader(source);
            ConfigDatabase = loader.getKafkaConfig();
        } catch (Exception e) {
            System.err.println("LỖI KHÔNG THỂ TẢI CẤU HÌNH: Dừng ứng dụng.");
            System.err.println("Chi tiết: " + e.getMessage());
            return;
        }
        MongoDBConfig mongoDBConfig = (MongoDBConfig) ConfigDatabase.get("mongodb");

        SparkConnect sparkConnect = new SparkConnect(
                "quandz-app",
                "local[*]",
                "4g",       // executorMemory
                2,          // executorCores
                "2g",       // driverMemory
                3,          // numExecutors
                jars,       // jars: List<String> - KHÔNG CÓ JARS
                null,       // sparkConf: Map<String, String> - KHÔNG CÓ CẤU HÌNH TÙY CHỈNH
                "ERROR"
        );

        // Đọc từ Kafka

        Dataset<Row> kafkaDF = sparkConnect
                .getSparkSession()
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaConfig.getBootstrapServers())
                .option("subscribe", kafkaConfig.getTopic())
                .option("kafka.group.id", "spark_group")
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load();

        Dataset<Row> rawDF = kafkaDF.select(
                col("value").cast("String")
        );

        Dataset<Row> cleanedDF = rawDF
                .withColumn("value", regexp_replace(col("value"), "\\\\", ""))
                .withColumn("value", regexp_replace(col("value"), "^\"|\"$", ""));

        // Parse JSON
        Dataset<Row> parsedDF = cleanedDF
                .select(from_json(col("value"), schema).alias("data"))
                .filter(col("data").isNotNull())
                .select("data.*");

        // Kiểm tra schema
        if (parsedDF == null || parsedDF.schema().fields().length == 0) {
            System.err.println("Không parse được JSON. Kiểm tra dữ liệu Kafka!");
            return;
        }

        StreamingQuery query = parsedDF.writeStream()
                .format("mongo")
                .option("uri", mongoDBConfig.getUri())
                .option("database", mongoDBConfig.getDbName())
                .option("collection", mongoDBConfig.getCollection())
                .option("checkpointLocation", "/tmp/spark-checkpoint/kafka-mongo/")
                .option("forceDeleteTempCheckpointLocation", "true")
                .trigger(Trigger.Continuous("1 second"))  // hoặc Trigger.ProcessingTime("1 second")
                .outputMode("append")
                .start();

        System.out.println("Streaming started: Kafka → MongoDB (github_data.Users)");
        query.awaitTermination();
    }
}
