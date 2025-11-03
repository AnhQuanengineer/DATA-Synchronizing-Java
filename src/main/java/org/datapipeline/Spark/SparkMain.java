package org.datapipeline.Spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

import org.datapipeline.Config.*;
import org.datapipeline.Connector.SparkConnect;

import java.io.FileNotFoundException;
import java.net.URL;
import java.util.Map;

public class SparkMain {
    private static final String INPUT_JSON_PATH = "json/2015-03-01-17.json";


    // Định nghĩa Schema cho actor (StructType)
    private static final StructType ACTOR_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("login", DataTypes.StringType, true),
            DataTypes.createStructField("gravatar_id", DataTypes.StringType, true),
            DataTypes.createStructField("url", DataTypes.StringType, true),
            DataTypes.createStructField("avatar_url", DataTypes.StringType, true)
    });

    // Định nghĩa Schema cho repo (StructType)
    private static final StructType REPO_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("name", DataTypes.StringType, true),
            DataTypes.createStructField("url", DataTypes.StringType, true)
    });

    // Định nghĩa Schema cho dataframe (StructType)

    private static final StructType GithubEvent_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("actor", ACTOR_SCHEMA, true),
            DataTypes.createStructField("repo", REPO_SCHEMA, true),
    });

    public static void main(String[] args) throws FileNotFoundException {
        SparkConnect sparkConnect = new SparkConnect(
                "quandz-app",
                "local[*]",
                "4g",       // executorMemory
                2,          // executorCores
                "2g",       // driverMemory
                3,          // numExecutors
                null,       // jars: List<String> - KHÔNG CÓ JARS
                null,       // sparkConf: Map<String, String> - KHÔNG CÓ CẤU HÌNH TÙY CHỈNH
                "ERROR"
        );

        System.out.println("--- BƯỚC 1: Đọc JSON vào DataFrame ---");

        // Lấy URL của resource (sẽ là "file:/...")
        URL resourceUrl = SparkMain.class.getClassLoader().getResource(INPUT_JSON_PATH);

        if (resourceUrl == null) {
            throw new FileNotFoundException("Resource not found: " + INPUT_JSON_PATH);
        }

        Dataset<Row> df = sparkConnect
                .getSparkSession()
                .read()
                .schema(GithubEvent_SCHEMA)
                .json(resourceUrl.getPath());

        System.out.println("Schema (DF) đã được định nghĩa:");
//        df.show();

//        // -----------------------------------------------------------
//        // BƯỚC 2: CHUYỂN SANG DATASET<CUSTOMER> VÀ XỬ LÝ NGHIỆP VỤ
//        // -----------------------------------------------------------
//        System.out.println("\n--- BƯỚC 2: Chuyển sang Dataset<Customer> và Xử lý Logic ---");
//
//        // Tạo Encoder cho lớp GithubEvent
//        // Spark sử dụng Reflection để tạo Encoder từ Java Bean class
//        Dataset<GithubEvent> githubEventsDs = df.as(Encoders.bean(GithubEvent.class));
//
//        // Bây giờ, bạn có thể xử lý dữ liệu với kiểu mạnh (type-safe)
//        githubEventsDs.show(5, false);

        Dataset<Row> userWriteTableDs = df.withColumn("spark_temp", lit("spark_write"))
                .select(
                        col("actor.id").alias("user_id")
                        ,col("actor.login").alias("login")
                        ,col("actor.gravatar_id").alias("gravatar_id")
                        ,col("actor.avatar_url").alias("avatar_url")
                        ,col("actor.url").alias("url")
                        ,col("spark_temp")
                );

        userWriteTableDs.show(5, false);

        Dataset<Row> repoWriteTableDs = df.select(
                col("repo.id").alias("repo_id")
                ,col("repo.name").alias("name")
                ,col("repo.url").alias("url")
        );
        repoWriteTableDs.show();

        Map<String, ValidateConfig> Config;
        try {
            ConfigurationSource source = new EnvironmentSource();
            ConfigLoader loader = new ConfigLoader(source);
            Config = loader.getDatabaseConfig();
        } catch (Exception e) {
            System.err.println("LỖI KHÔNG THỂ TẢI CẤU HÌNH: Dừng ứng dụng.");
            System.err.println("Chi tiết: " + e.getMessage());
            return;
        }

        MongoDBConfig mongoConfig = (MongoDBConfig) Config.get("mongodb");
        MySQLConfig mySQLConfig = (MySQLConfig) Config.get("mysql");

        SparkWriteData writeDataMySQL= new SparkWriteData(sparkConnect.getSparkSession(), Config);
        writeDataMySQL.writeAllDatabase(userWriteTableDs, mongoConfig.getCollection(), mySQLConfig.getTableUsers(), "append");

        SparkWriteData validateDataMySQL= new SparkWriteData(sparkConnect.getSparkSession(), Config);
        validateDataMySQL.valdateAllDatabase(userWriteTableDs, mongoConfig.getCollection(), mySQLConfig.getTableUsers(), "append");

        sparkConnect.stop();
    }
}
