package org.datapipeline.Spark;

import com.mongodb.client.MongoDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.conversions.Bson;
import org.datapipeline.Config.DatabaseConfig;
import org.datapipeline.Config.MongoDBConfig;
import org.datapipeline.Config.MySQLConfig;
import org.datapipeline.Connector.MongoDBConnect;
import org.datapipeline.Connector.MySQLConnect;
import static org.apache.spark.sql.functions.*;
import org.bson.Document;
import com.mongodb.client.model.Updates;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class SparkWriteData {
    private static final Logger LOG = LogManager.getLogger(SparkWriteData.class);
    private static final String DEFAULT_DATABASE_NAME = "github_data";

    private final SparkSession spark;
    private final Map<String, DatabaseConfig> sparkConfig;

    public SparkWriteData(SparkSession spark, Map<String, DatabaseConfig> sparkConfig) {
        this.spark = spark;
        this.sparkConfig = sparkConfig;
    }

    public void sparkWriteMySQL(Dataset<Row> dfWrite, String tableName, String mode) {
        MySQLConfig mysqlConfig = (MySQLConfig) this.sparkConfig.get("mysql");
        if (mysqlConfig == null) {
            throw new IllegalArgumentException("MySQL configuration not found in sparkConfig");
        }

        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s",
                mysqlConfig.getHost(), mysqlConfig.getPort(), mysqlConfig.getDatabase());

        addTempColumn(tableName, mysqlConfig);

        try {
            dfWrite.write()
                    .format("jdbc")
                    .option("url", jdbcUrl)
                    .option("dbtable", tableName)
                    .option("user", mysqlConfig.getUser())
                    .option("password", mysqlConfig.getPassword())
                    .option("driver", "com.mysql.cj.jdbc.Driver")
                    .option("batchsize", "1000")
                    .option("rewriteBatchedStatements", "true")
                    .mode(mode)
                    .save();

            LOG.info("Spark đã ghi dữ liệu thành công vào bảng: {}", tableName);

        } catch (IllegalArgumentException e) {
            LOG.error("SaveMode không hợp lệ: {}. Chỉ hỗ trợ: overwrite, append, ignore, error", mode);
            throw e;
        } catch (Exception e) {
            LOG.error("LỖI KHI GHI DỮ LIỆU QUA SPARK JDBC", e);
            throw new RuntimeException("Ghi dữ liệu thất bại", e);
        }
    }

    public void validateSparkMySQL(Dataset<Row> dfWrite, String tableName, String mode) {
        MySQLConfig mysqlConfig = (MySQLConfig) this.sparkConfig.get("mysql");
        if (mysqlConfig == null) {
            throw new IllegalArgumentException("MySQL configuration not found in sparkConfig");
        }

        LOG.info("Bắt đầu validate dữ liệu cho bảng: {}", tableName);

        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s",
                mysqlConfig.getHost(), mysqlConfig.getPort(), mysqlConfig.getDatabase());

        String subQuery = String.format(
                "(SELECT * FROM `%s` WHERE spark_temp = 'spark_write') AS sub_query", tableName);

        Dataset<Row> dfRead;
        try {
            dfRead = this.spark.read()
                    .format("jdbc")
                    .option("url", jdbcUrl)
                    .option("dbtable", subQuery)
                    .option("user", mysqlConfig.getUser())
                    .option("password", mysqlConfig.getPassword())
                    .option("driver", "com.mysql.cj.jdbc.Driver")
                    .option("pushDownPredicate", "true")
                    .load();
            LOG.debug("Đã đọc {} bản ghi từ MySQL", dfRead.count());
        } catch (Exception e) {
            LOG.error("Không thể đọc dữ liệu từ MySQL để validate", e);
            throw new RuntimeException("Lỗi đọc dữ liệu từ MySQL", e);
        }

        dfWrite.cache();
        dfRead.cache();

        long writeCount = dfWrite.count();
        long readCount = dfRead.count();

        Dataset<Row> diff = dfWrite.exceptAll(dfRead);
        long diffCount = diff.count();

        if (writeCount == readCount) {
            LOG.info("Validate thành công: {} bản ghi khớp hoàn toàn.", readCount);
        } else {
            LOG.warn("Phát hiện {} bản ghi thiếu (tổng: {}, hiện có: {})", diffCount, writeCount, readCount);
        }

        if (!diff.isEmpty()) {
            try {
                diff.write()
                        .format("jdbc")
                        .option("url", jdbcUrl)
                        .option("dbtable", tableName)
                        .option("user", mysqlConfig.getUser())
                        .option("password", mysqlConfig.getPassword())
                        .option("driver", "com.mysql.cj.jdbc.Driver")
                        .option("batchsize", "1000")
                        .option("rewriteBatchedStatements", "true")
                        .mode(mode)
                        .save();
                LOG.info("Đã chèn thành công {} bản ghi thiếu vào bảng: {}", diffCount, tableName);
            } catch (Exception e) {
                LOG.error("Lỗi khi ghi dữ liệu thiếu vào MySQL", e);
                throw new RuntimeException("Lỗi khi ghi dữ liệu thiếu", e);
            }
        } else {
            LOG.info("Không có bản ghi nào cần chèn.");
        }

        cleanupTempColumn(tableName, mysqlConfig);
    }

    private void addTempColumn(String tableName, MySQLConfig mysqlConfig) {
        String checkColumnSql = String.format(
                "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND COLUMN_NAME = 'spark_temp'",
                DEFAULT_DATABASE_NAME, tableName);

        String addColumnSql = String.format(
                "ALTER TABLE `%s` ADD COLUMN `spark_temp` VARCHAR(255)", tableName);

        try (MySQLConnect mysqlClient = MySQLConnect.builder()
                .host(mysqlConfig.getHost())
                .port(mysqlConfig.getPort())
                .user(mysqlConfig.getUser())
                .password(mysqlConfig.getPassword())
                .build()) {

            Connection conn = mysqlClient.getConnection();

            try (Statement useStmt = conn.createStatement()) {
                useStmt.executeUpdate("USE " + DEFAULT_DATABASE_NAME);
            }

            try (Statement checkStmt = conn.createStatement();
                 ResultSet rs = checkStmt.executeQuery(checkColumnSql)) {

                if (!rs.next()) {
                    try (Statement addStmt = conn.createStatement()) {
                        addStmt.executeUpdate(addColumnSql);
                        conn.commit();
                        LOG.info("Đã thêm cột `spark_temp` vào bảng: {}", tableName);
                    }
                } else {
                    LOG.debug("Cột `spark_temp` đã tồn tại, bỏ qua thêm.");
                }
            } catch (SQLException e) {
                if (conn != null && !conn.isClosed()) {
                    try { conn.rollback(); } catch (SQLException ex) { /* ignore */ }
                }
                LOG.debug("Đã rollback transaction do lỗi thêm cột");
            }

        } catch (SQLException e) {
            LOG.warn("Lỗi khi thêm cột `spark_temp` (có thể không ảnh hưởng)", e);
        } catch (Exception e) {
            LOG.error("Lỗi không mong muốn khi thêm cột `spark_temp`", e);
        }
    }

    // === 1. cleanupTempColumn CHO MYSQL ===
    private void cleanupTempColumn(String tableName, MySQLConfig mysqlConfig) {
        String checkColumnSql = String.format(
                "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND COLUMN_NAME = 'spark_temp'",
                DEFAULT_DATABASE_NAME, tableName);

        String dropColumnSql = String.format(
                "ALTER TABLE `%s` DROP COLUMN `spark_temp`", tableName);

        try (MySQLConnect mysqlClient = MySQLConnect.builder()
                .host(mysqlConfig.getHost())
                .port(mysqlConfig.getPort())
                .user(mysqlConfig.getUser())
                .password(mysqlConfig.getPassword())
                .build()) {

            Connection conn = mysqlClient.getConnection();

            try (Statement useStmt = conn.createStatement()) {
                useStmt.executeUpdate("USE " + DEFAULT_DATABASE_NAME);
            }

            try (Statement checkStmt = conn.createStatement();
                 ResultSet rs = checkStmt.executeQuery(checkColumnSql)) {

                if (rs.next()) {
                    try (Statement dropStmt = conn.createStatement()) {
                        dropStmt.executeUpdate(dropColumnSql);
                        conn.commit();
                        LOG.info("Đã xóa cột `spark_temp` khỏi bảng: {}", tableName);
                    }
                } else {
                    LOG.debug("Cột `spark_temp` không tồn tại, bỏ qua xóa.");
                }
            } catch (SQLException e) {
                if (conn != null && !conn.isClosed()) {
                    try { conn.rollback(); } catch (SQLException ex) { /* ignore */ }
                }
                LOG.debug("Đã rollback transaction do lỗi xóa cột");
            }

        } catch (SQLException e) {
            LOG.warn("Lỗi khi xóa cột `spark_temp` (có thể không ảnh hưởng)", e);
        } catch (Exception e) {
            LOG.error("Lỗi không mong muốn khi xóa cột `spark_temp`", e);
        }
    }

    // === 2. cleanupTempColumn CHO MONGODB ===
    private void cleanupTempColumn(String collectionName, MongoDBConfig mongoConfig) {
        try (MongoDBConnect mongoClient = MongoDBConnect.builder()
                .uri(mongoConfig.getUri())
                .dbName(mongoConfig.getDbName())
                .build()) {

            // 1. Kết nối và lấy database
            MongoDatabase db = mongoClient.connect();

            // 1. Bộ lọc (Filter): Chọn TẤT CẢ documents
            Document filter = new Document(); // Tương đương với {} trong JSON

            // 2. Cập nhật (Update): Sử dụng Bson $unset operator
            // Tương đương với: { $unset: { "spark_temp": "" } }
            Bson update = Updates.unset("spark_temp");

            // 3. Thực hiện updateMany
            long modifiedCount = db.getCollection(collectionName)
                    .updateMany(filter, update)
                    .getModifiedCount();

            LOG.info("Đã xóa field `spark_temp` khỏi {} document(s) trong collection: {}", modifiedCount, collectionName);
        } catch (Exception e) {
            LOG.error("Lỗi khi xóa field `spark_temp` từ MongoDB collection: {}", collectionName, e);
        }
    }

    public void sparkWriteMongoDB(
            Dataset<Row> dfWrite
            , String collectionName
            , String mode) {

        MongoDBConfig mongoConfig = (MongoDBConfig) this.sparkConfig.get("mongodb");
        if (mongoConfig == null) {
            throw new IllegalArgumentException("MongoDB configuration not found in sparkConfig");
        }

        try{
            dfWrite.write()
                    .format("mongo")
                    .option("uri", mongoConfig.getUri())
                    .option("database", mongoConfig.getDbName())
                    .option("collection", collectionName)
                    .mode(mode)
                    .save();

            LOG.info("Spark đã ghi dữ liệu thành công vào MongoDB collection: {}", collectionName);
        } catch (IllegalArgumentException e) {
            LOG.error("SaveMode không hợp lệ: {}. Chỉ hỗ trợ: overwrite, append, ignore, error", mode);
            throw e;
        } catch (Exception e) {
            LOG.error("LỖI KHI GHI DỮ LIỆU VÀO MONGODB", e);
            throw new RuntimeException("Ghi dữ liệu vào MongoDB thất bại", e);
        }
    }

    public void validateSparkMongoDB(
            Dataset<Row> dfWrite
            , String collectionName
            , String mode ) {
        //get Mongo config
        MongoDBConfig mongoConfig = (MongoDBConfig) this.sparkConfig.get("mongodb");
        if (mongoConfig == null) {
            throw new IllegalArgumentException("MongoDB configuration not found in sparkConfig");
        }

        LOG.info("Bắt đầu validate dữ liệu cho collection: {}", collectionName);

        Document matchStage = new Document("$match", new Document("spark_temp", "spark_write"));
        String pipeline = "[" + matchStage.toJson() + "]";

        Dataset<Row> dfReadMongo;
        try {
            dfReadMongo = this.spark.read()
                    .format("mongo")
                    .option("uri", mongoConfig.getUri())
                    .option("database", mongoConfig.getDbName())
                    .option("collection", collectionName)
                    .option("pipeline", pipeline)
                    .load();

            LOG.debug("Đã đọc {} bản ghi từ MongoDB", dfReadMongo.count());
        } catch (Exception e) {
            LOG.error("Không thể đọc dữ liệu từ MongoDB để validate", e);
            throw new RuntimeException("Lỗi đọc dữ liệu từ MongoDB", e);
        }

        Dataset<Row> dfRead = dfReadMongo.select(
                col("user_id")
                , col("login")
                , col("gravatar_id")
                , col("avatar_url")
                , col("url")
                , col("spark_temp")
        );
        //cache cho việc count nhiều
        dfWrite.cache();
        dfRead.cache();

        long writeCount = dfWrite.count();
        long readCount = dfRead.count();

        Dataset<Row> diff = dfWrite.exceptAll(dfRead);
        long diffCount = diff.count();

        if (writeCount == readCount) {
            LOG.info("Validate thành công: {} bản ghi khớp hoàn toàn.", readCount);
        } else {
            LOG.warn("Phát hiện {} bản ghi thiếu (tổng: {}, hiện có: {})", diffCount, writeCount, readCount);
        }

        if (!diff.isEmpty()) {
            try {
                diff.write()
                        .format("mongo")
                        .option("uri", mongoConfig.getUri())
                        .option("database", mongoConfig.getDbName())
                        .option("collection", collectionName)
                        .mode(mode)
                        .save();
                LOG.info("Đã chèn thành công {} bản ghi thiếu vào bảng: {}", diffCount, collectionName);
            } catch (Exception e) {
                LOG.error("Lỗi khi ghi dữ liệu thiếu vào MongoDB", e);
                throw new RuntimeException("Lỗi khi ghi dữ liệu thiếu", e);
            }
        } else {
            LOG.info("Không có bản ghi nào cần chèn.");
        }

        cleanupTempColumn(collectionName, mongoConfig);
    }

    public void writeAllDatabase(Dataset<Row> dfWrite, String collectionNameOrTableName, String mode) {

        if (dfWrite == null || dfWrite.isEmpty()) {
            LOG.warn("DataFrame rỗng hoặc null → bỏ qua ghi vào tất cả database");
            return;
        }
        try {
            this.sparkWriteMySQL(dfWrite, collectionNameOrTableName, mode);
            this.sparkWriteMongoDB(dfWrite, collectionNameOrTableName, mode);
            LOG.info("Write success to all databases: MySQL table '{}' và MongoDB collection '{}'", collectionNameOrTableName, collectionNameOrTableName);
        } catch (Exception e) {
            LOG.error("GHI Data THẤT BẠI ", e);
            throw new RuntimeException("Ghi dữ liệu ", e);
        }
    }

    public void valdateAllDatabase(Dataset<Row> dfWrite, String collectionNameOrTableName, String mode) {
        if (dfWrite == null || dfWrite.isEmpty()) {
            LOG.warn("DataFrame rỗng hoặc null → bỏ qua ghi vào tất cả database");
            return;
        }

        try {
            this.validateSparkMySQL(dfWrite, collectionNameOrTableName, mode);
            this.validateSparkMongoDB(dfWrite, collectionNameOrTableName, mode);
            LOG.info("Validate success to all databases: MySQL table '{}' và MongoDB collection '{}'", collectionNameOrTableName, collectionNameOrTableName);
        } catch (Exception e) {
            LOG.error("Validate Data THẤT BẠI ", e);
            throw new RuntimeException("Validate dữ liệu ", e);
        }
    }
}