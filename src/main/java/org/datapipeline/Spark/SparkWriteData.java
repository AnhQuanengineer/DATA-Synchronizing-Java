package org.datapipeline.Spark;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datapipeline.Config.DatabaseConfig;
import org.datapipeline.Config.MySQLConfig;
import org.datapipeline.Connector.MySQLConnect;

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
            dfRead = spark.read()
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
}