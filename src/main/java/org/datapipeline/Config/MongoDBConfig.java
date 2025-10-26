package org.datapipeline.Config;

public class MongoDBConfig extends DatabaseConfig {
    private final String uri;
    private final String dbName;
    private final String jarPath; // Có thể là null
    private final String collection = "Users"; // Giá trị mặc định

    private MongoDBConfig(Builder builder) {
        this.uri = builder.uri;
        this.dbName = builder.dbName;
        this.jarPath = builder.jarPath;
        super.validate();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String uri;
        private String dbName;
        private String jarPath;

        public Builder uri(String uri) { this.uri = uri; return this; }
        public Builder dbName(String dbName) { this.dbName = dbName; return this; }
        public Builder jarPath(String jarPath) { this.jarPath = jarPath; return this; }

        public MongoDBConfig build() {
            if (uri == null || dbName == null) {
                throw new IllegalStateException("MongoDB config requires 'uri' and 'dbName'.");
            }
            return new MongoDBConfig(this);
        }
    }

    // Getters...
    public String getUri() { return uri; }
    public String getDbName() { return dbName; }
    public String getCollection() { return collection; }
    public String getJarPath() { return jarPath; }
}