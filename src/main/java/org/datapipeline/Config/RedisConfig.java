package org.datapipeline.Config;

public class RedisConfig extends ValidateConfig {
    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final int database;
    private final String jarPath;

    private final String keyColumn = "id"; // Giá trị mặc định

    private RedisConfig(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.user = builder.user;
        this.password = builder.password;
        this.database = builder.database;
        this.jarPath = builder.jarPath;
        super.validate();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String host;
        private Integer port;
        private String user;
        private String password;
        private int database;
        private String jarPath;

        public Builder host(String host) { this.host = host; return this; }
        public Builder port(int port) { this.port = port; return this; }
        public Builder user(String user) { this.user = user; return this; }
        public Builder password(String password) { this.password = password; return this; }
        public Builder database(int database) { this.database = database; return this; }
        public Builder jarPath(String jarPath) { this.jarPath = jarPath; return this; }

        public RedisConfig build() {
            if (host == null || port == null || user == null || password == null) {
                throw new IllegalStateException("Redis config requires host, port, user, password, and database.");
            }
            return new RedisConfig(this);
        }
    }

    // Getters...
    public String getHost() { return host; }
    public int getPort() { return port; }
    public String getUser() { return user; }
    public int getDatabase() { return database; }
    public String getJarPath() { return jarPath; }
    public String getPassword() { return password; }
}