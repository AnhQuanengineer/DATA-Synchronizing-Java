package org.datapipeline.Config;

public class MySQLConfig extends ValidateConfig {
    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final String database;
    private final String jarPath;

    private final String tableUsers;
    private final String tableRepositories;

    private MySQLConfig(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.user = builder.user;
        this.password = builder.password;
        this.database = builder.database;
        this.jarPath = builder.jarPath;
        this.tableUsers = builder.tableUsers;
        this.tableRepositories = builder.tableRepositories;
        super.validate();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String host;
        private Integer port; // Dùng Integer để có thể kiểm tra null
        private String user;
        private String password;
        private String database;
        private String jarPath;
        private String tableUsers = "Users";
        private String tableRepositories = "Repositories";

        public Builder host(String host) { this.host = host; return this; }
        // Port được nhận là int, nhưng lưu vào Integer để kiểm tra null trong build()
        public Builder port(int port) { this.port = port; return this; }
        public Builder user(String user) { this.user = user; return this; }
        public Builder password(String password) { this.password = password; return this; }
        public Builder database(String database) { this.database = database; return this; }
        public Builder jarPath(String jarPath) { this.jarPath = jarPath; return this; }
        public Builder tableUsers(String tableUsers) { this.tableUsers = tableUsers; return this; }
        public Builder tableRepositories(String tableRepositories) { this.tableRepositories = tableRepositories; return this; }

        public MySQLConfig build() {
            if (host == null || port == null || user == null || password == null || database == null) {
                throw new IllegalStateException("MySQL config requires host, port, user, password, and database.");
            }
            return new MySQLConfig(this);
        }
    }

    // Getters...
    public String getHost() { return host; }
    public int getPort() { return port; }
    public String getUser() { return user; }
    public String getDatabase() { return database; }
    public String getPassword() { return password; }
    public String getTableUsers() { return tableUsers; }
    public String getTableRepositories() { return tableRepositories; }

}