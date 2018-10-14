/*-----------------------------------------------------------------------------
 - Copyright (C) BlueLapiz.net - All Rights Reserved                          -
 - Unauthorized copying of this file, via any medium is strictly prohibited   -
 - Proprietary and confidential                                               -
 - Written by Alexander Sagen <alexmsagen@gmail.com>                          -
 -----------------------------------------------------------------------------*/

package app.sagen.mysqlutils;

public class MySQLConfig {

    public static Builder builder() {
        return new Builder();
    }

    private String database;
    private String host;
    private String user;
    private String pass;

    private MySQLConfig(Builder builder) {
        this.database = builder.database;
        this.host = builder.host;
        this.user = builder.user;
        this.pass = builder.pass;
    }

    public String getDatabase() {
        return database;
    }

    public String getHost() {
        return host;
    }

    public String getUser() {
        return user;
    }

    public String getPass() {
        return pass;
    }

    public static class Builder {
        private String database = "minecraft";
        private String host = "localhost";
        private String user = "root";
        private String pass = "";

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder pass(String pass) {
            this.pass = pass;
            return this;
        }

        public MySQLConfig build() {
            return new MySQLConfig(this);
        }
    }
}
