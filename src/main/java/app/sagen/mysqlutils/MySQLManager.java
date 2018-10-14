/*-----------------------------------------------------------------------------
 - Copyright (C) BlueLapiz.net - All Rights Reserved                          -
 - Unauthorized copying of this file, via any medium is strictly prohibited   -
 - Proprietary and confidential                                               -
 - Written by Alexander Sagen <alexmsagen@gmail.com>                          -
 -----------------------------------------------------------------------------*/

package app.sagen.mysqlutils;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

@SuppressWarnings({"Unused", "Duplicate"})
public class MySQLManager {
	
	private static MySQLManager mySQLManager;

	private static MySQLHelper_legacy mySQLHelperLegacy;
	private static MySQLHelper mySQLHelperExperimental;
    private static MySQLTableHelper mySQLTableCreator;

    public static boolean setup(MySQLConfig config) {
        try {
        	mySQLManager = new MySQLManager(config);
            mySQLManager.getConnection().close();
		} catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static MySQLHelper getSQLHelper() {
        if(mySQLHelperExperimental == null) mySQLHelperExperimental = new MySQLHelper();
        return mySQLHelperExperimental;
    }

    public static MySQLHelper_legacy getSQLHelper_legacy() {
        if(mySQLHelperLegacy == null) mySQLHelperLegacy = new MySQLHelper_legacy();
        return mySQLHelperLegacy;
    }

    public static MySQLTableHelper getTableHelper() {
        if(mySQLTableCreator == null) mySQLTableCreator = new MySQLTableHelper();
        return mySQLTableCreator;
    }

	public static MySQLManager getMySQLMan() {
		return mySQLManager;
	}

    private MySQLConfig config;
    private HikariDataSource dataSource;

	private MySQLManager(MySQLConfig config) {
	    this.config = config;
    }

	public Connection getConnection() {

		if(dataSource == null) {

			try {
				Class.forName("com.mysql.jdbc.Driver");
			}
			catch (ClassNotFoundException e) {} // ignore

			String url = "jdbc:mysql://" + config.getHost() + "/" + config.getDatabase() + "?allowMultiQueries=true";
			String user = config.getUser();
			String password = config.getPass();

			this.dataSource = new HikariDataSource();
			dataSource.setJdbcUrl(url);
			dataSource.setUsername(user);
			dataSource.setPassword(password);
			dataSource.setRegisterMbeans(true);

			dataSource.setPoolName("BlueLapiz-Core-CP");

			dataSource.setMinimumIdle(2);
			dataSource.setMaximumPoolSize(20);
			dataSource.setLeakDetectionThreshold(4_000);
			dataSource.addDataSourceProperty("useUnicode", "true");
			dataSource.addDataSourceProperty("characterEncoding", "utf-8");
			dataSource.addDataSourceProperty("rewriteBatchedStatements", "true");
			dataSource.addDataSourceProperty("cachePrepStmts", "true");
			dataSource.addDataSourceProperty("prepStmtCacheSize", "250");
			dataSource.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
			dataSource.addDataSourceProperty("useServerPrepStmts", true);
			dataSource.addDataSourceProperty("verifyServerCertificate", false);
			dataSource.addDataSourceProperty("registerMbeans", true);

		} try {
			return dataSource.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return null;
	}

    public HikariDataSource getDataSource() {
        return dataSource;
    }
	
	public void closeConnection() {
		if (dataSource != null) {
			dataSource.close();
		}
	}

	public void close(AutoCloseable...resources) {
	    for(AutoCloseable resource : resources) {
            if (resource != null) {
                try {
                    resource.close();
                } catch (Exception e) {}
            }
        }
	}
}
