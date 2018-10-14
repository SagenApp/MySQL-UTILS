/*-----------------------------------------------------------------------------
 - Copyright (C) BlueLapiz.net - All Rights Reserved                          -
 - Unauthorized copying of this file, via any medium is strictly prohibited   -
 - Proprietary and confidential                                               -
 - Written by Alexander Sagen <alexmsagen@gmail.com>                          -
 -----------------------------------------------------------------------------*/

package app.sagen.mysqlutils;

import app.sagen.mysqlutils.callback.MySQLCountCallback;
import app.sagen.mysqlutils.callback.MySQLSelectCallback;
import app.sagen.mysqlutils.callback.MySQLUpdateCallback;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Helper class for basic mysql queries.
 */
@SuppressWarnings({"unused", "Duplicates"})
public class MySQLHelper_legacy {

    MySQLHelper_legacy() {
    }

    /**
     * Execute an query in this thread
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public LinkedList<Map<String, Object>> selectSync(String sql) throws SQLException {
        LinkedList<Map<String, Object>> rows = new LinkedList<>();
        //CoreManager.getSkyLandsCore().printSkyLandsDebug(sql, DebugLevel.INFO);
        try (Connection conn = MySQLManager.getMySQLMan().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            rows = getDataFromResultSet(rs);
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rows;
    }

    /**
     * Executes an update in this thread.
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public int updateSync(String sql) throws SQLException {
        int rowsAffected = 0;
        //CoreManager.getSkyLandsCore().printSkyLandsDebug(sql, DebugLevel.INFO);
        try (Connection conn = MySQLManager.getMySQLMan().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            rowsAffected = ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rowsAffected;
    }

    public void select(String sql, MySQLSelectCallback callback) {
        new Thread(() ->
        {
            try {
                callback.success(selectSync(sql));
            } catch (SQLException e) {
                callback.failure(MySQLExceptionType.ERROR, "Error while executing querry!\nError: " + e.getMessage());
                e.printStackTrace();
            }
        }).run();
    }

    public void update(String sql, MySQLUpdateCallback callback) {
        new Thread(() ->
        {
            try {
                callback.success(updateSync(sql));
            } catch (SQLException e) {
                callback.failure(MySQLExceptionType.ERROR, "Error while executing querry!\nError: " + e.getMessage());
                e.printStackTrace();
            }
        }).run();
    }

    /**
     * Updates rows in the database async. Returns the result to the callback when ready.
     *
     * @param table    The table to update from
     * @param callback The callback function
     */
    public void selectAllRowsFrom(String table, MySQLSelectCallback callback) {
        select(table, null, null, MySQLBooleanOperatorType.AND, callback, -1, MySQLOrder.NONE);
    }

    /**
     * Updates one row in the database async. Returns the result to the callback when ready.
     *
     * @param table    The table to update from
     * @param callback The callback function
     */
    public void selectOneRowFrom(String table, MySQLSelectCallback callback) {
        select(table, null, null, MySQLBooleanOperatorType.AND, callback, 1, MySQLOrder.NONE);
    }

    /**
     * Updates one or more rows in the database async. Returns the result to the callback when ready.
     *
     * @param table     The table to update from
     * @param checks    The values to check for stored in a map of column and data
     * @param values    The new values to insert stored in a map of column and data
     * @param callback  The callback function
     */
    public void update(final String table, String[] checks, MySQLBooleanOperatorType type, final Map<String, SQLData> values, MySQLUpdateCallback callback) {
        new Thread(() ->
        {
            try {
                String booleanOperator = (type == null ? " AND " : type.getString());

                // no new updates
                if (values.size() < 1) {
                    callback.failure(MySQLExceptionType.ERROR, "No new values set!");
                    return;
                }

                StringBuilder sql = new StringBuilder("UPDATE " + table + " SET ");

                // add update clause
                boolean first = true;
                for(Map.Entry<String, SQLData> e : values.entrySet()) {
                    if(!first) sql.append(", ");
                    first = false;
                    sql.append(e.getKey()).append(" = ").append(e.getValue());
                }

                // add WHERE clause
                if (checks == null || checks.length > 0) {
                    StringBuilder arguments = new StringBuilder(" WHERE  ");
                    arguments.append(String.join(booleanOperator, checks));
                    sql.append(arguments);
                }
                sql.append(";");

                int affectedRows = updateSync(sql.toString());
                if (affectedRows == 0) {
                    callback.failure(MySQLExceptionType.ERROR, "Error while updating sql!");
                    return;
                }
                callback.success(affectedRows);
            } catch (SQLException e) {
                callback.failure(MySQLExceptionType.ERROR, "Error while updating sql!\nError: " + e.getMessage());
                e.printStackTrace();
            }
        }).run();
    }

    public void updateSync(final String table, String[] checks, MySQLBooleanOperatorType type, final Map<String, SQLData> values, MySQLUpdateCallback callback) {
        try {
            String booleanOperator = (type == null ? " AND " : type.getString());

            // no new updates
            if (values.size() < 1) {
                callback.failure(MySQLExceptionType.ERROR, "No new values set!");
                return;
            }

            StringBuilder sql = new StringBuilder("UPDATE " + table + " SET ");

            // add update clause
            boolean first = true;
            for(Map.Entry<String, SQLData> e : values.entrySet()) {
                if(!first) sql.append(", ");
                first = false;
                sql.append(e.getKey()).append(" = ").append(e.getValue());
            }

            // add WHERE clause
            if (checks == null || checks.length > 0) {
                StringBuilder arguments = new StringBuilder(" WHERE  ");
                arguments.append(String.join(booleanOperator, checks));
                sql.append(arguments);
            }
            sql.append(";");

            int affectedRows = updateSync(sql.toString());
            if (affectedRows == 0) {
                callback.failure(MySQLExceptionType.ERROR, "Error while updating sql!");
                return;
            }
            callback.success(affectedRows);
        } catch (SQLException e) {
            callback.failure(MySQLExceptionType.ERROR, "Error while updating sql!\nError: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void select(final String table, final List<String> columns, String[] checks, MySQLBooleanOperatorType type, final MySQLSelectCallback callback) {
        select(table, columns, checks, type, callback, 0, MySQLOrder.NONE);
    }

    /**
     * Select data from the database async. Returns the result to the callback when ready.
     *
     * @param table    The table to select from
     * @param columns  The columns to select
     * @param checks   The values to check for stored in a map og column and data
     * @param callback The callback function
     * @param limit    The maximum number of results to return
     * @param order    If the result should be ordered or not
     * @param orderBy  If ordering, the tables to order by
     */
    public void select(final String table, final List<String> columns, String[] checks, MySQLBooleanOperatorType type, final MySQLSelectCallback callback, int limit, MySQLOrder order, String... orderBy) {
        new Thread(() ->
        {
            try {
                String booleanOperator = (type == null ? " AND " : type.getString());

                // build initial sql
                List<String> col = columns;
                if (col == null || col.size() == 0) {
                    col = Arrays.asList("*");
                }
                StringBuilder sql = new StringBuilder("SELECT " + String.join(", ", col) + " FROM " + table + "");

                // add WHERE clause
                if (checks != null && checks.length > 0) {
                    StringBuilder arguments = new StringBuilder(" WHERE ");
                    arguments.append(String.join(booleanOperator, checks));
                    sql.append(arguments);
                }

                // add LIMIT clause
                if (limit > 0) {
                    sql.append(" LIMIT " + limit);
                }

                // add ORDER BY clause
                if (order != MySQLOrder.NONE && orderBy.length > 0) {
                    String orderSql = " ORDER BY " + String.join(", ", orderBy) + (order == MySQLOrder.ASC ? " ASC" : " DESC");
                    sql.append(orderSql);
                }
                sql.append(";");

                // Fetch all data
                LinkedList<Map<String, Object>> result = selectSync(sql.toString());
                if(result == null) {
                    callback.failure(MySQLExceptionType.ERROR, "MySQL returned NULL!");
                    return;
                }
                if (result.size() == 0) {
                    callback.failure(MySQLExceptionType.NO_RESULT, "No results from query!");
                    return;
                }

                callback.success(result);
                return;
            } catch (SQLException e) {
                callback.failure(MySQLExceptionType.ERROR, "Could not query result from database!\nError: " + e.getMessage());
                e.printStackTrace();
                return;
            }
        }).run();
    }

    public void selectSync(final String table, final List<String> columns, String[] checks, MySQLBooleanOperatorType type, final MySQLSelectCallback callback) {
        selectSync(table, columns, checks, type, callback, 0, MySQLOrder.NONE);
    }

    public void selectSync(final String table, final List<String> columns, String[] checks, MySQLBooleanOperatorType type, final MySQLSelectCallback callback, int limit, MySQLOrder order, String... orderBy) {
        try {
            String booleanOperator = (type == null ? " AND " : type.getString());

            // build initial sql
            List<String> col = columns;
            if (col == null || col.size() == 0) {
                col = Arrays.asList("*");
            }
            StringBuilder sql = new StringBuilder("SELECT " + String.join(", ", col) + " FROM " + table + "");

            // add WHERE clause
            if (checks != null && checks.length > 0) {
                StringBuilder arguments = new StringBuilder(" WHERE ");
                arguments.append(String.join(booleanOperator, checks));
                sql.append(arguments);
            }

            // add LIMIT clause
            if (limit > 0) {
                sql.append(" LIMIT " + limit);
            }

            // add ORDER BY clause
            if (order != MySQLOrder.NONE && orderBy.length > 0) {
                String orderSql = " ORDER BY " + String.join(", ", orderBy) + (order == MySQLOrder.ASC ? " ASC" : " DESC");
                sql.append(orderSql);
            }
            sql.append(";");

            // Fetch all data
            LinkedList<Map<String, Object>> result = selectSync(sql.toString());
            if(result == null) {
                callback.failure(MySQLExceptionType.ERROR, "MySQL returned NULL!");
                return;
            }
            if (result.size() == 0) {
                callback.failure(MySQLExceptionType.NO_RESULT, "No results from query!");
                return;
            }

            callback.success(result);
            return;
        } catch (SQLException e) {
            callback.failure(MySQLExceptionType.ERROR, "Could not query result from database!\nError: " + e.getMessage());
            e.printStackTrace();
            return;
        }
    }

    /**
     * Delete from the database async. Calls the callback function with the result when done
     *
     * @param table    The table to delete from
     * @param checks   The values to check for (The WHERE clause)
     * @param callback The callback function
     */
    public void delete(final String table, String[] checks, MySQLBooleanOperatorType type, final MySQLUpdateCallback callback) {
        new Thread(() ->
        {
            try {
                String booleanOperator = (type == null ? " AND " : type.getString());

                if (checks.length < 1) {
                    callback.failure(MySQLExceptionType.ERROR, "No where clause set!");
                    return;
                }
                StringBuilder sql = new StringBuilder("DELETE FROM " + table);

                StringBuilder arguments = new StringBuilder(" WHERE ");
                arguments.append(String.join(booleanOperator, checks));
                sql.append(arguments);
                sql.append(";");

                int affectedRows = updateSync(sql.toString());

                callback.success(affectedRows);
            } catch (SQLException e) {
                callback.failure(MySQLExceptionType.ERROR, "Error with mysql update!\nError: " + e.getMessage());
                e.printStackTrace();
            }
        }).run();
    }

    public void countRows(final String table, String[] checks, MySQLBooleanOperatorType type, MySQLCountCallback callback) {
        new Thread(() ->
        {
            try {
                String booleanOperator = (type == null ? " AND " : type.getString());

                StringBuilder sql = new StringBuilder("SELECT COUNT(*) AS COUNT FROM " + table);

                if (checks.length > 0) {
                    StringBuilder arguments = new StringBuilder(" WHERE ");
                    arguments.append(String.join(booleanOperator, checks));
                    sql.append(arguments);
                }
                sql.append(";");

                LinkedList<Map<String, Object>> result = selectSync(sql.toString());

                if (result.size() == 0) {
                    callback.failure(MySQLExceptionType.ERROR, "Error with mysql select count(*)! No result returned!");
                    return;
                }

                callback.success((Long) (result.getFirst()).get("COUNT"));
            } catch (SQLException e) {
                callback.failure(MySQLExceptionType.ERROR, "Error with mysql update!\nError: " + e.getMessage());
                e.printStackTrace();
            }
        }).run();
    }

    /**
     * Insert data to the database async. Calls back the callback function when done with the result.
     *
     * @param table    The table to insert into
     * @param values   The values to insert stored in a Map of column and data
     * @param callback The callback function
     */
    public void insert(final String table, final Map<String, SQLData> values, final MySQLUpdateCallback callback) {
        new Thread(() ->
        {
            try {
                if (values.size() < 1) {
                    callback.failure(MySQLExceptionType.ERROR, "No values clause set!");
                    return;
                }
                StringBuilder sql = new StringBuilder("REPLACE INTO " + table);

                StringBuilder columns = new StringBuilder("(");
                StringBuilder vals = new StringBuilder("(");
                boolean first = true;
                for (Map.Entry<String, SQLData> e : values.entrySet()) {
                    if (!first) {
                        columns.append(", ");
                        vals.append(", ");
                    }
                    first = false;

                    columns.append(e.getKey());
                    vals.append(e.getValue());
                }
                columns.append(")");
                vals.append(")");

                sql.append(columns + " VALUES " + vals + ";");

                int affectedRows = updateSync(sql.toString());

                callback.success(affectedRows);
            } catch (SQLException e) {
                callback.failure(MySQLExceptionType.ERROR, "Error with mysql update!\nError: " + e.getMessage());
                e.printStackTrace();
            }
        }).run();
    }

    public void updateMultipleRowsAddNumber(String table, String keyToChange, String columnToChange, Map<String, Integer> valuesToAdd, final MySQLUpdateCallback callback) {
        new Thread(() ->
        {
            try {
                if (valuesToAdd.size() == 0) {
                    callback.failure(MySQLExceptionType.ERROR, "No new values set!");
                    return;
                }

                StringBuilder sql = new StringBuilder("UPDATE " + table + " SET " + columnToChange + " = " + columnToChange + " + (CASE " + keyToChange);

                for (Map.Entry<String, Integer> e : valuesToAdd.entrySet()) {
                    sql.append(" WHEN " + e.getKey() + " THEN " + e.getValue());
                }

                sql.append(" END) WHERE " + keyToChange + " IN (" + String.join(", ", valuesToAdd.keySet()) + ");");

                int affectedRows = updateSync(sql.toString());

                callback.success(affectedRows);
            } catch (SQLException e) {
                callback.failure(MySQLExceptionType.ERROR, "Error with mysql update!\nError: " + e.getMessage());
                e.printStackTrace();
            }
        }).run();
    }

    public void updateMultipleRowsSetValue(String table, String keyToChange, String columnToChange, Map<String, SQLData> valuesToSet, final MySQLUpdateCallback callback) {
        new Thread(() ->
        {
            try {
                if (valuesToSet.size() == 0) {
                    callback.failure(MySQLExceptionType.ERROR, "No new values set!");
                    return;
                }

                StringBuilder sql = new StringBuilder("UPDATE " + table + " SET " + columnToChange + " = (CASE " + keyToChange);

                for (Map.Entry<String, SQLData> e : valuesToSet.entrySet()) {
                    sql.append(" WHEN " + e.getKey() + " THEN " + e.getValue());
                }

                sql.append(" END) WHERE " + keyToChange + " IN (" + String.join(", ", valuesToSet.keySet()) + ");");

                int affectedRows = updateSync(sql.toString());

                callback.success(affectedRows);
            } catch (SQLException e) {
                callback.failure(MySQLExceptionType.ERROR, "Error with mysql update!\nError: " + e.getMessage());
                e.printStackTrace();
            }
        }).run();
    }

    /**
     * Copy data from ResultSet to a LinkedList of Maps
     *
     * @param rs The ResultSet
     * @return The values in a list of maps
     * @throws SQLException
     */
    private LinkedList<Map<String, Object>> getDataFromResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        LinkedList<Map<String, Object>> rows = new LinkedList<>();
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= columns; i++) {
                row.put(md.getColumnName(i), rs.getObject(i));
            }
            rows.push(row);
        }
        return rows;
    }

    /**
     * Querries the server for the list of tables in the database
     *
     * @return The list of tables
     */
    public List<String> getTables() {
        try (Connection conn = MySQLManager.getMySQLMan().getConnection()) {
            DatabaseMetaData dbm = conn.getMetaData();
            ResultSet rs = dbm.getTables(conn.getCatalog().toLowerCase(), null, "%", new String[]{"TABLE"});
            ArrayList<String> list = new ArrayList<>();
            while (rs != null && rs.next()) {
                list.add(rs.getString("TABLE_NAME").toLowerCase());
            }
            rs.close();
            return list;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }
}
