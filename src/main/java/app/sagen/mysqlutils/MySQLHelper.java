/*-----------------------------------------------------------------------------
 - Copyright (C) BlueLapiz.net - All Rights Reserved                          -
 - Unauthorized copying of this file, via any medium is strictly prohibited   -
 - Proprietary and confidential                                               -
 - Written by Alexander Sagen <alexmsagen@gmail.com>                          -
 -----------------------------------------------------------------------------*/

package app.sagen.mysqlutils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Helper class for basic mysql queries.
 */
@SuppressWarnings({"unused", "Duplicates"})
public class MySQLHelper {

    MySQLHelper() {
    }

    public CompletableFuture<LinkedList<Map<String, Object>>> select(String sql) {
        return CompletableFuture.supplyAsync(() -> {
            LinkedList<Map<String, Object>> rows = new LinkedList<>();
            try (Connection conn = MySQLManager.getMySQLMan().getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql);
                 ResultSet rs = ps.executeQuery();) {
                return getDataFromResultSet(rs);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Exception while executing select statement! Error: " + e.getMessage());
            }
        });
    }

    public CompletableFuture<Integer> update(String sql) {
        return CompletableFuture.supplyAsync(() -> {
            int rowsAffected = 0;
            try (Connection conn = MySQLManager.getMySQLMan().getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                rowsAffected = ps.executeUpdate();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Exception while executing update statement! Error: " + e.getMessage());
            }
            return rowsAffected;
        });
    }

    public CompletableFuture<Integer> update(final String table, String[] checks, MySQLBooleanOperatorType type, final Map<String, SQLData> values) {
        return CompletableFuture.supplyAsync(() -> {
            String booleanOperator = (type == null ? " AND " : type.getString());

            if (values.size() < 1) {
                throw new IllegalArgumentException("No new values is set!");
            }

            StringBuilder sql = new StringBuilder("UPDATE " + table + " SET ");

            // add update clause
            boolean first = true;
            for (Map.Entry<String, SQLData> e : values.entrySet()) {
                if (!first) sql.append(", ");
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
            return sql.toString();
        }).thenCompose(this::update);
    }

    public CompletableFuture<LinkedList<Map<String, Object>>> selectAllRowsFrom(String table) {
        return select(table, null, null, MySQLBooleanOperatorType.AND, -1, MySQLOrder.NONE);
    }

    public CompletableFuture<Map<String, Object>> selectOneRowFrom(String table) {
        return select(table, null, null, MySQLBooleanOperatorType.AND, 1, MySQLOrder.NONE)
                .thenApply(LinkedList::getFirst);
    }

    public CompletableFuture<LinkedList<Map<String, Object>>> select(final String table, final List<String> columns, String[] checks, MySQLBooleanOperatorType type) {
        return select(table, columns, checks, type, 0, MySQLOrder.NONE);
    }

    public CompletableFuture<LinkedList<Map<String, Object>>> select(final String table, final List<String> columns, String[] checks, MySQLBooleanOperatorType type, int limit, MySQLOrder order, String... orderBy) {
        return CompletableFuture.supplyAsync(() -> {
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

            return sql.toString();
        }).thenCompose(this::select);
    }

    public CompletableFuture<Integer> delete(final String table, String[] checks, MySQLBooleanOperatorType type) {
        return CompletableFuture.supplyAsync(() -> {
            String booleanOperator = (type == null ? " AND " : type.getString());

            if (checks.length < 1) {
                throw new IllegalArgumentException("No where clause set!");
            }
            StringBuilder sql = new StringBuilder("DELETE FROM " + table);

            StringBuilder arguments = new StringBuilder(" WHERE ");
            arguments.append(String.join(booleanOperator, checks));
            sql.append(arguments);
            return sql.append(";").toString();
        }).thenCompose(this::update);
    }

    public CompletableFuture<Long> countRows(final String table, String[] checks, MySQLBooleanOperatorType type) {
        return CompletableFuture.supplyAsync(() -> {
            String booleanOperator = (type == null ? " AND " : type.getString());

            StringBuilder sql = new StringBuilder("SELECT COUNT(*) AS COUNT FROM " + table);

            if (checks.length > 0) {
                StringBuilder arguments = new StringBuilder(" WHERE ");
                arguments.append(String.join(booleanOperator, checks));
                sql.append(arguments);
            }
            sql.append(";");
            return sql.toString();
        }).thenCompose(this::select).thenApply(v -> {
            if (v.size() == 0) {
                throw new RuntimeException("No result returned in count statuement!");
            }
            return (Long) v.getFirst().get("COUNT");
        });
    }

    public CompletableFuture<List<Integer>> insert(final String table, final Map<String, SQLData> values) {
        return CompletableFuture.supplyAsync(() -> {
            if (values.size() < 1) {
                throw new IllegalArgumentException("No where clause set!");
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
            sql.append(columns).append(" VALUES ").append(vals).append(";");
            return sql.toString();
        }).thenCompose((String sql) -> CompletableFuture.supplyAsync(() -> {
            try (Connection conn = MySQLManager.getMySQLMan().getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                 ps.executeUpdate();
                 ResultSet rs = ps.getGeneratedKeys();
                 List<Integer> keys = new ArrayList<>();
                 while(rs.next()) {
                    keys.add(rs.getInt(1));
                 }
                 return keys;
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Exception while executing update statement! Error: " + e.getMessage());
            }
        }));
    }

    public CompletableFuture<Integer> updateMultipleRowsAddNumber(String table, String keyToChange, String columnToChange, Map<String, Integer> valuesToAdd) {
        return CompletableFuture.supplyAsync(() -> {
            if (valuesToAdd.size() == 0) {
                throw new IllegalArgumentException("No new values is set!");
            }

            StringBuilder sql = new StringBuilder("UPDATE " + table + " SET " + columnToChange + " = " + columnToChange + " + (CASE " + keyToChange);

            for (Map.Entry<String, Integer> e : valuesToAdd.entrySet()) {
                sql.append(" WHEN " + e.getKey() + " THEN " + e.getValue());
            }

            sql.append(" END) WHERE " + keyToChange + " IN (" + String.join(", ", valuesToAdd.keySet()) + ");");
            return sql.toString();
        }).thenCompose(this::update);
    }

    public CompletableFuture<Integer> updateMultipleRowsSetValue(String table, String keyToChange, String columnToChange, Map<String, SQLData> valuesToSet) {
        return CompletableFuture.supplyAsync(() -> {
            if (valuesToSet.size() == 0) {
                throw new IllegalArgumentException("No new values is set!");
            }

            StringBuilder sql = new StringBuilder("UPDATE " + table + " SET " + columnToChange + " = (CASE " + keyToChange);

            for (Map.Entry<String, SQLData> e : valuesToSet.entrySet()) {
                sql.append(" WHEN " + e.getKey() + " THEN " + e.getValue());
            }

            sql.append(" END) WHERE " + keyToChange + " IN (" + String.join(", ", valuesToSet.keySet()) + ");");
            return sql.toString();
        }).thenCompose(this::update);
    }

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
