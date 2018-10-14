/*-----------------------------------------------------------------------------
 - Copyright (C) BlueLapiz.net - All Rights Reserved                          -
 - Unauthorized copying of this file, via any medium is strictly prohibited   -
 - Proprietary and confidential                                               -
 - Written by Alexander Sagen <alexmsagen@gmail.com>                          -
 -----------------------------------------------------------------------------*/

package app.sagen.mysqlutils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class MySQLTableHelper {

    private static final HashMap<String, String> tableCreators = new HashMap<>();

    private static final ArrayList<String> tablesCreated = new ArrayList<>();

    public void addTable(String tableName, String createStatement) {
        if(tableCreators.containsKey(tableName.toLowerCase()))
            throw new IllegalStateException("Cannot create multiple tables with the same name!\nTable: " + tableName);
        tableCreators.put(tableName.toLowerCase(), createStatement);
    }

    public void createTablesNotExisting() {
        List<String> existingTables = MySQLManager.getSQLHelper_legacy().getTables().stream()
                .map(String::toLowerCase)
                .collect(Collectors.toList());

        tableCreators.forEach((k, v) -> {
            if(existingTables.contains(k.toLowerCase())) return;

            try {
                MySQLManager.getSQLHelper_legacy().updateSync(v.replace("%table", k));
                tablesCreated.add(k);
            } catch (SQLException e) {
                System.out.println("Could not create table!!\nError: " + e.getMessage() + "\nSql: " + v);
                e.printStackTrace();
            }
        });
    }
}
