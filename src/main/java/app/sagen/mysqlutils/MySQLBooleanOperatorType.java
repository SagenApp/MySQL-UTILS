/*-----------------------------------------------------------------------------
 - Copyright (C) BlueLapiz.net - All Rights Reserved                          -
 - Unauthorized copying of this file, via any medium is strictly prohibited   -
 - Proprietary and confidential                                               -
 - Written by Alexander Sagen <alexmsagen@gmail.com>                          -
 -----------------------------------------------------------------------------*/

package app.sagen.mysqlutils;

public enum MySQLBooleanOperatorType {

    AND(" AND "),
    OR(" OR ");

    private String string;

    MySQLBooleanOperatorType(String string) {
        this.string = string;
    }

    public String getString() {
        return string;
    }
}
