/*-----------------------------------------------------------------------------
 - Copyright (C) BlueLapiz.net - All Rights Reserved                          -
 - Unauthorized copying of this file, via any medium is strictly prohibited   -
 - Proprietary and confidential                                               -
 - Written by Alexander Sagen <alexmsagen@gmail.com>                          -
 -----------------------------------------------------------------------------*/

package app.sagen.mysqlutils;

import java.util.UUID;

public class SQLData {

    public static SQLData ofNull() {
        return new SQLData("NULL", MySQLDataType.STRING);
    }

    public static SQLData ofString(String string) {
        if(string == null) return ofNull();
        return new SQLData("'" + string + "'", MySQLDataType.STRING);
    }

    public static SQLData ofInteger(long data) {
        return new SQLData(String.valueOf(data), MySQLDataType.NUMERAL);
    }

    public static SQLData ofDouble(double data) {
        return new SQLData(String.valueOf(data), MySQLDataType.NUMERAL);
    }

    public static SQLData ofUuid(UUID uuid) {
        if(uuid == null) return ofNull();
        return new SQLData("'" + uuid.toString().replace("-", "") + "'", MySQLDataType.STRING);
    }

    public static SQLData ofUuid(String uuid) {
        if(uuid == null) return ofNull();
        return new SQLData("'" + uuid.replace("-", "") + "'", MySQLDataType.STRING);
    }

    private String data;

    private MySQLDataType dataType;

    private SQLData(String data, MySQLDataType dataType) {
        this.data = data;
        this.dataType = dataType;
    }

    @Override
    public String toString()
    {
        return data;
    }
}
