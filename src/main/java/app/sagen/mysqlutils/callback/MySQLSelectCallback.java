/*-----------------------------------------------------------------------------
 - Copyright (C) BlueLapiz.net - All Rights Reserved                          -
 - Unauthorized copying of this file, via any medium is strictly prohibited   -
 - Proprietary and confidential                                               -
 - Written by Alexander Sagen <alexmsagen@gmail.com>                          -
 -----------------------------------------------------------------------------*/

package app.sagen.mysqlutils.callback;

import app.sagen.mysqlutils.MySQLExceptionType;

import java.util.LinkedList;
import java.util.Map;

public interface MySQLSelectCallback {

    void success(LinkedList<Map<String, Object>> result);

    void failure(MySQLExceptionType exceptionType, String errorMessage);

}
