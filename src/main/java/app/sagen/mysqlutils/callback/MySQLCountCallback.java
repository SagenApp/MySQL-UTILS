/*-----------------------------------------------------------------------------
 - Copyright (C) BlueLapiz.net - All Rights Reserved                          -
 - Unauthorized copying of this file, via any medium is strictly prohibited   -
 - Proprietary and confidential                                               -
 - Written by Alexander Sagen <alexmsagen@gmail.com>                          -
 -----------------------------------------------------------------------------*/

package app.sagen.mysqlutils.callback;

import app.sagen.mysqlutils.MySQLExceptionType;

public interface MySQLCountCallback {
    void success(long result);

    void failure(MySQLExceptionType exceptionType, String errorMessage);
}
