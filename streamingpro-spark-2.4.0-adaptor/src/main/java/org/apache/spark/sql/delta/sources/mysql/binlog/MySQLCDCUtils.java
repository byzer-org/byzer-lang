package org.apache.spark.sql.delta.sources.mysql.binlog;

import java.io.Serializable;

/**
 * 2019-06-13 WilliamZhu(allwefantasy@gmail.com)
 */
public class MySQLCDCUtils {

    public static Object getWritableObject(Serializable value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return new String((byte[]) value);
        }
        return value;
    }
}
