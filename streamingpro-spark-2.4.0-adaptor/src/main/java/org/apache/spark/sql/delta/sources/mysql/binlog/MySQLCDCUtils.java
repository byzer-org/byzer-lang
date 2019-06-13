package org.apache.spark.sql.delta.sources.mysql.binlog;

import java.io.Serializable;

/**
 * 2019-06-13 WilliamZhu(allwefantasy@gmail.com)
 */
public class MySQLCDCUtils {

    public static Object getWritableObject(Integer type, Serializable value) {
        if (value == null) {
            return null;
        }
        if (type == null) {
            if (value instanceof byte[]) {
                return new String((byte[]) value);
            } else if (value instanceof Number) {
                return value;
            }
        } else if (value instanceof Number) {
            return value;
        } else {
            if (value instanceof byte[]) {
                return new String((byte[]) value);
            } else {
                return value.toString();
            }
        }
        return null;
    }
}
