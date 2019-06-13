package org.apache.spark.sql.delta.sources.mysql.binlog.io;

/**
 * 2019-06-13 WilliamZhu(allwefantasy@gmail.com)
 */
public class EventInfo {

    // Event type constants
    public static String BEGIN_EVENT = "begin";
    public static String COMMIT_EVENT = "commit";
    public static String INSERT_EVENT = "insert";
    public static String DELETE_EVENT = "delete";
    public static String UPDATE_EVENT = "update";
    public static String DDL_EVENT = "ddl";


}
