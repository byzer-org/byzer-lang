package tech.mlsql.autosuggest.app;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLType;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;



public enum MysqlType implements SQLType {

    /**
     * DECIMAL[(M[,D])] [UNSIGNED] [ZEROFILL]
     * A packed "exact" fixed-point number. M is the total number of digits (the precision) and D is the number of digits
     * after the decimal point (the scale). The decimal point and (for negative numbers) the "-" sign are not counted in M.
     * If D is 0, values have no decimal point or fractional part. The maximum number of digits (M) for DECIMAL is 65.
     * The maximum number of supported decimals (D) is 30. If D is omitted, the default is 0. If M is omitted, the default is 10.
     *
     * Protocol: FIELD_TYPE_DECIMAL = 0
     * Protocol: FIELD_TYPE_NEWDECIMAL = 246
     *
     * These types are synonyms for DECIMAL:
     * DEC[(M[,D])] [UNSIGNED] [ZEROFILL],
     * NUMERIC[(M[,D])] [UNSIGNED] [ZEROFILL],
     * FIXED[(M[,D])] [UNSIGNED] [ZEROFILL]
     */
    DECIMAL("DECIMAL", Types.DECIMAL, BigDecimal.class, MysqlType.FIELD_FLAG_ZEROFILL, MysqlType.IS_DECIMAL, 65L, "[(M[,D])] [UNSIGNED] [ZEROFILL]"),
    /**
     * DECIMAL[(M[,D])] UNSIGNED [ZEROFILL]
     *
     * @see MysqlType#DECIMAL
     */
    DECIMAL_UNSIGNED("DECIMAL UNSIGNED", Types.DECIMAL, BigDecimal.class, MysqlType.FIELD_FLAG_UNSIGNED | MysqlType.FIELD_FLAG_ZEROFILL, MysqlType.IS_DECIMAL,
            65L, "[(M[,D])] [UNSIGNED] [ZEROFILL]"),
    /**
     * TINYINT[(M)] [UNSIGNED] [ZEROFILL]
     * A very small integer. The signed range is -128 to 127. The unsigned range is 0 to 255.
     *
     * Protocol: FIELD_TYPE_TINY = 1
     */
    TINYINT("TINYINT", Types.TINYINT, Integer.class, MysqlType.FIELD_FLAG_ZEROFILL, MysqlType.IS_DECIMAL, 3L, "[(M)] [UNSIGNED] [ZEROFILL]"),
    /**
     * TINYINT[(M)] UNSIGNED [ZEROFILL]
     *
     * @see MysqlType#TINYINT
     */
    TINYINT_UNSIGNED("TINYINT UNSIGNED", Types.TINYINT, Integer.class, MysqlType.FIELD_FLAG_UNSIGNED | MysqlType.FIELD_FLAG_ZEROFILL, MysqlType.IS_DECIMAL, 3L,
            "[(M)] [UNSIGNED] [ZEROFILL]"),
    /**
     * BOOL, BOOLEAN
     * These types are synonyms for TINYINT(1). A value of zero is considered false. Nonzero values are considered true
     *
     * BOOLEAN is converted to TINYINT(1) during DDL execution i.e. it has the same precision=3. Thus we have to
     * look at full data type name and convert TINYINT to BOOLEAN (or BIT) if it has "(1)" length specification.
     *
     * Protocol: FIELD_TYPE_TINY = 1
     */
    BOOLEAN("BOOLEAN", Types.BOOLEAN, Boolean.class, 0, MysqlType.IS_NOT_DECIMAL, 3L, ""),
    /**
     * SMALLINT[(M)] [UNSIGNED] [ZEROFILL]
     * A small integer. The signed range is -32768 to 32767. The unsigned range is 0 to 65535.
     *
     * Protocol: FIELD_TYPE_SHORT = 2
     */
    SMALLINT("SMALLINT", Types.SMALLINT, Integer.class, MysqlType.FIELD_FLAG_ZEROFILL, MysqlType.IS_DECIMAL, 5L, "[(M)] [UNSIGNED] [ZEROFILL]"),
    /**
     * SMALLINT[(M)] UNSIGNED [ZEROFILL]
     *
     * @see MysqlType#SMALLINT
     */
    SMALLINT_UNSIGNED("SMALLINT UNSIGNED", Types.SMALLINT, Integer.class, MysqlType.FIELD_FLAG_UNSIGNED | MysqlType.FIELD_FLAG_ZEROFILL, MysqlType.IS_DECIMAL,
            5L, "[(M)] [UNSIGNED] [ZEROFILL]"),
    /**
     * INT[(M)] [UNSIGNED] [ZEROFILL]
     * A normal-size integer. The signed range is -2147483648 to 2147483647. The unsigned range is 0 to 4294967295.
     *
     * Protocol: FIELD_TYPE_LONG = 3
     *
     * INTEGER[(M)] [UNSIGNED] [ZEROFILL] is a synonym for INT.
     */
    INT("INT", Types.INTEGER, Integer.class, MysqlType.FIELD_FLAG_ZEROFILL, MysqlType.IS_DECIMAL, 10L, "[(M)] [UNSIGNED] [ZEROFILL]"),
    /**
     * INT[(M)] UNSIGNED [ZEROFILL]
     *
     * @see MysqlType#INT
     */
    INT_UNSIGNED("INT UNSIGNED", Types.INTEGER, Long.class, MysqlType.FIELD_FLAG_UNSIGNED | MysqlType.FIELD_FLAG_ZEROFILL, MysqlType.IS_DECIMAL, 10L,
            "[(M)] [UNSIGNED] [ZEROFILL]"),
    /**
     * FLOAT[(M,D)] [UNSIGNED] [ZEROFILL]
     * A small (single-precision) floating-point number. Permissible values are -3.402823466E+38 to -1.175494351E-38, 0,
     * and 1.175494351E-38 to 3.402823466E+38. These are the theoretical limits, based on the IEEE standard. The actual
     * range might be slightly smaller depending on your hardware or operating system.
     *
     * M is the total number of digits and D is the number of digits following the decimal point. If M and D are omitted,
     * values are stored to the limits permitted by the hardware. A single-precision floating-point number is accurate to
     * approximately 7 decimal places.
     *
     * Protocol: FIELD_TYPE_FLOAT = 4
     *
     * Additionally:
     * FLOAT(p) [UNSIGNED] [ZEROFILL]
     * A floating-point number. p represents the precision in bits, but MySQL uses this value only to determine whether
     * to use FLOAT or DOUBLE for the resulting data type. If p is from 0 to 24, the data type becomes FLOAT with no M or D values.
     * If p is from 25 to 53, the data type becomes DOUBLE with no M or D values. The range of the resulting column is the same as
     * for the single-precision FLOAT or double-precision DOUBLE data types.
     */
    FLOAT("FLOAT", Types.REAL, Float.class, MysqlType.FIELD_FLAG_ZEROFILL, MysqlType.IS_DECIMAL, 12L, "[(M,D)] [UNSIGNED] [ZEROFILL]"),
    /**
     * FLOAT[(M,D)] UNSIGNED [ZEROFILL]
     *
     * @see MysqlType#FLOAT
     */
    FLOAT_UNSIGNED("FLOAT UNSIGNED", Types.REAL, Float.class, MysqlType.FIELD_FLAG_UNSIGNED | MysqlType.FIELD_FLAG_ZEROFILL, MysqlType.IS_DECIMAL, 12L,
            "[(M,D)] [UNSIGNED] [ZEROFILL]"),
    /**
     * DOUBLE[(M,D)] [UNSIGNED] [ZEROFILL]
     * A normal-size (double-precision) floating-point number. Permissible values are -1.7976931348623157E+308 to
     * -2.2250738585072014E-308, 0, and 2.2250738585072014E-308 to 1.7976931348623157E+308. These are the theoretical limits,
     * based on the IEEE standard. The actual range might be slightly smaller depending on your hardware or operating system.
     *
     * M is the total number of digits and D is the number of digits following the decimal point. If M and D are omitted,
     * values are stored to the limits permitted by the hardware. A double-precision floating-point number is accurate to
     * approximately 15 decimal places.
     *
     * Protocol: FIELD_TYPE_DOUBLE = 5
     *
     * These types are synonyms for DOUBLE:
     * DOUBLE PRECISION[(M,D)] [UNSIGNED] [ZEROFILL],
     * REAL[(M,D)] [UNSIGNED] [ZEROFILL]. Exception: If the REAL_AS_FLOAT SQL mode is enabled, REAL is a synonym for FLOAT rather than DOUBLE.
     */
    DOUBLE("DOUBLE", Types.DOUBLE, Double.class, MysqlType.FIELD_FLAG_ZEROFILL, MysqlType.IS_DECIMAL, 22L, "[(M,D)] [UNSIGNED] [ZEROFILL]"),
    /**
     * DOUBLE[(M,D)] UNSIGNED [ZEROFILL]
     *
     * @see MysqlType#DOUBLE
     */
    DOUBLE_UNSIGNED("DOUBLE UNSIGNED", Types.DOUBLE, Double.class, MysqlType.FIELD_FLAG_UNSIGNED | MysqlType.FIELD_FLAG_ZEROFILL, MysqlType.IS_DECIMAL, 22L,
            "[(M,D)] [UNSIGNED] [ZEROFILL]"),
    /**
     * FIELD_TYPE_NULL = 6
     */
    NULL("NULL", Types.NULL, Object.class, 0, MysqlType.IS_NOT_DECIMAL, 0L, ""),
    /**
     * TIMESTAMP[(fsp)]
     * A timestamp. The range is '1970-01-01 00:00:01.000000' UTC to '2038-01-19 03:14:07.999999' UTC.
     * TIMESTAMP values are stored as the number of seconds since the epoch ('1970-01-01 00:00:00' UTC).
     * A TIMESTAMP cannot represent the value '1970-01-01 00:00:00' because that is equivalent to 0 seconds
     * from the epoch and the value 0 is reserved for representing '0000-00-00 00:00:00', the "zero" TIMESTAMP value.
     * An optional fsp value in the range from 0 to 6 may be given to specify fractional seconds precision. A value
     * of 0 signifies that there is no fractional part. If omitted, the default precision is 0.
     *
     * Protocol: FIELD_TYPE_TIMESTAMP = 7
     *
     */
    // TODO If MySQL server run with the MAXDB SQL mode enabled, TIMESTAMP is identical with DATETIME. If this mode is enabled at the time that a table is created, TIMESTAMP columns are created as DATETIME columns.
    // As a result, such columns use DATETIME display format, have the same range of values, and there is no automatic initialization or updating to the current date and time
    TIMESTAMP("TIMESTAMP", Types.TIMESTAMP, Timestamp.class, 0, MysqlType.IS_NOT_DECIMAL, 26L, "[(fsp)]"),
    /**
     * BIGINT[(M)] [UNSIGNED] [ZEROFILL]
     * A large integer. The signed range is -9223372036854775808 to 9223372036854775807. The unsigned range is 0 to 18446744073709551615.
     *
     * Protocol: FIELD_TYPE_LONGLONG = 8
     *
     * SERIAL is an alias for BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE.
     */
    BIGINT("BIGINT", Types.BIGINT, Long.class, MysqlType.FIELD_FLAG_ZEROFILL, MysqlType.IS_DECIMAL, 19L, "[(M)] [UNSIGNED] [ZEROFILL]"),
    /**
     * BIGINT[(M)] UNSIGNED [ZEROFILL]
     *
     * @see MysqlType#BIGINT
     */
    BIGINT_UNSIGNED("BIGINT UNSIGNED", Types.BIGINT, BigInteger.class, MysqlType.FIELD_FLAG_UNSIGNED | MysqlType.FIELD_FLAG_ZEROFILL, MysqlType.IS_DECIMAL, 20L,
            "[(M)] [UNSIGNED] [ZEROFILL]"),
    /**
     * MEDIUMINT[(M)] [UNSIGNED] [ZEROFILL]
     * A medium-sized integer. The signed range is -8388608 to 8388607. The unsigned range is 0 to 16777215.
     *
     * Protocol: FIELD_TYPE_INT24 = 9
     */
    MEDIUMINT("MEDIUMINT", Types.INTEGER, Integer.class, MysqlType.FIELD_FLAG_ZEROFILL, MysqlType.IS_DECIMAL, 7L, "[(M)] [UNSIGNED] [ZEROFILL]"),
    /**
     * MEDIUMINT[(M)] UNSIGNED [ZEROFILL]
     *
     * @see MysqlType#MEDIUMINT
     */
    MEDIUMINT_UNSIGNED("MEDIUMINT UNSIGNED", Types.INTEGER, Integer.class, MysqlType.FIELD_FLAG_UNSIGNED | MysqlType.FIELD_FLAG_ZEROFILL, MysqlType.IS_DECIMAL,
            8L, "[(M)] [UNSIGNED] [ZEROFILL]"),
    /**
     * DATE
     * A date. The supported range is '1000-01-01' to '9999-12-31'. MySQL displays DATE values in 'YYYY-MM-DD' format,
     * but permits assignment of values to DATE columns using either strings or numbers.
     *
     * Protocol: FIELD_TYPE_DATE = 10
     */
    DATE("DATE", Types.DATE, Date.class, 0, MysqlType.IS_NOT_DECIMAL, 10L, ""),
    /**
     * TIME[(fsp)]
     * A time. The range is '-838:59:59.000000' to '838:59:59.000000'. MySQL displays TIME values in
     * 'HH:MM:SS[.fraction]' format, but permits assignment of values to TIME columns using either strings or numbers.
     * An optional fsp value in the range from 0 to 6 may be given to specify fractional seconds precision. A value
     * of 0 signifies that there is no fractional part. If omitted, the default precision is 0.
     *
     * Protocol: FIELD_TYPE_TIME = 11
     */
    TIME("TIME", Types.TIME, Time.class, 0, MysqlType.IS_NOT_DECIMAL, 16L, "[(fsp)]"),
    /**
     * DATETIME[(fsp)]
     * A date and time combination. The supported range is '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'.
     * MySQL displays DATETIME values in 'YYYY-MM-DD HH:MM:SS[.fraction]' format, but permits assignment of values to
     * DATETIME columns using either strings or numbers.
     * An optional fsp value in the range from 0 to 6 may be given to specify fractional seconds precision. A value
     * of 0 signifies that there is no fractional part. If omitted, the default precision is 0.
     *
     * Protocol: FIELD_TYPE_DATETIME = 12
     */
    DATETIME("DATETIME", Types.TIMESTAMP, Timestamp.class, 0, MysqlType.IS_NOT_DECIMAL, 26L, "[(fsp)]"),
    /**
     * YEAR[(4)]
     * A year in four-digit format. MySQL displays YEAR values in YYYY format, but permits assignment of
     * values to YEAR columns using either strings or numbers. Values display as 1901 to 2155, and 0000.
     * Protocol: FIELD_TYPE_YEAR = 13
     */
    YEAR("YEAR", Types.DATE, Date.class, 0, MysqlType.IS_NOT_DECIMAL, 4L, "[(4)]"),
    /**
     * [NATIONAL] VARCHAR(M) [CHARACTER SET charset_name] [COLLATE collation_name]
     * A variable-length string. M represents the maximum column length in characters. The range of M is 0 to 65,535.
     * The effective maximum length of a VARCHAR is subject to the maximum row size (65,535 bytes, which is shared among
     * all columns) and the character set used. For example, utf8 characters can require up to three bytes per character,
     * so a VARCHAR column that uses the utf8 character set can be declared to be a maximum of 21,844 characters.
     *
     * MySQL stores VARCHAR values as a 1-byte or 2-byte length prefix plus data. The length prefix indicates the number
     * of bytes in the value. A VARCHAR column uses one length byte if values require no more than 255 bytes, two length
     * bytes if values may require more than 255 bytes.
     *
     * Note
     * MySQL 5.7 follows the standard SQL specification, and does not remove trailing spaces from VARCHAR values.
     *
     * VARCHAR is shorthand for CHARACTER VARYING. NATIONAL VARCHAR is the standard SQL way to define that a VARCHAR
     * column should use some predefined character set. MySQL 4.1 and up uses utf8 as this predefined character set.
     * NVARCHAR is shorthand for NATIONAL VARCHAR.
     *
     * Protocol: FIELD_TYPE_VARCHAR = 15
     * Protocol: FIELD_TYPE_VAR_STRING = 253
     */
    VARCHAR("VARCHAR", Types.VARCHAR, String.class, 0, MysqlType.IS_NOT_DECIMAL, 65535L, "(M) [CHARACTER SET charset_name] [COLLATE collation_name]"),
    /**
     * VARBINARY(M)
     * The VARBINARY type is similar to the VARCHAR type, but stores binary byte strings rather than nonbinary
     * character strings. M represents the maximum column length in bytes.
     *
     * Protocol: FIELD_TYPE_VARCHAR = 15
     * Protocol: FIELD_TYPE_VAR_STRING = 253
     */
    VARBINARY("VARBINARY", Types.VARBINARY, null, 0, MysqlType.IS_NOT_DECIMAL, 65535L, "(M)"),
    /**
     * BIT[(M)]
     * A bit-field type. M indicates the number of bits per value, from 1 to 64. The default is 1 if M is omitted.
     * Protocol: FIELD_TYPE_BIT = 16
     */
    BIT("BIT", Types.BIT, Boolean.class, 0, MysqlType.IS_DECIMAL, 1L, "[(M)]"), // TODO maybe precision=8 ?
    /**
     * The size of JSON documents stored in JSON columns is limited to the value of the max_allowed_packet system variable (max value 1073741824).
     * (While the server manipulates a JSON value internally in memory, it can be larger; the limit applies when the server stores it.)
     *
     * Protocol: FIELD_TYPE_BIT = 245
     */
    JSON("JSON", Types.LONGVARCHAR, String.class, 0, MysqlType.IS_NOT_DECIMAL, 1073741824L, ""),
    /**
     * ENUM('value1','value2',...) [CHARACTER SET charset_name] [COLLATE collation_name]
     * An enumeration. A string object that can have only one value, chosen from the list of values 'value1',
     * 'value2', ..., NULL or the special '' error value. ENUM values are represented internally as integers.
     * An ENUM column can have a maximum of 65,535 distinct elements. (The practical limit is less than 3000.)
     * A table can have no more than 255 unique element list definitions among its ENUM and SET columns considered as a group
     *
     * Protocol: FIELD_TYPE_ENUM = 247
     */
    ENUM("ENUM", Types.CHAR, String.class, 0, MysqlType.IS_NOT_DECIMAL, 65535L,
            "('value1','value2',...) [CHARACTER SET charset_name] [COLLATE collation_name]"),
    /**
     * SET('value1','value2',...) [CHARACTER SET charset_name] [COLLATE collation_name]
     * A set. A string object that can have zero or more values, each of which must be chosen from the list
     * of values 'value1', 'value2', ... SET values are represented internally as integers.
     * A SET column can have a maximum of 64 distinct members. A table can have no more than 255 unique
     * element list definitions among its ENUM and SET columns considered as a group
     *
     * Protocol: FIELD_TYPE_SET = 248
     */
    SET("SET", Types.CHAR, String.class, 0, MysqlType.IS_NOT_DECIMAL, 64L, "('value1','value2',...) [CHARACTER SET charset_name] [COLLATE collation_name]"),
    /**
     * TINYBLOB
     * A BLOB column with a maximum length of 255 (28 - 1) bytes. Each TINYBLOB value is stored using a
     * 1-byte length prefix that indicates the number of bytes in the value.
     *
     * Protocol:FIELD_TYPE_TINY_BLOB = 249
     */
    TINYBLOB("TINYBLOB", Types.VARBINARY, null, 0, MysqlType.IS_NOT_DECIMAL, 255L, ""),
    /**
     * TINYTEXT [CHARACTER SET charset_name] [COLLATE collation_name]
     * A TEXT column with a maximum length of 255 (28 - 1) characters. The effective maximum length
     * is less if the value contains multibyte characters. Each TINYTEXT value is stored using
     * a 1-byte length prefix that indicates the number of bytes in the value.
     *
     * Protocol:FIELD_TYPE_TINY_BLOB = 249
     */
    TINYTEXT("TINYTEXT", Types.VARCHAR, String.class, 0, MysqlType.IS_NOT_DECIMAL, 255L, " [CHARACTER SET charset_name] [COLLATE collation_name]"),
    /**
     * MEDIUMBLOB
     * A BLOB column with a maximum length of 16,777,215 (224 - 1) bytes. Each MEDIUMBLOB value is stored
     * using a 3-byte length prefix that indicates the number of bytes in the value.
     *
     * Protocol: FIELD_TYPE_MEDIUM_BLOB = 250
     */
    MEDIUMBLOB("MEDIUMBLOB", Types.LONGVARBINARY, null, 0, MysqlType.IS_NOT_DECIMAL, 16777215L, ""),
    /**
     * MEDIUMTEXT [CHARACTER SET charset_name] [COLLATE collation_name]
     * A TEXT column with a maximum length of 16,777,215 (224 - 1) characters. The effective maximum length
     * is less if the value contains multibyte characters. Each MEDIUMTEXT value is stored using a 3-byte
     * length prefix that indicates the number of bytes in the value.
     *
     * Protocol: FIELD_TYPE_MEDIUM_BLOB = 250
     */
    MEDIUMTEXT("MEDIUMTEXT", Types.LONGVARCHAR, String.class, 0, MysqlType.IS_NOT_DECIMAL, 16777215L, " [CHARACTER SET charset_name] [COLLATE collation_name]"),
    /**
     * LONGBLOB
     * A BLOB column with a maximum length of 4,294,967,295 or 4GB (232 - 1) bytes. The effective maximum length
     * of LONGBLOB columns depends on the configured maximum packet size in the client/server protocol and available
     * memory. Each LONGBLOB value is stored using a 4-byte length prefix that indicates the number of bytes in the value.
     *
     * Protocol: FIELD_TYPE_LONG_BLOB = 251
     */
    LONGBLOB("LONGBLOB", Types.LONGVARBINARY, null, 0, MysqlType.IS_NOT_DECIMAL, 4294967295L, ""),
    /**
     * LONGTEXT [CHARACTER SET charset_name] [COLLATE collation_name]
     * A TEXT column with a maximum length of 4,294,967,295 or 4GB (232 - 1) characters. The effective
     * maximum length is less if the value contains multibyte characters. The effective maximum length
     * of LONGTEXT columns also depends on the configured maximum packet size in the client/server protocol
     * and available memory. Each LONGTEXT value is stored using a 4-byte length prefix that indicates
     * the number of bytes in the value.
     *
     * Protocol: FIELD_TYPE_LONG_BLOB = 251
     */
    LONGTEXT("LONGTEXT", Types.LONGVARCHAR, String.class, 0, MysqlType.IS_NOT_DECIMAL, 4294967295L, " [CHARACTER SET charset_name] [COLLATE collation_name]"),
    /**
     * BLOB[(M)]
     * A BLOB column with a maximum length of 65,535 (216 - 1) bytes. Each BLOB value is stored using
     * a 2-byte length prefix that indicates the number of bytes in the value.
     * An optional length M can be given for this type. If this is done, MySQL creates the column as
     * the smallest BLOB type large enough to hold values M bytes long.
     *
     * Protocol: FIELD_TYPE_BLOB = 252
     */
    BLOB("BLOB", Types.LONGVARBINARY, null, 0, MysqlType.IS_NOT_DECIMAL, 65535L, "[(M)]"),
    /**
     * TEXT[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]
     * A TEXT column with a maximum length of 65,535 (216 - 1) characters. The effective maximum length
     * is less if the value contains multibyte characters. Each TEXT value is stored using a 2-byte length
     * prefix that indicates the number of bytes in the value.
     * An optional length M can be given for this type. If this is done, MySQL creates the column as
     * the smallest TEXT type large enough to hold values M characters long.
     *
     * Protocol: FIELD_TYPE_BLOB = 252
     */
    TEXT("TEXT", Types.LONGVARCHAR, String.class, 0, MysqlType.IS_NOT_DECIMAL, 65535L, "[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]"),
    /**
     * [NATIONAL] CHAR[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]
     * A fixed-length string that is always right-padded with spaces to the specified length when stored.
     * M represents the column length in characters. The range of M is 0 to 255. If M is omitted, the length is 1.
     * Note
     * Trailing spaces are removed when CHAR values are retrieved unless the PAD_CHAR_TO_FULL_LENGTH SQL mode is enabled.
     * CHAR is shorthand for CHARACTER. NATIONAL CHAR (or its equivalent short form, NCHAR) is the standard SQL way
     * to define that a CHAR column should use some predefined character set. MySQL 4.1 and up uses utf8
     * as this predefined character set.
     *
     * MySQL permits you to create a column of type CHAR(0). This is useful primarily when you have to be compliant
     * with old applications that depend on the existence of a column but that do not actually use its value.
     * CHAR(0) is also quite nice when you need a column that can take only two values: A column that is defined
     * as CHAR(0) NULL occupies only one bit and can take only the values NULL and '' (the empty string).
     *
     * Protocol: FIELD_TYPE_STRING = 254
     */
    CHAR("CHAR", Types.CHAR, String.class, 0, MysqlType.IS_NOT_DECIMAL, 255L, "[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]"),
    /**
     * BINARY(M)
     * The BINARY type is similar to the CHAR type, but stores binary byte strings rather than nonbinary character strings.
     * M represents the column length in bytes.
     *
     * The CHAR BYTE data type is an alias for the BINARY data type.
     *
     * Protocol: no concrete type on the wire TODO: really?
     */
    BINARY("BINARY", Types.BINARY, null, 0, MysqlType.IS_NOT_DECIMAL, 255L, "(M)"),
    /**
     * Top class for Spatial Data Types
     *
     * Protocol: FIELD_TYPE_GEOMETRY = 255
     */
    GEOMETRY("GEOMETRY", Types.BINARY, null, 0, MysqlType.IS_NOT_DECIMAL, 65535L, ""), // TODO check precision, it isn't well documented, only mentioned that WKB format is represented by BLOB
    /**
     * Fall-back type for those MySQL data types which c/J can't recognize.
     * Handled the same as BLOB.
     *
     * Has no protocol ID.
     */
    UNKNOWN("UNKNOWN", Types.OTHER, null, 0, MysqlType.IS_NOT_DECIMAL, 65535L, "");

    

    public static MysqlType getByJdbcType(int jdbcType) {
        switch (jdbcType) {
            case Types.BIGINT:
                return BIGINT;
            case Types.BINARY:
                return BINARY;
            case Types.BIT:
                return BIT;
            case Types.BOOLEAN:
                return BOOLEAN;
            case Types.CHAR:
            case Types.NCHAR: // TODO check that it's correct
                return CHAR;
            case Types.DATE:
                return DATE;
            case Types.DECIMAL:
            case Types.NUMERIC:
                return DECIMAL;
            case Types.DOUBLE:
            case Types.FLOAT:
                return DOUBLE;
            case Types.INTEGER:
                return INT;
            case Types.LONGVARBINARY:
            case Types.BLOB: // TODO check that it's correct
            case Types.JAVA_OBJECT: // TODO check that it's correct
                return BLOB;
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR: // TODO check that it's correct
            case Types.CLOB: // TODO check that it's correct
            case Types.NCLOB: // TODO check that it's correct
                return TEXT;
            case Types.NULL:
                return NULL;
            case Types.REAL:
                return FLOAT;
            case Types.SMALLINT:
                return SMALLINT;
            case Types.TIME:
                return TIME;
            case Types.TIMESTAMP:
                return TIMESTAMP;
            case Types.TINYINT:
                return TINYINT;
            case Types.VARBINARY:
                return VARBINARY;
            case Types.VARCHAR:
            case Types.NVARCHAR: // TODO check that it's correct
            case Types.DATALINK: // TODO check that it's correct
            case Types.SQLXML: // TODO check that it's correct
                return VARCHAR;

            case Types.REF_CURSOR:
                throw new RuntimeException("REF_CURSOR type is not supported");
            case Types.TIME_WITH_TIMEZONE:
                throw new RuntimeException("TIME_WITH_TIMEZONE type is not supported");
            case Types.TIMESTAMP_WITH_TIMEZONE:
                throw new RuntimeException("TIMESTAMP_WITH_TIMEZONE type is not supported");

                // TODO check next types
            case Types.ARRAY:
            case Types.DISTINCT:
            case Types.OTHER:
            case Types.REF:
            case Types.ROWID:
            case Types.STRUCT:

            default:
                return UNKNOWN;
        }
    }

    /**
     * Is CONVERT between the given SQL types supported?
     *
     * @param fromType
     *            the type to convert from
     * @param toType
     *            the type to convert to
     * @return true if so
     * @see Types
     */
    public static boolean supportsConvert(int fromType, int toType) {

        // TODO use MysqlTypes here ?

        switch (fromType) {
            /*
             * The char/binary types can be converted to pretty much anything.
             */
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:

                switch (toType) {
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                    case Types.REAL:
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.FLOAT:
                    case Types.DOUBLE:
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.OTHER:
                    case Types.DATE:
                    case Types.TIME:
                    case Types.TIMESTAMP:
                        return true;

                    default:
                        return false;
                }

                /*
                 * We don't handle the BIT type yet.
                 */
            case Types.BIT:
                return false;

            /*
             * The numeric types. Basically they can convert among themselves, and with char/binary types.
             */
            case Types.DECIMAL:
            case Types.NUMERIC:
            case Types.REAL:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.DOUBLE:

                switch (toType) {
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                    case Types.REAL:
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.FLOAT:
                    case Types.DOUBLE:
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        return true;

                    default:
                        return false;
                }

                /* MySQL doesn't support a NULL type. */
            case Types.NULL:
                return false;

            /*
             * With this driver, this will always be a serialized object, so the char/binary types will work.
             */
            case Types.OTHER:

                switch (toType) {
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        return true;

                    default:
                        return false;
                }

                /* Dates can be converted to char/binary types. */
            case Types.DATE:

                switch (toType) {
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        return true;

                    default:
                        return false;
                }

                /* Time can be converted to char/binary types */
            case Types.TIME:

                switch (toType) {
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        return true;

                    default:
                        return false;
                }

                /*
                 * Timestamp can be converted to char/binary types and date/time types (with loss of precision).
                 */
            case Types.TIMESTAMP:

                switch (toType) {
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.TIME:
                    case Types.DATE:
                        return true;

                    default:
                        return false;
                }

                /* We shouldn't get here! */
            default:
                return false; // not sure
        }
    }

    public static boolean isSigned(MysqlType type) {
        switch (type) {
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case MEDIUMINT:
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    private final String name;
    protected int jdbcType;
    protected final Class<?> javaClass;
    private final int flagsMask;
    private final boolean isDecimal;
    private final Long precision;
    private final String createParams;

    /**
     *
     * @param mysqlTypeName
     *            mysqlTypeName
     * @param jdbcType
     *            jdbcType
     * @param javaClass
     *            javaClass
     * @param allowedFlags
     *            allowedFlags
     * @param isDec
     *            isDec
     * @param precision
     *            represents the maximum column size that the server supports for the given datatype.
     *            <ul>
     *            <li>For numeric data, this is the maximum precision.
     *            <li>
     *            For character data, this is the length in characters.
     *            <li>For datetime datatypes, this is the length in characters of the String
     *            representation (assuming the maximum allowed precision of the fractional seconds component).
     *            <li>For binary data, this is the length in bytes.
     *            <li>For the ROWID datatype, this is the length in bytes.
     *            <li>Null is returned for data types where the column size is not applicable.
     *            </ul>
     * @param createParams
     *            params
     */
    private MysqlType(String mysqlTypeName, int jdbcType, Class<?> javaClass, int allowedFlags, boolean isDec, Long precision, String createParams) {
        this.name = mysqlTypeName;
        this.jdbcType = jdbcType;
        this.javaClass = javaClass;
        this.flagsMask = allowedFlags;
        this.isDecimal = isDec;
        this.precision = precision;
        this.createParams = createParams;
    }

    public String getName() {
        return this.name;
    }

    public int getJdbcType() {
        return this.jdbcType;
    }

    public boolean isAllowed(int flag) {
        return ((this.flagsMask & flag) > 0);
    }

    public String getClassName() {
        if (this.javaClass == null) {
            return "[B";
        }
        return this.javaClass.getName();
    }

    /**
     * Checks if the MySQL Type is a Decimal/Number Type
     *
     * @return true if the MySQL Type is a Decimal/Number Type
     */
    public boolean isDecimal() {
        return this.isDecimal;
    }

    /**
     * The PRECISION column represents the maximum column size that the server supports for the given datatype.
     * <ul>
     * <li>For numeric data, this is the maximum
     * precision.
     * <li>For character data, this is the length in characters.
     * <li>For datetime datatypes, this is the length in characters of the String
     * representation (assuming the maximum allowed precision of the fractional seconds component).
     * <li>For binary data, this is the length in bytes.
     * <li>For
     * the ROWID datatype, this is the length in bytes.
     * <li>Null is returned for data types where the column size is not applicable.
     * </ul>
     *
     * @return precision
     */
    public Long getPrecision() {
        return this.precision;
    }

    public String getCreateParams() {
        return this.createParams;
    }

    public static final int FIELD_FLAG_NOT_NULL = 1;
    public static final int FIELD_FLAG_PRIMARY_KEY = 2;
    public static final int FIELD_FLAG_UNIQUE_KEY = 4;
    public static final int FIELD_FLAG_MULTIPLE_KEY = 8;
    public static final int FIELD_FLAG_BLOB = 16;
    public static final int FIELD_FLAG_UNSIGNED = 32;
    public static final int FIELD_FLAG_ZEROFILL = 64;
    public static final int FIELD_FLAG_BINARY = 128;
    public static final int FIELD_FLAG_AUTO_INCREMENT = 512;

    private static final boolean IS_DECIMAL = true;
    private static final boolean IS_NOT_DECIMAL = false;

    @Override
    public String getVendor() {
        return "com.mysql.cj";
    }

    @Override
    public Integer getVendorTypeNumber() {
        return this.jdbcType;
    }

    // Protocol field type numbers
    public static final int FIELD_TYPE_DECIMAL = 0;
    public static final int FIELD_TYPE_TINY = 1;
    public static final int FIELD_TYPE_SHORT = 2;
    public static final int FIELD_TYPE_LONG = 3;
    public static final int FIELD_TYPE_FLOAT = 4;
    public static final int FIELD_TYPE_DOUBLE = 5;
    public static final int FIELD_TYPE_NULL = 6;
    public static final int FIELD_TYPE_TIMESTAMP = 7;
    public static final int FIELD_TYPE_LONGLONG = 8;
    public static final int FIELD_TYPE_INT24 = 9;
    public static final int FIELD_TYPE_DATE = 10;
    public static final int FIELD_TYPE_TIME = 11;
    public static final int FIELD_TYPE_DATETIME = 12;
    public static final int FIELD_TYPE_YEAR = 13;
    public static final int FIELD_TYPE_VARCHAR = 15;
    public static final int FIELD_TYPE_BIT = 16;
    public static final int FIELD_TYPE_JSON = 245;
    public static final int FIELD_TYPE_NEWDECIMAL = 246;
    public static final int FIELD_TYPE_ENUM = 247;
    public static final int FIELD_TYPE_SET = 248;
    public static final int FIELD_TYPE_TINY_BLOB = 249;
    public static final int FIELD_TYPE_MEDIUM_BLOB = 250;
    public static final int FIELD_TYPE_LONG_BLOB = 251;
    public static final int FIELD_TYPE_BLOB = 252;
    public static final int FIELD_TYPE_VAR_STRING = 253;
    public static final int FIELD_TYPE_STRING = 254;
    public static final int FIELD_TYPE_GEOMETRY = 255;

}
