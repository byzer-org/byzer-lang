connect jdbc where driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/wow?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
and driver="com.mysql.jdbc.Driver"
and user="root"
and password="csdn.net"
as tableau;

select "a" as a,"b" as b
as tod_boss_dashboard_sheet_1;

save append tod_boss_dashboard_sheet_1
as jdbc.`tableau.tod_boss_dashboard_sheet_1`
options truncate="true";