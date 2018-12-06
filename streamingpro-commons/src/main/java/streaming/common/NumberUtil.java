package streaming.common;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Random;

/**
 * 4/28/16 WilliamZhu(allwefantasy@gmail.com)
 */
public class NumberUtil {
    private static final String NUM_REGEX = "^(\\+|\\-)?(0+\\.[\\d]+|[1-9]+[\\d]*\\.[\\d]+|[\\d]+)$";

    /**
     * 判断是否是数字
     * @param str
     * @return
     */
    public  static boolean isNum(String str){

        if(str == null || str.isEmpty()){
            return false;
        }else{

            return str.matches(NUM_REGEX);
        }
    }

    private  static void testIsNum(){

        System.out.println(isNum(null) == false);
        System.out.println(isNum("") == false);
        System.out.println(isNum("NaN") == false);
        System.out.println(isNum(" ") == false);
        System.out.println(isNum("-") == false);
        System.out.println(isNum("-adf") == false);
        System.out.println(isNum("-123") == true);
        System.out.println(isNum("-0.123") == true);
        System.out.println(isNum("+0.123") == true);
        System.out.println(isNum("0.123") == true);
        System.out.println(isNum("234234") == true);
        System.out.println(isNum("234234.343") == true);
        System.out.println(isNum("234234") == true);


        System.out.println(isNum("1324791546078"));
    }

    /*
     * @param obj
     * @return
     */
    public static int parseNumber(Object obj){
        int result = 0;
        try {
            result = Integer.valueOf(obj.toString());
        } catch (Exception e) {
            result = 0;
        }
        return result;
    }

    /**
     *
     * @param obj
     * @return
     */
    public static long parseLong(Object obj){
        long result = 0;
        try {
            result = Long.valueOf(obj.toString());
        } catch (Exception e) {
            result = 0;
        }
        return result;
    }

    /**
     *
     * @param obj
     * @return
     */
    public static double parseDouble(Object obj){
        double result = 0;
        try {
            result = Double.valueOf(obj.toString());
        } catch (Exception e) {
            result = 0;
        }
        return result;
    }

    /**
     *
     * @param obj
     * @return
     */
    public static float parseFloat(Object obj){
        float result = 0;
        try {
            result = Float.valueOf(obj.toString());
        } catch (Exception e) {
            result = 0;
        }
        return result;
    }

    /**
     *
     * @param obj
     * @return
     */
    public static String toNumStr(Object obj){
        int result = 0;
        try {
            result = Integer.valueOf(obj.toString());
        } catch (Exception e) {
            result = 0;
        }
        return String.valueOf(result);
    }

    /**
     * deal the decimal by the decimal format
     * @param d
     * @param df
     * @return
     */
    public static double dealDecimal(double d, DecimalFormat df){
        double pd = 0;
        try {
            pd = parseDouble(df.format(d));
        } catch (Exception e) {
        }
        return pd;
    }

    /**
     * deal the decimal to define number decimal places
     * @param d
     * @param num decimal places
     * @return
     */
    public static double dealDecimal(double d, int num){
        double result = 0;
        try {
            StringBuffer pattern = new StringBuffer("#");
            if(num>0) pattern.append(".");
            for(int i=1; i<=num; i++){
                pattern.append("0");
            }
            DecimalFormat df = new DecimalFormat(pattern.toString());
            result = NumberUtil.dealDecimal(d, df);
        } catch (Exception e) {
        }
        return result;

    }

    /**
     * 返回指定范围的随机数
     * @param start
     * @param end
     * @return
     */
    public static int getRandNum(int start, int end){
        int result = start + new Random().nextInt(5);
        if(result>end) result=end;
        return result;
    }
    /**
     * 格式化百分比
     * @param obj
     * @return
     */
    public static String getPercent(Object obj){
        NumberFormat format= NumberFormat.getPercentInstance();
        format.setMaximumFractionDigits(2);
        format.format(obj);
        return format.format(obj);
    }

    public static String getNumTInt(Object obj){
        NumberFormat formatter = new DecimalFormat("###,###");
        return formatter.format(obj)+"";
    }

    public static String getNumInt(Object obj){
        NumberFormat formatter = new DecimalFormat("######");
        return formatter.format(obj)+"";
    }

    public static String getNumTPoint(Object obj) {
        NumberFormat formatter = new DecimalFormat("###,###.###");
        return formatter.format(obj)+"";
    }

    public static double formatDaouble(double number, int digits) {
        DecimalFormat df=(DecimalFormat)NumberFormat.getInstance();
        df.setMaximumFractionDigits(digits);

        return Double.parseDouble(df.format(number).replaceAll(",", ""));
    }

    /**
     * 将对象转出成int
     * @param object
     * @return
     */
    public static int getIntValue(Object object){

        if(object == null){
            return 0;
        }
        else {
            try {
                return Integer.parseInt(object.toString());
            } catch (Exception e) {
                return 0;
            }
        }
    }

    /**
     * 将对象转出成int
     * @param object
     * @return
     */
    public static int getIntValue(Object object, int defaultValue){

        if(object == null){
            return defaultValue;
        }
        else {
            try {
                return Integer.parseInt(object.toString());
            } catch (Exception e) {
                return defaultValue;
            }
        }
    }

    /**
     * 将对象转出成int
     * @param object
     * @return
     */
    public static long getLongValue(Object object){

        if(object == null){
            return 0;
        }
        else {
            try {
                return Long.parseLong(object.toString());
            } catch (Exception e) {
                return 0;
            }
        }
    }


    public static long getLongValue(Object object, long defaultValue){

        if(object == null){
            return defaultValue;
        }
        else {
            try {
                return Long.parseLong(object.toString());
            } catch (Exception e) {
                return defaultValue;
            }
        }
    }

    public static int getTimeValue(Object object){

        if(object == null){
            return 0;
        }
        else {
            try {
                String arr[] = object.toString().split(":");
                if(arr.length == 2){
                    return NumberUtil.getIntValue(arr[0]) * 60 + NumberUtil.getIntValue(arr[1]);
                }else{
                    return 0;
                }

            } catch (Exception e) {
                return 0;
            }
        }
    }

    /**
     * 将对象转出成double
     * @param object
     * @return
     */
    public static double getDoubleValue(Object object){

        if(object == null){
            return 0;
        }
        else {
            try {
                return Double.parseDouble(object.toString());
            } catch (Exception e) {
                return 0;
            }
        }
    }


    public static double getDoubleValue(Object object, double defaultValue){

        if(object == null){
            return defaultValue;
        }
        else {
            try {
                return Double.parseDouble(object.toString());
            } catch (Exception e) {
                return defaultValue;
            }
        }
    }


    public static String getPercent(String str1, String str2){

        long long1 = getLongValue(str1);
        long long2 = getLongValue(str2);
        double r = 0.00f;
        if(long2 > 0 &&  long1 > 0){

            r = long1 * 1.0 / long2;
        }
        return String.format("%.2f", r * 100);
    }

    public static String getPercent(String str1, String str2,int decimalNum){

        long long1 = getLongValue(str1);
        long long2 = getLongValue(str2);
        double r = 0.00f;
        if(long2 > 0 &&  long1 > 0){

            r = long1 * 1.0 / long2;
        }
        return String.format("%."+decimalNum+"f", r * 100);
    }



    public static void main(String[] args) {
        testIsNum();
    }
}
