package streaming.common;


import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 4/28/16 WilliamZhu(allwefantasy@gmail.com)
 */
public class ParamsUtil {

    private static final char PARAM_START_FLAG = '-'; //参数开始标记
    private static  char PARAM_SPLIT_FLAG [] = new char[]{'\t' ,' '};//参数间隔标记
    private static  char PARAM_QUOTE_FLAG  = '"';//引号


    private char paramsCharArray [];
    private String mainParam = null;
    private String startMainParam = null;
    private String endMainParam = null;

    private ParamPairParseIm paramPairParse;
    private ParamOnlyParseIm paramOnlyParse;


    private Map<String, String> paramsMap = new HashMap<String, String>();
    private String info;
    private Logger logger = Logger.getLogger(ParamsUtil.class);

    public ParamsUtil(String s, String info){

        this.info = info;
        if(s != null && s.trim().length() > 0){
            this.paramsCharArray = s.toCharArray();
            this.init();
            this.parse();

            if(this.hasParam("h") && info != null){

                showHelp();
                System.exit(0);
                //throw new Exception();
            }
        }

    }

    public void showHelp(){

        if(info != null){

            System.out.println("[使用帮助]");
            System.out.println(info);
        }
    }

    public ParamsUtil(String s){
        this(s, null);
    }
    public ParamsUtil(String s[]){

        this(s, null);

    }
    public ParamsUtil(String s[], String info){
        this(arrayToString(s, " "), info);
    }


    private void init(){

        this.paramPairParse = new ParamPairParse();
        this.paramOnlyParse = new ParamOnlyParse();
        Arrays.sort(PARAM_SPLIT_FLAG);

    }
    /**
     * 字符是参数间字符吗
     * @return
     */
    public static boolean isParamSplit(char c){

        return Arrays.binarySearch(PARAM_SPLIT_FLAG, c) >= 0;


    }


    /**
     * 字符是参数的开始吗
     * @return
     */
    public static boolean isParamStart(char c){

        return c == PARAM_START_FLAG;


    }

    /**
     * 字符是引号吗
     * @return
     */
    public static boolean isQuote(char c){

        return PARAM_QUOTE_FLAG == c;


    }

    private void parse(){

        if(paramsCharArray != null){


            int len = paramsCharArray.length - 1;
            int mark = 0;
            char c;
            while(true){


                c = paramsCharArray[mark];
                //System.out.println(mark);
                if(isParamStart(c)){
                    //System.out.println("pair");
                    mark = this.paramPairParse.parse(paramsCharArray, mark + 1, paramsMap);
                }else if (isParamSplit(c)) {

                    //System.out.println("normal");
                }else {
                    //System.out.println("param");
                    //是单个参数
                    StringBuilder param = new StringBuilder();

                    mark = this.paramOnlyParse.parse(paramsCharArray, mark, param);

                    if(this.startMainParam == null && paramsMap.size() == 0){
                        this.startMainParam = param.toString().trim();
                    }else {
                        this.endMainParam = param.toString().trim();
                    }
                }

                mark++;

                if(mark > len){

                    break;
                }
            }

//			System.out.println(paramsCharArray);
//			System.out.println(paramsMap);
//			System.out.println("["+startMainParam + "]");
//			System.out.println("["+endMainParam + "]");
//
//
//


        }


    }

    public String getParam(String key ){

        return paramsMap.get(key);
    }

    public int getIntParam(String key ){

        return NumberUtil.getIntValue(getParam(key));
    }
    public int getIntParamAndCheck(String key ) throws Exception {

        if(hasParam(key))
            return NumberUtil.getIntValue(getParam(key));
        else{
            this.showHelp();
            throw new Exception("参数中没有包含" + key + "参数");
        }
    }

    public void err(String key) throws Exception {

        this.showHelp();
        throw new Exception("参数中没有包含" + key + "参数");
    }



    public int getIntParam(String key , int defaultValue){

        return NumberUtil.getIntValue(getParam(key), defaultValue);
    }

    public long getLongParam(String key ){

        return NumberUtil.getLongValue(getParam(key));
    }

    public long getLongParamAndCheck(String key ) throws Exception {

        if(hasParam(key))
            return NumberUtil.getLongValue(getParam(key));
        else{
            this.showHelp();
            throw new Exception("参数中没有包含" + key + "参数");
        }
    }

    public long getLongParam(String key , long defaultValue){

        return NumberUtil.getLongValue(getParam(key), defaultValue);
    }

    public double getDoubleParam(String key ){

        return NumberUtil.getDoubleValue(getParam(key));
    }
    public double getDoubleParamAndCheck(String key ) throws Exception {

        if(hasParam(key))
            return NumberUtil.getDoubleValue(getParam(key));
        else{
            this.showHelp();
            throw new Exception("参数中没有包含" + key + "参数");
        }
    }

    public double getDoubleParam(String key , double defaultValue){

        return NumberUtil.getDoubleValue(getParam(key), defaultValue);
    }

    public boolean getBooleanParam(String key ){


        return "true".equals(getParam(key, "").toLowerCase());
    }

    public boolean getBooleanParamAndCheck(String key ) throws Exception {


        if(hasParam(key))
            return "true".equals(getParam(key, "").toLowerCase());
        else{
            this.showHelp();
            throw new Exception("参数中没有包含" + key + "参数");
        }
    }

    public boolean getBooleanParam(String key , boolean defaultValue){



        if(hasParam(key)){
            return getBooleanParam(key);
        }else{
            return defaultValue;
        }
    }


    public Map<String, String> getParamsMap(){
        return paramsMap;
    }
    public String getMainParam(){

        if(endMainParam == null || endMainParam.isEmpty() ){
            return this.startMainParam;
        }else {

            return this.endMainParam;
        }
    }

    public String getStartMainParam(){

        return this.startMainParam;
    }

    public String getEndMainParam(){

        return this.endMainParam;
    }

    /**
     * 获取参数，没有时返回默认值
     * @param key
     * @param defaultValue
     * @return
     */
    public String getParam(String key , String defaultValue){

        String value =  paramsMap.get(key);
        return (value == null || "".equals(value) ) ? defaultValue : value;
    }

    /**
     * 是否有指定参数
     * @param key
     * @return
     */
    public boolean hasParam(String key){


        return paramsMap.containsKey(key);
    }

    /**
     * 是否有指定参数,没有时直接抛出异常
     * @param key
     * @return
     * @throws Exception
     */
    public boolean hasParamAndCheck(String key) throws Exception{


        if(paramsMap.containsKey(key)){
            return true;
        }else {
            this.showHelp();
            throw new Exception("参数中没有包含" + key + "参数");
        }
    }

    /**
     * 为空时直接抛出异常
     * @return
     * @throws Exception
     */
    public String getMainParamAndCheck() throws Exception{


        if(this.mainParam == null){
            this.showHelp();
            throw new Exception("mainParam没有设定值");
        }
        return this.mainParam;

    }


    public String getParamAndCheck(String key ) throws Exception{

        String value =  paramsMap.get(key);
        if(value == null){

            this.showHelp();
            throw new Exception("参数" + key + "没有设定值");
        }
        return value;

    }
    public  static  String arrayToString(String []s , String split){

        if(s != null && split != null){

            StringBuilder result = new StringBuilder();

            for(String c : s){
                result.append(c).append(split);
            }

            if(result.length() > split.length()){
                return result.substring(0, result.length() - split.length());
            }
        }

        return null;

    }

    public static void main(String[] args) throws Exception {

        ParamsUtil paramsUtil = new ParamsUtil("sql " , "test asdfhasdf ");

        System.out.println(paramsUtil.getParamAndCheck("sql"));
//		System.out.println("get:" + "post gres测试".equals(paramsUtil.getParam("U")));
//		System.out.println("get:" + "6543".equals(paramsUtil.getParam("P")));
//		System.out.println("get:" + "123".equals(paramsUtil.getParam("h")));
//		System.out.println("main:"+"pv_proxy           sdf".equals(paramsUtil.getMainParam()));
//		System.out.println("startMain:"+"psql".equals(paramsUtil.getStartMainParam()));
//		System.out.println("endMain:"+"pv_proxy           sdf".equals(paramsUtil.getEndMainParam()));
//		System.out.println("has:"+paramsUtil.hasParam("r"));




    }

}

interface ParamPairParseIm{

    public int parse(char[] paramsCharArray, int start, Map<String, String> collector);
}

interface ParamOnlyParseIm{

    public int parse(char[] paramsCharArray, int start, StringBuilder collector);
}


/**
 * 解析键值对的参数
 * @author fengbingjian
 *
 */
class ParamPairParse implements ParamPairParseIm{


    public int parse(char[] paramsCharArray, int start,
                     Map<String, String> collector) {

        StringBuilder key = new StringBuilder();
        StringBuilder value = new StringBuilder();
        char last = '"';
        boolean keyFlag = true;

        int mark = start;


        char cur;
        boolean breakFlag = false;
        boolean quoteValue = false;
        boolean quoteEnd = true;

        int len = paramsCharArray.length - 1;
        while (true) {


            cur = paramsCharArray[mark];

            if(!quoteValue && ParamsUtil.isParamStart(cur) &&ParamsUtil.isParamSplit(last)){

                //回退一个位置，并退出
                mark--;
                breakFlag = true;

            }else if(ParamsUtil.isParamSplit(cur)){
                //碰到了空格或\t,如果在""间，需要特殊处理


                if(quoteValue){
                    //是在""中
                    if(keyFlag){
                        key.append(cur);

                    }else {
                        value.append(cur);
                    }
                }else {
                    //不是""中时，如果正在读取value部分，说明碰到了结尾

                    if(keyFlag || value.length() == 0){
                        keyFlag = false;
                    }else {
                        breakFlag = true;

                    }

                }



            }else {

                if(ParamsUtil.isQuote(cur)){

                    if(!keyFlag){
                        quoteValue = true;
                        if(!quoteEnd){
                            quoteEnd = true;
                            breakFlag = true;
                        }else {
                            quoteEnd = false;
                        }

                    }

                }else {
                    //普通字符
                    if(keyFlag){
                        //在算key
                        key.append(cur);
                    }else {
                        value.append(cur);
                    }
                }

            }



            if(breakFlag || mark >= len ){
                if(key.length() > 0){

                    if(!quoteEnd){
                        System.out.println("警告:" + key.toString() + "\"配对不正确");
                    }
                    collector.put(key.toString().trim(), value.toString().trim());
                }

                //System.out.println(key + "," + value);
                return mark;
            }else {
                mark++;
                last = cur;
            }

        }

    }

}


/**
 * 解析单个参数
 * @author fengbingjian
 *
 */
class ParamOnlyParse implements ParamOnlyParseIm{

    public int parse(char[] paramsCharArray, int start,
                     StringBuilder collector) {



        char last = '"';
        boolean keyFlag = false;

        int mark = start;


        char cur;
        boolean breakFlag = false;
        boolean quoteValue = false;
        boolean quoteEnd = true;

        int len = paramsCharArray.length - 1;
        while (true) {


            cur = paramsCharArray[mark];

            if(!quoteValue && ParamsUtil.isParamStart(cur) &&ParamsUtil.isParamSplit(last)){

                //回退一个位置，并退出
                mark--;
                breakFlag = true;

            }else if(ParamsUtil.isParamSplit(cur)){
                //碰到了空格或\t,如果在""间，需要特殊处理


                if(quoteValue){
                    //是在""中
                    if(keyFlag){
                        collector.append(cur);

                    }else {
                        collector.append(cur);
                    }
                }else {
                    //不是""中时，如果正在读取value部分，说明碰到了结尾

                    if(keyFlag || collector.length() == 0){
                        keyFlag = false;
                    }else {
                        breakFlag = true;

                    }

                }



            }else {

                if(ParamsUtil.isQuote(cur)){

                    if(!keyFlag){
                        quoteValue = true;
                        if(!quoteEnd){
                            quoteEnd = true;
                            breakFlag = true;
                        }else {
                            quoteEnd = false;
                        }

                    }

                }else {
                    //普通字符
                    if(keyFlag){
                        //在算key
                        collector.append(cur);
                    }else {
                        collector.append(cur);
                    }
                }

            }



            if(breakFlag || mark >= len ){

                //System.out.println(key + "," + value);
                return mark;
            }else {
                mark++;
                last = cur;
            }

        }


    }


}
