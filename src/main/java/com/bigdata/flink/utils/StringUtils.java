package com.bigdata.flink.utils;

/**
 * @ author spencer
 * @ date 2020/5/26 13:17
 */
public class StringUtils {

    public static void main(String[] args) {

        String str = "sessionid=7291cc307f96432f8da9d926fd7d88e5|searchKeywords=洗面奶,小龙虾,机器学习,苹果,华为手机|clickCategoryIds=11,93,36,66,60|visitLength=3461|stepLength=43|startTime=2019-05-30 14:01:01";

        String searchKeywords = getFieldFromConcatString(str, "\\|", "searchKeywords");

        System.out.println(searchKeywords);
    }

    public static String getFieldFromConcatString(String str, String delimiter, String field) {
        String[] splits = str.split(delimiter);
        for (String split : splits) {
            String[] keyAndValue = split.split("=");
            if (keyAndValue.length == 2){
                String key = keyAndValue[0];
                String value = keyAndValue[1];
                if (key.equals(field)){
                    return value;
                }
            }
        }

        return null;
    }
}
