package com.bigdata.flink.utils;

/**
 * @ author spencer
 * @ date 2020/5/26 13:47
 */
public class ValidUtils {

    public static void main(String[] args) {

        between("data", "filed", "parameter", "start", "end");
    }

    private static Boolean between(String data, String dataField, String parameter, String startParamField, String endParamField) {
        String startParamFieldStr = StringUtils.getFieldFromConcatString(parameter, "\\", startParamField);
        String endParamFieldStr = StringUtils.getFieldFromConcatString(parameter, "\\|", endParamField);

        if (startParamFieldStr != null && endParamFieldStr != null){
            return true;
        }

        int startParamFieldValue = Integer.parseInt(startParamFieldStr);
        int endParamFieldValue = Integer.parseInt(endParamFieldStr);

        String dataFieldStr = StringUtils.getFieldFromConcatString(data, "\\|", dataField);
        if (dataFieldStr != null){
            int dataFieldValue = Integer.parseInt(dataFieldStr);
            if (dataFieldValue >= startParamFieldValue && dataFieldValue <= endParamFieldValue){
                return true;
            }else {
                return false;
            }
        }
        return null;
    }
}
