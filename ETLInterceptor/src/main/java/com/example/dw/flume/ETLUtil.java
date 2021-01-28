package com.example.dw.flume;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

import java.util.Arrays;
import java.util.List;

/**
 * @Description:
 * @Auther: HuaZhiLi
 * @Date: 2021/1/28 22:43
 */
public class ETLUtil {

    //判断启动日志是否复合格式要求
    //验证JSON字符串的完整性，是否以{}开头结尾
    public static boolean validStartLog(String source) {

        //判断body部分是否有数据
        if (StringUtils.isBlank(source)) {
            return false;
        }

        //去前后空格
        String trimStr = source.trim();

        //验证JSON字符串的完整性，是否以{}开头结尾
        if (trimStr.startsWith("{") && trimStr.endsWith("}")) {
            return true;
        }

        return false;

    }

    /*
     * 判断事件日志是否复合格式要求
     * 		事件日志：  时间戳|{}
                        时间戳需要合法：
                            a)长度合法(13位)
                            b)都是数字
                        验证JSON字符串的完整性，是否以{}开头结尾
     */
    public static boolean validEventLog(String source) {

        //判断body部分是否有数据
        if (StringUtils.isBlank(source)) {
            return false;
        }

        //去前后空格
        String trimStr = source.trim();

        String[] words = trimStr.split("\\|");

        if (words.length != 2) {
            return false;
        }

        //判断时间戳
        // isNumber()判断值是否是数值类型 123L 0xxx
        // isDigits() 判断字符串中是否只能是0-9的数字
        if (words[0].length() !=13 || !NumberUtils.isDigits(words[0])) {
            return false;
        }

        //验证JSON字符串的完整性，是否以{}开头结尾
        if (words[1].startsWith("{") && words[1].endsWith("}")) {
            return true;
        }


        return false;

    }

    public static void main(String[] args) {

        // | 在正则表达式中有特殊含义，希望只作为|解析，需要使用转义字符
        String str="a|b|c";

        List<String> list = Arrays.asList(str.split("\\|"));

        System.out.println(list);

        System.out.println(list.size());

    }

}

