package com.bazinga.util;

import java.text.SimpleDateFormat;
import java.util.Date;


//URL字符串转码
public class URLUtil {
    public static void main(String[] args) {
        try {
            System.out.println(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
