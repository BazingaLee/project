package com.bazinga.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;


//URL字符串转码
public class URLUtil {
    public static void main(String[] args) {
        try {
            System.out.println(URLDecoder.decode("http%3A%2F%2Fw.baofoo.net%2Fpages%2Fviewpage.action%3FpageId%3D8074823%26tdsourcetag%3Ds_pcqq_aiomsg"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
