package com.bazinga.util;

import java.math.BigDecimal;
import java.text.DecimalFormat;

/*
    1.关于 Java 小数点位数保留的解决方案

 */
public class BigDecimalUtil {

    public static void main(String[] args) {
        double d = 1.19;
        System.out.println(formatDecimal1(d));
    }

    public static String formatDecimal1(double d) {
        DecimalFormat df = new DecimalFormat("0.00");
        return df.format(d);
    }

    public static String formatDecimal2(double d) {
        DecimalFormat df = new DecimalFormat("#.##");
        return df.format(d);
    }
    public static String formatDecimal3(double d){
        return String.format("%.2f",d);
    }

    public static double formatDecimal4(double d){
        BigDecimal bd=new BigDecimal(d);
        double d1=bd.setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
        return d1;
    }
}
