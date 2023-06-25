package com.turing.common;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-10
 */
public class DateUtils {

    public static String getCurrentDateOfPattern(String dateFormat) {
        // 设置为东八区
        TimeZone time = TimeZone.getTimeZone("GMT+8");
        // 这个是国际化所用的
        time = TimeZone.getDefault();
        // 设置时区
        TimeZone.setDefault(time);
        Calendar calendar = Calendar.getInstance();
        DateFormat format1 = new SimpleDateFormat(dateFormat);
        Date date = calendar.getTime();
        String tDate = format1.format(date);

        return tDate;
    }

}
