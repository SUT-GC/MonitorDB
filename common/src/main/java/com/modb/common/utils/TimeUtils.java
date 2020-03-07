package com.modb.common.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

public class TimeUtils {

    public final static int ONE_SECONDS = (int) TimeUnit.SECONDS.toMillis(1);
    public final static int ONE_MINUTE = 60 * ONE_SECONDS;
    public final static int ONE_HOUR = 60 * ONE_MINUTE;
    public static final long ONE_DAY = 24 * ONE_HOUR;
    public static final long ONE_WEEK = 7 * ONE_DAY;
    public static final long ONE_MONTH = 31 * ONE_DAY;
    public static final long ONE_YEAR = 365 * ONE_DAY;

    public static Long genNowTimeMS() {

        return System.currentTimeMillis();
    }

    public static String genNowTimeMSForStr() {

        return String.valueOf(genNowTimeMS());
    }

    public static String genPathFromLocalDateTime(LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return "/";
        }

        return "/" +
                localDateTime.getYear() +
                "/" +
                localDateTime.getMonthValue() +
                "/" +
                localDateTime.getDayOfMonth() +
                "/" +
                localDateTime.getHour();
    }
}
