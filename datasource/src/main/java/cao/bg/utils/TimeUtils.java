package cao.bg.utils;

import org.apache.commons.lang3.time.FastDateFormat;

/**
 * @Author : CGL
 * @Date : 2022 2022/1/4 16:38
 * @Desc : 时间工具类
 */
public class TimeUtils {

    public static String format(Long timestamp,String pattern){
        return FastDateFormat.getInstance(pattern).format(timestamp);
    }

    public static void main(String[] args) {
        String format = TimeUtils.format(System.currentTimeMillis(), "yyyy-MM-dd");
        System.out.println(format);
    }
}
