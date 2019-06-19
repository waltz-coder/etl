package com.waltz.etl.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Ip地址解析工具，输入ip地址，输出ip所属的地区。。
 */
public class IPUtil {
    private static final TreeMap<Long, String> map = new TreeMap<>();

    /**
     * 在静态块里加载 ip字典，读入treeMap中
     */
    static {
        if (map != null && map.isEmpty()) {
            try (
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(IPUtil.class.getResourceAsStream("/ip.dat"), StandardCharsets.UTF_8))
            ) {
                String line;
                Pattern pattern;
                Matcher matcher;
                while ((line = bufferedReader.readLine()) != null) {
                    pattern = Pattern.compile("((\\d{1,3}\\.){3}\\d{1,3})\\s+((\\d{1,3}\\.){3}\\d{1,3})\\s+(.+)");
                    matcher = pattern.matcher(line);
                    if (matcher.find()) {
                        map.put(new Long(stringToLong(matcher.group(3))), matcher.group(5).split(" ")[0]);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     * @param ip 输入ip地址的string格式
     * @return 返回ip地址去掉。之后所对应的Long类型
     */
    private static Long stringToLong(String ip) {
        String[] splits = ip.split("\\.");
        if (splits.length != 4) {
            return null;
        }
        long lip = 0;
        int offset = 24;
        for (String split : splits) {
            lip |= Long.parseLong(split) << offset;
            offset -= 8;
        }
        return lip;
    }

    /**
     *
     * @param ip 地址所对应的Long类型
     * @return "."分隔的ip地址字符串类型
     */
    private static String longToString(Long ip) {
        StringBuffer ipBuffer = new StringBuffer();
        long mask = 255l;
        int offset = 8;
        for (int i = 0; i < 4; i++) {
            ipBuffer.insert(0, (ip & mask) + ".");
            ip >>= offset;
        }
        return ipBuffer.substring(0, ipBuffer.length() - 1);
    }

    /**
     *
     * @param ip 输入字符串类型"."分隔的 ip地址
     * @return 输入对应的ip地址类型所在地。
     */
    public static String getAreaByIp(String ip) {
        String area;
        if (map == null || map.isEmpty()) {
            System.out.print("map is null");
            System.exit(-1);
        }
        Long ceilingKey = map.ceilingKey(stringToLong(ip));
        if (ceilingKey == null) {
            area = "未知区域";
        } else {
            area = map.get(ceilingKey);
        }
        return area;
    }

    public static void main(String[] args) {
        System.out.println(new IPUtil().getAreaByIp("1.2.4.8"));
    }
}
