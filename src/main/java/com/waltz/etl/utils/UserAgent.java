package com.waltz.etl.utils;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;

import java.io.IOException;

public class UserAgent {

    static UASparser uasParser = null;

    static {

        try {

            // 加载user_agent 文件
            uasParser = new UASparser(OnlineUpdater.getVendoredInputStream());

            // java.lang.UnsupportedClassVersionError:
            // cz/mallat/uasparser/UASparser : Unsupported major.minor version 51.0
            // 用jdk1.6测试时会报以上错，需要jdk1.7以上版本支持

        } catch (IOException e) {

            e.printStackTrace();

        }
    }

    public static void main(String[] args) {
        String str = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.130 Safari/537.36";
//        try {
//            UserAgentInfo userAgentInfo = UserAgent.uasParser.parse(str);
//            System.out.println("操作系统家族：" + userAgentInfo.getOsFamily());
//            System.out.println("操作系统详细名称：" + userAgentInfo.getOsName());
//            System.out.println("浏览器名称和版本:" + userAgentInfo.getUaName());
//            System.out.println("类型：" + userAgentInfo.getType());
//            System.out.println("浏览器名称：" + userAgentInfo.getUaFamily());
//            System.out.println("浏览器版本：" + userAgentInfo.getBrowserVersionInfo());
//            System.out.println("设备类型：" + userAgentInfo.getDeviceType());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        UserAgentSparser userAgentSparser = UserAgentSparser.getInstance(str);

        System.out.println(userAgentSparser.getOsCompany());
        System.out.println(userAgentSparser.getOsCompanyUrl());
        System.out.println(userAgentSparser.getOsFamily());
        System.out.println(userAgentSparser.getOsIcon());
        System.out.println(userAgentSparser.getOsName());
        System.out.println(userAgentSparser.getOsUrl());
        System.out.println(userAgentSparser.getBrowserVersionInfo());
        System.out.println(userAgentSparser.getDeviceIcon());
        System.out.println(userAgentSparser.getDeviceInfoUrl());
        System.out.println(userAgentSparser.getDeviceType());
        System.out.println(userAgentSparser.getUaCompany());
        System.out.println(userAgentSparser.getUaCompanyUrl());
        System.out.println(userAgentSparser.getUaFamily());
        System.out.println(userAgentSparser.getUaIcon());
        System.out.println(userAgentSparser.getUaInfoUrl());
        System.out.println(userAgentSparser.getUaName());
        System.out.println(userAgentSparser.getUaUrl());
    }
}

