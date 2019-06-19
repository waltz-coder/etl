package com.waltz.etl.utils;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;

import java.io.IOException;

/**
 * get detail information from useragent
 */

public class UserAgentSparser {
    private static String userAgentString;
    private static UASparser uaSparser = null;
    private static UserAgentInfo userAgentInfo;
    private static  UserAgentSparser userAgentSparser;

    public UserAgentSparser(){}
    public UserAgentSparser(String userAgentString){
        this.userAgentString=userAgentString;
    }
    static {
        try {

            // 加载user_agent 文件
            uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream());
            // java.lang.UnsupportedClassVersionError:
            // cz/mallat/uasparser/UASparser : Unsupported major.minor version 51.0
            // 用jdk1.6测试时会报以上错，需要jdk1.7以上版本支持

        } catch (IOException e) {

            e.printStackTrace();
            System.exit(-1);

        }
    }
    /**
     *
     * 获取instance
     *
     */
     public static UserAgentSparser getInstance(String userAgentString) {
         if(userAgentSparser==null){
             userAgentSparser = new UserAgentSparser(userAgentString);
         }
        return userAgentSparser;
    }
    /**
     * 初始化userAgentInfo， initUserAgentInfo。
     */
    private static void initUserAgentInfo() {
        initUserAgentInfo(userAgentString);
    }

    /**
     * 初始化userAgentInfo， initUserAgentInfo。
     */
    private static void initUserAgentInfo(String userAgentString) {
        try {
            userAgentInfo = uaSparser.parse(userAgentString);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

/****------------------------------------------------------------------------------------------------------------
 *                                             获取操作系统的信息                                                *
 ----------------------------------------------------------------------------------------------------------------*/
    /**
     * @return OsFamily信息
     */
    public static String getOsFamily() {
        return getOsFamily(userAgentString);
    }

    /**
     * @param userAgentString 输入userAgent字符串
     * @return OsFamily信息
     */
    private static String getOsFamily(String userAgentString) {
        initUserAgentInfo(userAgentString);
        return userAgentInfo.getOsFamily();
    }

    /**
     *
     * @return OsName信息
     */
    public static String getOsName() {
        return getOsName(userAgentString);
    }

    /**
     * @param userAgentString 输入userAgent字符串
     * @return OsName信息
     */
    private static String getOsName(String userAgentString) {
        initUserAgentInfo(userAgentString);
        return userAgentInfo.getOsName();
    }

    /**
     *
     * @return OsCompany信息
     */
    public static String getOsCompany() {
        return getOsCompany(userAgentString);
    }

    /**
     * @param userAgentString 输入userAgent字符串
     * @return OsCompany信息
     */
    private static String getOsCompany(String userAgentString) {
        initUserAgentInfo(userAgentString);
        return userAgentInfo.getOsCompany();
    }

    /**
     *
     * @return OsUrl信息
     */
    public static String getOsUrl() {
        return getOsUrl(userAgentString);
    }

    /**
     * @param userAgentString 输入userAgent字符串
     * @return OsUrl信息
     */
    private static String getOsUrl(String userAgentString) {
        initUserAgentInfo(userAgentString);
        return userAgentInfo.getOsUrl();
    }

    /**
     * @return OsIcon信息
     */
    public static String getOsIcon() {
        return getOsIcon(userAgentString);
    }

    /**
     * @param userAgentString 输入userAgent字符串
     * @return OsIcon信息
     */
    private static String getOsIcon(String userAgentString) {
        initUserAgentInfo(userAgentString);
        return userAgentInfo.getOsIcon();
    }

    /**
     *
     * @return OsCompanyUrl信息
     */
    public static String getOsCompanyUrl() {
        return getOsCompanyUrl(userAgentString);
    }

    /**
     * @param userAgentString 输入userAgent字符串
     * @return OsCompanyUrl信息
     */
    private static String getOsCompanyUrl(String userAgentString) {
        initUserAgentInfo(userAgentString);
        return userAgentInfo.getOsCompanyUrl();
    }

/****------------------------------------------------------------------------------------------------------------
 *                                              获取浏览器的信息                                                *
 ----------------------------------------------------------------------------------------------------------------*/
    /**
     * @return BrowserVersion信息
     */
    public static String getBrowserVersionInfo() {
        return getBrowserVersionInfo(userAgentString);
    }

    /**
     * @param userAgentString 输入userAgent字符串
     * @return BrowserVersion信息
     */
    private static String getBrowserVersionInfo(String userAgentString) {
        initUserAgentInfo(userAgentString);
        return userAgentInfo.getBrowserVersionInfo();
    }
/****------------------------------------------------------------------------------------------------------------
 *                                               获取设备的信息                                                 *
 ----------------------------------------------------------------------------------------------------------------*/
    /**
     * @return DeviceType信息
     */
    public static String getDeviceType() {
        return getDeviceType(userAgentString);
    }

    /**
     * @param userAgentString 输入userAgent字符串
     * @return DeviceType信息
     */
    private static String getDeviceType(String userAgentString) {
        initUserAgentInfo(userAgentString);
        return userAgentInfo.getDeviceType();
    }

    /**
     * @return Type信息
     */
    public static String getType() {
        return getType(userAgentString);
    }

    /**
     * @param userAgentString 输入userAgent字符串
     * @return Type信息
     */
    private static String getType(String userAgentString) {
        initUserAgentInfo(userAgentString);
        return userAgentInfo.getType();
    }

    /**
     * @return DeviceIcon信息
     */
    public static String getDeviceIcon() {
        return getDeviceIcon(userAgentString);
    }

    /**
     * @param userAgentString 输入userAgent字符串
     * @return DeviceIcon信息
     */
    private static String getDeviceIcon(String userAgentString) {
        initUserAgentInfo(userAgentString);
        return userAgentInfo.getDeviceIcon();
    }

    /**
     *
     * @return DeviceInfoUrl信息
     */
    public static String getDeviceInfoUrl() {
        return getDeviceInfoUrl(userAgentString);
    }

    /**
     * @param userAgentString 输入userAgent字符串
     * @return DeviceInfoUrl信息
     */
    private static String getDeviceInfoUrl(String userAgentString) {
        initUserAgentInfo(userAgentString);
        return userAgentInfo.getDeviceInfoUrl();
    }

    /****------------------------------------------------------------------------------------------------------------
     *                                                 获取Us的信息                                                 *
     ----------------------------------------------------------------------------------------------------------------*/

    /**
     * @return UaFamily信息
     */
    public static String getUaFamily() {
        return getUaFamily(userAgentString);
    }

    /**
     * @param userAgentString 输入userAgent字符串
     * @return UaFamily信息
     */
    private static String getUaFamily(String userAgentString) {
        initUserAgentInfo(userAgentString);
        return userAgentInfo.getUaFamily();
    }

    /**
     * @return UaName信息
     */
    public static String getUaName() {
        return getUaName(userAgentString);
    }

    /**
     * @param userAgentString 输入userAgent字符串
     * @return UaName信息
     */
    private static String getUaName(String userAgentString) {
        initUserAgentInfo(userAgentString);
        return userAgentInfo.getUaName();
    }

    /**
     * @return UaCompany信息
     */
    public static String getUaCompany() {
        return getUaCompany(userAgentString);
    }

    /**
     * @param userAgentString 输入userAgent字符串
     * @return UaCompany信息
     */
    private static String getUaCompany(String userAgentString) {
        initUserAgentInfo(userAgentString);
        return userAgentInfo.getUaCompany();
    }

    /**
     * @return UaCompanyUrl信息
     */
    public static String getUaCompanyUrl() {
        return getUaCompanyUrl(userAgentString);
    }

    /**
     * @param userAgentString 输入userAgent字符串
     * @return UaCompanyUrl信息
     */
    private static String getUaCompanyUrl(String userAgentString) {
        initUserAgentInfo(userAgentString);
        return userAgentInfo.getUaCompanyUrl();
    }

    /**
     * @return UaIcon信息
     */
    public static String getUaIcon() {
        return getUaIcon(userAgentString);
    }

    /**
     * @param userAgentString 输入userAgent字符串
     * @return UaIcon信息
     */
    private static String getUaIcon(String userAgentString) {
        initUserAgentInfo(userAgentString);
        return userAgentInfo.getUaIcon();
    }

    /**
     * @return UaInfoUrl信息
     */
    public static String getUaInfoUrl() {
        return getUaInfoUrl(userAgentString);
    }

    /**
     * @param userAgentString 输入userAgent字符串
     * @return UaInfoUrl信息
     */
    private static String getUaInfoUrl(String userAgentString) {
        initUserAgentInfo(userAgentString);
        return userAgentInfo.getUaInfoUrl();
    }

    /**
     * @return UaUrl信息
     */
    public static String getUaUrl() {
        return getUaUrl(userAgentString);
    }

    /**
     * @param userAgentString 输入userAgent字符串
     * @return UaUrl信息
     */
    private static String getUaUrl(String userAgentString) {
        initUserAgentInfo(userAgentString);
        return userAgentInfo.getUaUrl();
    }
}
