package org.yinqin.mqs.common;


/**
 * 常量定义类
 *
 * @author YinQin
 * @version 1.0.6
 * @createDate 2023年10月13日
 * @since 1.0.0
 */
public final class Constants {
    public static final int SUCCESS = 1;
    public static final int ERROR = 0;
    public static final String TRAN = "TRAN";
    public static final String BATCH = "BATCH";
    public static final String BROADCAST = "BROADCAST";
    public static final String TRUE = "true";
    public static final String EMPTY = "";
    public static final String HYPHEN = "-";
    public static final String UNDER_SCORE = "_";
    public static final String BROADCAST_CONNECTOR = "_BROADCAST_";
    public static final String BROADCAST_SUFFIX = "_BROADCAST";
    public static final String BATCH_SUFFIX = "_BATCH";
    public static final String WILDCARD ="*";

    private Constants() {
        throw new AssertionError("Cannot instantiate utility class");
    }
}
