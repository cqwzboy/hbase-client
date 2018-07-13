package com.qc.itaojin.common;

/**
 * @desc 错误码
 * @author fuqinqin
 * @date 2018-07-10
 */
public class HBaseErrorCode {

    /**
     * 表不存在
     * */
    public static final int TABLE_NOT_FOUND = 100;
    /**
     * 修改版本号失败
     * */
    public static final int UPDATE_VERSIONS_FAILED = 101;
    /**
     * 修改数据失败
     * */
    public static final int UPDATE_FAILED = 102;
    /**
     * 删除数据失败
     * */
    public static final int DELETE_FAILED = 103;
    /**
     * NameSpace已经存在
     * */
    public static final int NAMESPACE_ALREADY_EXISTS = 104;
    /**
     * 创建命名空间失败
     * */
    public static final int CREATE_NAMESPACE_FAILED = 105;

}
