package com.qc.itaojin.service;

import com.qc.itaojin.exception.ItaojinHBaseException;

/**
 * @desc hbase ddl 操作
 * @author fuqinqin
 * @date 2018-07-14
 * */
public interface IHBaseDDLService {

    /**
     * 创建命名空间
     * */
    void createNameSpace(String nameSpace) throws ItaojinHBaseException;

    /**
     * 查询表结构信息
     * */
    void queryDescribe(String nameSpace, String tableName) throws ItaojinHBaseException;

}
