package com.qc.itaojin.service;

import java.util.Map;

/**
 * @desc HBase服务类
 * @author fuqinqin
 * @date 2018-07-02
 */
public interface IHBaseService {

    /**
     * 插入/修改 数据，所有的列数据都放在 f1 列族下
     * */
    boolean update(String nameSpace, String table, String rowKey, Map<String, String> columns);

    /**
     * 删除一行数据
     * */
    boolean delete(String nameSpace, String tableName, String rowKey);

}
