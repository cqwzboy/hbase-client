package com.qc.itaojin.service;

import com.qc.itaojin.common.HBaseConnectionPool;
import com.qc.itaojin.common.HBaseErrorCode;
import com.qc.itaojin.exception.ItaojinHBaseException;
import com.qc.itaojin.util.StringUtils;
import lombok.Data;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.springframework.util.Assert;

import java.io.IOException;

/**
 * Created by fuqinqin on 2018/7/13.
 */
@Data
public class HBaseBaseService extends BaseService {

    protected HBaseConnectionPool pool;

    /**
     * 创建HTable
     * */
    protected HTable getHTable(String tableName) throws ItaojinHBaseException {
        try {
            return new HTable(pool.getConfiguration(), tableName);
        } catch (IOException e) {
            e.printStackTrace();
            String message = e.getMessage();
            if(e instanceof TableNotFoundException){
                throw new ItaojinHBaseException(HBaseErrorCode.TABLE_NOT_FOUND, tableName, e.getMessage());
            }else if(message.contains("was not found") && message.contains("Table")){
                throw new ItaojinHBaseException(HBaseErrorCode.TABLE_NOT_FOUND, tableName, e.getMessage());
            }
            throw new ItaojinHBaseException(HBaseErrorCode.UPDATE_FAILED, tableName, e.getMessage());
        }
    }

    /**
     * 关闭HTable
     * */
    protected void closeHTable(HTable hTable){
        if(hTable != null){
            try {
                hTable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 关闭ResultScanner
     * */
    protected void closeResultScanner(ResultScanner resultScanner){
        if(resultScanner != null){
            resultScanner.close();
        }
    }

    /**
     * 构建hbase的tableName
     * */
    protected String buildTableName(String nameSpace, String table){
        Assert.hasLength(nameSpace, "nameSpace must not be null");
        Assert.hasLength(table, "table must not be null");

        return StringUtils.contact(nameSpace, ":", table);
    }

    /**
     * 抛异常
     * */
    protected void throwException(int defaultErrorCode, Exception e, String tableName) throws ItaojinHBaseException {
        Assert.notNull(e, "ex must not be null");

        String message = e.getMessage();
        if(e instanceof TableNotFoundException){
            throw new ItaojinHBaseException(HBaseErrorCode.TABLE_NOT_FOUND, tableName, e.getMessage());
        }else if(message.contains("was not found") && message.contains("Table")){
            throw new ItaojinHBaseException(HBaseErrorCode.TABLE_NOT_FOUND, tableName, e.getMessage());
        }
        throw new ItaojinHBaseException(defaultErrorCode, tableName, e.getMessage());
    }

}
