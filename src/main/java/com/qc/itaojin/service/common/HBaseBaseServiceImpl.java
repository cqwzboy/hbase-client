package com.qc.itaojin.service.common;

import com.qc.itaojin.common.HBaseConnectionPool;
import com.qc.itaojin.common.HBaseErrorCode;
import com.qc.itaojin.exception.ItaojinHBaseException;
import com.qc.itaojin.service.BaseServiceImpl;
import com.qc.itaojin.service.IBaseService;
import com.qc.itaojin.util.StringUtils;
import lombok.Data;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.springframework.util.Assert;

import java.io.IOException;

/**
 * Created by fuqinqin on 2018/7/13.
 */
@Data
public class HBaseBaseServiceImpl extends BaseServiceImpl implements IBaseService {

    protected HBaseConnectionPool pool;

    protected ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<>();

    protected ThreadLocal<Boolean> useSingleConn = new ThreadLocal<>();

    private static final String DEFAULT_FAMILY = "f1";

    /**
     * 列族
     * */
    protected String family = DEFAULT_FAMILY;

    public HBaseBaseServiceImpl(){
        useSingleConn.set(false);
    }

    protected Connection getConn(){
        // 同一线程，较多使用Hbase连接时，可以采用始终使用单一连接的方案
        if(useSingleConn.get()){
            if(connectionThreadLocal.get() == null){
                connectionThreadLocal.set(pool.getConn());
            }

            return connectionThreadLocal.get();
        }

        return pool.getConn();
    }

    @Override
    public void useSingleConn() {
        this.useSingleConn.set(true);
    }

    @Override
    public void closeSingleConn(){
        if(connectionThreadLocal.get() != null){
            pool.close(connectionThreadLocal.get());
            connectionThreadLocal.set(null);
        }

        this.useSingleConn.set(false);
    }

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
     * 关闭 Admin
     * */
    protected void closeAdmin(Admin admin){
        if(admin != null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 关闭Connection
     * */
    protected void closeConn(Connection connection){
        if(connection != null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
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

    protected void throwException(int defaultErrorCode, Exception e) throws ItaojinHBaseException {
        Assert.notNull(e, "ex must not be null");

        String message = e.getMessage();
        throw new ItaojinHBaseException(defaultErrorCode, e.getMessage());
    }

}
