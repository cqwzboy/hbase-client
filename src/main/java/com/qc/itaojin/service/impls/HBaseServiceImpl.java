package com.qc.itaojin.service.impls;

import com.qc.itaojin.annotation.HBaseColumn;
import com.qc.itaojin.annotation.HBaseEntity;
import com.qc.itaojin.common.HBaseErrorCode;
import com.qc.itaojin.exception.ItaojinHBaseException;
import com.qc.itaojin.service.HBaseBaseService;
import com.qc.itaojin.service.IHBaseService;
import com.qc.itaojin.util.ReflectUtils;
import com.qc.itaojin.util.ReflectionUtils;
import com.qc.itaojin.util.StringUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.Assert;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by fuqinqin on 2018/7/2.
 */
@Data
@Slf4j
public class HBaseServiceImpl extends HBaseBaseService implements IHBaseService {

    private static final String DEFAULT_FAMILY = "f1";

    ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<>();

    ThreadLocal<Boolean> useSingleConn = new ThreadLocal<>();

    public HBaseServiceImpl(){
        useSingleConn.set(false);
    }

    /**
     * 列族
     * */
    private String family = DEFAULT_FAMILY;


    private Connection getConn(){
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

    @Override
    public void updateVersions(String nameSpace, String table, String family, int versions) throws ItaojinHBaseException {
        Assert.hasText(nameSpace, "nameSpace must not be null");
        Assert.hasText(table, "table must not be null");
        Assert.hasText(family, "family must not be null");
        if(versions <= 0){
            throw new IllegalArgumentException("versions must larger than 0");
        }

        table = StringUtils.contact(nameSpace, ":", table);
        Connection connection = null;
        try {
            connection = getConn();
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(table);
            if(admin.tableExists(tableName)){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
                hColumnDescriptor.setVersions(versions, versions);
                admin.modifyColumn(tableName, hColumnDescriptor);
                log.info("updateVersions success, tables={}", table);
            }else{
                log.info("HBase table {} is not existed", table);
                throw new ItaojinHBaseException(HBaseErrorCode.UPDATE_VERSIONS_FAILED, table,
                        StringUtils.contact("HBase table {} is not existed", table));
            }
        } catch (IOException e) {
            throwException(HBaseErrorCode.UPDATE_VERSIONS_FAILED, e, table);
        } finally {
            if(!useSingleConn.get() && connection!=null){
                pool.close(connection);
            }
        }
    }

    @Override
    public void update(String nameSpace, String table, String rowKey, Map<String, String> columns) throws ItaojinHBaseException {
        Assert.hasText(nameSpace, "nameSpace must not be null");
        Assert.hasText(table, "table must not be null");
        Assert.hasText(rowKey, "rowKey must not be null");
        Assert.notEmpty(columns, "columns must not be empty");

        String tableName = StringUtils.contact(nameSpace, ":", table);

        HTable hTable = null;
        try {
            hTable = getHTable(tableName);
            for(Map.Entry<String, String> entry : columns.entrySet()){
                Put put = new Put(toBytes(rowKey));
                String columnName = entry.getKey();
                String columnValue = entry.getValue();
                put.addColumn(toBytes(family), toBytes(columnName), toBytes(columnValue));
                hTable.put(put);
            }
        } catch (IOException e) {
            throwException(HBaseErrorCode.UPDATE_FAILED, e, tableName);
        } finally {
            closeHTable(hTable);
        }
    }

    @Override
    public void delete(String nameSpace, String table, String rowKey) throws ItaojinHBaseException {
        Assert.hasText(nameSpace, "nameSpace must not be null");
        Assert.hasText(table, "table must not be null");
        Assert.hasText(rowKey, "rowKey must not be null");

        String tableName = StringUtils.contact(nameSpace, ":", table);
        HTable hTable = null;
        try {
            hTable = getHTable(tableName);
            List<Delete> list = new ArrayList<>();
            Delete delete = new Delete(toBytes(rowKey));
            list.add(delete);

            hTable.delete(list);
            log.info("delete success");
        } catch (IOException e) {
            throwException(HBaseErrorCode.DELETE_FAILED, e, tableName);
        } finally {
            closeHTable(hTable);
        }
    }

    @Override
    public <T> List<T> scanAll(Class<T> clazz) throws ItaojinHBaseException {
        Assert.notNull(clazz, "clazz must not be null");
        ReflectUtils.isAnnotationPresent(clazz, HBaseEntity.class);

        String tableName = null;
        Annotation[] clazzDeclaredAnnotations = clazz.getDeclaredAnnotations();
        for (Annotation clazzDeclaredAnnotation : clazzDeclaredAnnotations) {
            if(clazzDeclaredAnnotation.annotationType().equals(HBaseEntity.class)){
                tableName = ((HBaseEntity)clazzDeclaredAnnotation).table();
                break;
            }
        }

        HTable hTable = null;
        ResultScanner resultScanner = null;

        try{
            hTable = getHTable(tableName);
            log.info("hbase table is {}", tableName);
            Scan scan = new Scan();
            resultScanner = hTable.getScanner(scan);

            if(resultScanner == null){
                return null;
            }

            // get all fields
            Field[] fields = clazz.getDeclaredFields();
            List<T> list = new ArrayList<>();
            for (Result result : resultScanner) {
                T t = clazz.newInstance();
                for (Field field : fields) {
                    String fieldName = field.getName();
                    String setMethodName = ReflectUtils.buildSet(fieldName);

                    // whether use HBaseColumn
                    if(field.isAnnotationPresent(HBaseColumn.class)){
                        // get all annotations
                        Annotation[] fieldAnnotations = field.getDeclaredAnnotations();
                        anno:for (Annotation fieldAnnotation : fieldAnnotations) {
                            if(fieldAnnotation.annotationType().equals(HBaseColumn.class)){
                                fieldName = ((HBaseColumn)fieldAnnotation).value();
                                break anno;
                            }
                        }
                    }else{
                        // trans to hump format
                        fieldName = StringUtils.converseToHump(fieldName);
                    }


                    byte[] bys = result.getValue(Bytes.toBytes("f1"), Bytes.toBytes(fieldName));
                    if(bys == null){
                        continue;
                    }

                    String value = new String(bys);
                    Method method = ReflectionUtils.findMethod(clazz, setMethodName, ReflectUtils.mapClass(field.getType()));
                    method.invoke(t, ReflectUtils.transValue(field, value));
                }
                list.add(t);
            }

            return list;
        } catch (Exception e) {
            throwException(HBaseErrorCode.UPDATE_FAILED, e, tableName);
        } finally {
            closeHTable(hTable);
            closeResultScanner(resultScanner);
        }

        return null;
    }
}
