package com.qc.itaojin.service.impls;

import com.qc.itaojin.common.HBaseErrorCode;
import com.qc.itaojin.exception.ItaojinHBaseException;
import com.qc.itaojin.service.IHBaseDDLService;
import com.qc.itaojin.service.common.HBaseBaseServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.springframework.util.Assert;

import java.io.IOException;

/**
 * @desc
 * @author fuqinqin
 * @date 2018-07-14
 * */
@Slf4j
public class HBaseDDLServiceImpl extends HBaseBaseServiceImpl implements IHBaseDDLService {

    @Override
    public void createNameSpace(String nameSpace) throws ItaojinHBaseException {
        Assert.hasLength(nameSpace, "nameSpace must not be null");

        Connection connection = null;
        Admin admin = null;

        try {
            connection = getConn();
            admin = connection.getAdmin();
            try{
                admin.getNamespaceDescriptor(nameSpace);
                throwException(HBaseErrorCode.NAMESPACE_ALREADY_EXISTS,
                        new Exception(String.format("namespace %s already exists",
                                nameSpace)));
            } catch (NamespaceExistException inner){

            }
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
            admin.createNamespace(namespaceDescriptor);
            log.info("namespace {} created success", nameSpace);
        } catch (IOException e) {
            throwException(HBaseErrorCode.CREATE_NAMESPACE_FAILED, e);
        } finally {
            if(!useSingleConn.get()){
                closeConn(connection);
                closeAdmin(admin);
            }
        }
    }

    @Override
    public void queryDescribe(String nameSpace, String tableName) throws ItaojinHBaseException {
        Assert.hasLength(nameSpace, "nameSpace must not be null");
        Assert.hasLength(tableName, "tableName must not be null");

        Connection connection = null;
        Admin admin = null;

        String fullName = buildTableName(nameSpace, tableName);

        try {
            connection = getConn();
            admin = connection.getAdmin();
            HTableDescriptor hTableDescriptor = admin.getTableDescriptor(TableName.valueOf(fullName));
            HColumnDescriptor[] hColumnDescriptors = hTableDescriptor.getColumnFamilies();
            if(hColumnDescriptors != null){
                for (HColumnDescriptor hColumnDescriptor : hColumnDescriptors) {
                    System.out.println(">>>>>>>>>>>>>>>>>>>>>>>.. "+hColumnDescriptor.getNameAsString());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(!useSingleConn.get()){
                closeConn(connection);
                closeAdmin(admin);
            }
        }
    }
}
