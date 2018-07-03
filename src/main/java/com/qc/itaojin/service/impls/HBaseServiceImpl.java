package com.qc.itaojin.service.impls;

import com.qc.itaojin.config.ItaojinHBaseConfig;
import com.qc.itaojin.config.ItaojinZKConfig;
import com.qc.itaojin.service.BaseService;
import com.qc.itaojin.service.IHBaseService;
import com.qc.itaojin.util.StringUtils;
import lombok.Data;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by fuqinqin on 2018/7/2.
 */
@Data
public class HBaseServiceImpl extends BaseService implements IHBaseService {

    private ItaojinHBaseConfig hBaseConfig;
    private ItaojinZKConfig zkConfig;

    private Configuration configuration;

    private static final String DEFAULT_FAMILY = "f1";

    /**
     * 列族
     * */
    private String family = DEFAULT_FAMILY;

    private Configuration genConfiguration(){
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", String.valueOf(zkConfig.getPort()));
        configuration.set("hbase.zookeeper.quorum", zkConfig.getQuorum());
        configuration.set("hbase.master", hBaseConfig.getMaster());
        return configuration;
    }

    private Configuration getConfiguration(){
        if(this.configuration == null){
            this.configuration = genConfiguration();
        }

        return this.configuration;
    }

    @Override
    public boolean update(String nameSpace, String table, String rowKey, Map<String, String> columns) {
        if(StringUtils.isBlank(nameSpace) || StringUtils.isBlank(table) || MapUtils.isEmpty(columns)){
            return false;
        }

        String tableName = StringUtils.contact(nameSpace, ":", table);

        HTable hTable = null;
        try {
            hTable = new HTable(getConfiguration(), tableName);
            for(Map.Entry<String, String> entry : columns.entrySet()){
                Put put = new Put(toBytes(rowKey));
                String columnName = entry.getKey();
                String columnValue = entry.getValue();
                put.addColumn(toBytes(family), toBytes(columnName), toBytes(columnValue));
                hTable.put(put);
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(hTable != null){
                try {
                    hTable.close();
                } catch (IOException e) {

                }
            }
        }
        return false;
    }

    @Override
    public boolean delete(String nameSpace, String table, String rowKey) {
        String tableName = StringUtils.contact(nameSpace, ":", table);
        HTable hTable = null;
        try {
            hTable = new HTable(getConfiguration(), tableName);
            List<Delete> list = new ArrayList<>();
            Delete delete = new Delete(toBytes(rowKey));
            list.add(delete);

            hTable.delete(list);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(hTable != null){
                try {
                    hTable.close();
                } catch (IOException e) {

                }
            }
        }
        return false;
    }
}
