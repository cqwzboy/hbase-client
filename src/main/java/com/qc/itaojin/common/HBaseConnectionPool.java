package com.qc.itaojin.common;

import com.qc.itaojin.config.ItaojinHBaseConfig;
import com.qc.itaojin.config.ItaojinZKConfig;
import com.qc.itaojin.util.StringUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by fuqinqin on 2018/7/10.
 */
@Data
@Slf4j
public class HBaseConnectionPool {

    private ItaojinHBaseConfig hBaseConfig;
    private ItaojinZKConfig zkConfig;

    private Configuration configuration;

    public void init(){
        if(StringUtils.isNotBlank(hBaseConfig.getMaster())){
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.property.clientPort", String.valueOf(zkConfig.getPort()));
            configuration.set("hbase.zookeeper.quorum", zkConfig.getQuorum());
            configuration.set("hbase.master", hBaseConfig.getMaster());
            log.info("none HA HBase connection pool init success");
        }else{
            try {
                configuration = HBaseConfiguration.createClusterConf(HBaseConfiguration.create(),
                        StringUtils.contact(zkConfig.getQuorum(), ":", String.valueOf(zkConfig.getPort()), ":", hBaseConfig.getParent()));
                log.info("HA HBase connection pool init success");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 返回一个连接
     * */
    public Connection getConn(){
        try {
            return ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public void close(Connection connection){
        if(connection != null){
            try {
                connection.close();
            } catch (IOException e) {

            }
        }
    }

}
