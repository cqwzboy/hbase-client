package com.qc.itaojin.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Created by fuqinqin on 2018/7/2.
 */
@ConfigurationProperties(prefix = "itaojin.hbase")
@Data
public class ItaojinHBaseConfig {

    private static final String PARENT_DIR = "/hbase";

    /**
     * 如果指定了该参数，则按照非高可用HBase的形式创建连接，反之则按照HA形式创建
     * */
    private String master;

    /**
     * hbase在ZooKeeper注册的父级Znode
     * */
    private String parent = PARENT_DIR;

}
