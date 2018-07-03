package com.qc.itaojin.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Created by fuqinqin on 2018/7/2.
 */
@ConfigurationProperties(prefix = "itaojin.hbase")
@Data
public class ItaojinHBaseConfig {

    private static final int DEFAULT_PORT = 2181;

    private String master;

}
