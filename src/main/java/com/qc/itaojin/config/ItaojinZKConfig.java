package com.qc.itaojin.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Created by fuqinqin on 2018/7/3.
 */
@ConfigurationProperties(prefix = "itaojin.zookeeper")
@Data
public class ItaojinZKConfig {
    private static final int DEFAULT_PORT = 2181;

    private String quorum;

    private int port = DEFAULT_PORT;
}
