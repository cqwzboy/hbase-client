package com.qc.itaojin.autoConfiguration;

import com.qc.itaojin.config.ItaojinHBaseConfig;
import com.qc.itaojin.config.ItaojinZKConfig;
import com.qc.itaojin.service.IHBaseService;
import com.qc.itaojin.service.impls.HBaseServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by fuqinqin on 2018/7/3.
 */
@Configuration
@EnableConfigurationProperties({ItaojinHBaseConfig.class, ItaojinZKConfig.class})
@ConditionalOnClass(IHBaseService.class)
@ConditionalOnProperty(prefix = "itaojin", value = "enabled", matchIfMissing = true)
public class ItaojinHBaseAutoConfiguration {

    @Autowired
    private ItaojinHBaseConfig hBaseConfig;
    @Autowired
    private ItaojinZKConfig zkConfig;

    @Bean
    @ConditionalOnMissingBean
    public IHBaseService ihBaseService(){
        IHBaseService hBaseService = new HBaseServiceImpl();
        ((HBaseServiceImpl) hBaseService).setHBaseConfig(hBaseConfig);
        ((HBaseServiceImpl) hBaseService).setZkConfig(zkConfig);
        return hBaseService;
    }

}
