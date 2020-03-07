package com.modb.core.config;

import com.modb.core.rocksdb.RocksDBFactory;
import com.modb.core.rocksdb.RocksDBTunnel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@ComponentScan("com.modb.core")
@Configuration
public class CoreDBConfig {

    private static final Logger logger = LoggerFactory.getLogger(CoreDBConfig.class);

    @Value("${monitordb.meta.path}")
    private String metaDBPath;

    @Bean("metaDB")
    public RocksDBTunnel metaDB() {

        return RocksDBFactory.gen(metaDBPath);
    }
}
