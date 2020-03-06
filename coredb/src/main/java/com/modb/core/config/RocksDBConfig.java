package com.modb.core.config;

import com.modb.common.utils.TimeUtils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@ComponentScan("com.modb.core")
@Configuration
public class RocksDBConfig {

    private static final Logger logger = LoggerFactory.getLogger(RocksDBConfig.class);

    @Value("${rocksdb.path}")
    private String dbPath;

    @Bean
    public RocksDB rocksDB() throws RocksDBException {
        RocksDB rocksDB = RocksDB.open(initOption(), dbPath);

        logger.info("rocks db open success , path is: " + dbPath);

        return rocksDB;
    }

    private Options initOption() {
        Options options = new Options();

        options.setAllowMmapReads(true);
        options.setAllowMmapWrites(true);

        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        options.setKeepLogFileNum(3);
        options.setDeleteObsoleteFilesPeriodMicros(10 * TimeUtils.ONE_MINUTE * 1000);

        return options;
    }
}
