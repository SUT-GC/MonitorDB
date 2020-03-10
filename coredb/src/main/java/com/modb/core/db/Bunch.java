package com.modb.core.db;

import com.modb.common.utils.FileUtils;
import com.modb.common.utils.JacksonUtils;
import com.modb.common.utils.TimeUtils;
import com.modb.core.exception.rocksdb.RocksDBOperateException;
import com.modb.core.rocksdb.RocksDBFactory;
import com.modb.core.rocksdb.RocksDBTunnel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Map;

public class Bunch {
    private static final Logger logger = LoggerFactory.getLogger(Bunch.class);

    private LocalDate date;
    private RocksDBTunnel metaDB;
    private RocksDBTunnel tagDB;
    private RocksDBTunnel fieldDB;

    private String rootPath;
    private String bunchPath;
    private String projectName;

    private static final String META_DB_FILE = "/meta";
    private static final String TAG_DB_FILE = "/tag";
    private static final String FIELD_DB_FILE = "/field";


    public Bunch(String rootPath, String projectName, LocalDateTime dateTime) {
        // init root path
        this.rootPath = rootPath;

        //init projectName
        this.projectName = projectName;

        // init bunch path
        String bunchPathE = "/" + projectName + TimeUtils.genPathFromLocalDateTime(dateTime);
        if (this.rootPath.endsWith("/")) {
            this.bunchPath = rootPath + bunchPathE.replaceFirst("/", "");
        } else {
            this.bunchPath = rootPath + bunchPathE;
        }

        String metaDBPath = bunchPath + META_DB_FILE;
        String tagDBPath = bunchPath + TAG_DB_FILE;
        String fieldDBPath = bunchPath + FIELD_DB_FILE;

        FileUtils.mkdirs(Arrays.asList(rootPath, bunchPath, metaDBPath, tagDBPath, fieldDBPath));

        // init meta db
        this.metaDB = RocksDBFactory.gen(metaDBPath);
        // init tag db
        this.tagDB = RocksDBFactory.gen(tagDBPath);
        // init field db
        this.fieldDB = RocksDBFactory.gen(fieldDBPath);

        logger.info("bunch init success, path: " + this.bunchPath);
    }

    public void write(String sid, Map<String, String> tags, Map<String, String> fields) throws RocksDBOperateException {
        String tagStr = JacksonUtils.toJsonStringCoverE(tags);
        String filedStr = JacksonUtils.toJsonStringCoverE(fields);

        this.tagDB.writeStringKV(sid, tagStr);
        this.fieldDB.writeStringKV(sid, filedStr);
    }
}
