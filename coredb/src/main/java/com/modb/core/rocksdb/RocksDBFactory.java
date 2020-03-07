package com.modb.core.rocksdb;

import com.modb.common.utils.TimeUtils;
import com.modb.core.exception.BaseRuntimeException;
import com.modb.core.exception.rocksdb.RocksDBOperateException;
import com.modb.core.exception.system.SystemException;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

public class RocksDBFactory {

    public static final String INIT_ROCKS_DB_ERROR_CODE = "INIT_ROCKS_DB_ERROR_CODE";

    public static RocksDBTunnel gen(String path) {
        try {
            return new RocksDBTunnel(path);
        } catch (RocksDBOperateException e) {
            throw SystemException.of("init rocks db error, path: " + path, INIT_ROCKS_DB_ERROR_CODE, e);
        }
    }
}
