package com.modb.core.rocksdb;

import com.modb.core.exception.rocksdb.RocksDBOperateException;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * rocks db tunnel component
 * all rocks db operate in this class
 */
@Component
public class RocksDBTunnel {

    private static final Logger logger = LoggerFactory.getLogger(RocksDBTunnel.class);

    private RocksDB rocksDB;

    @Autowired
    public RocksDBTunnel(RocksDB rocksDB) {
        this.rocksDB = rocksDB;
    }

    /**
     * write ky but key and value both are string type
     * <p>
     * if write exception, will throws @see RocksDBOperateException
     * </p>
     *
     * @param key   kv'key
     * @param value kv'value
     * @throws RocksDBOperateException rocks db operate exception
     */
    public void writeStringKV(String key, String value) throws RocksDBOperateException {
        try (WriteOptions options = new WriteOptions(); WriteBatch writeBatch = new WriteBatch()) {
            writeBatch.put(key.getBytes(), value.getBytes());
            this.rocksDB.write(options, writeBatch);
            writeBatch.clear();
        } catch (Exception e) {
            logger.error(String.format("write rocks db error key:%s, value:%s", key, value));

            throw RocksDBOperateException.ofWriteException(e);
        }
    }

    /**
     * read ky but key and value both are string type
     * <p>
     * if read exception, will throws @see RocksDBOperateException
     * </p>
     *
     * @param key kv'key
     * @return kv'value
     * @throws RocksDBOperateException rocks db operate exception
     */
    public String getStringKV(String key) throws RocksDBOperateException {
        try {
            byte[] valueBytes = this.rocksDB.get(key.getBytes());
            return new String(valueBytes);

        } catch (Exception e) {
            logger.error(String.format("get rocks db error key:%s", key));

            throw RocksDBOperateException.ofReadException(e);
        }
    }
}
