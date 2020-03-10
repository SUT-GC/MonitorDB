package com.modb.core.rocksdb;

import com.modb.common.utils.TimeUtils;
import com.modb.core.exception.rocksdb.RocksDBOperateException;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * rocks db tunnel component
 * all rocks db operate in this class
 */
public class RocksDBTunnel {

    private static final Logger logger = LoggerFactory.getLogger(RocksDBTunnel.class);

    private RocksDB rocksDB;
    private String rocksDBPath;

    public RocksDBTunnel(String path) throws RocksDBOperateException {
        this.rocksDBPath = path;
        init();
    }

    private void init() throws RocksDBOperateException {
        try {
            this.rocksDB = RocksDB.open(initOption(), this.rocksDBPath);

            logger.info("open rocks db success path: " + this.rocksDBPath);
        } catch (Exception e) {
            logger.error("open rocks db error path: " + this.rocksDBPath, e);

            throw RocksDBOperateException.ofReadException(e);
        }
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
     * batch write ky but key and value both are string type
     * <p>
     * if write exception, will throws @see RocksDBOperateException
     * </p>
     *
     * @param kvs k and v gen to map
     * @throws RocksDBOperateException rocks db operate exception
     */
    public void batchWriteStringKV(Map<String, String> kvs) throws RocksDBOperateException {
        try (WriteOptions options = new WriteOptions(); WriteBatch writeBatch = new WriteBatch()) {
            for (Map.Entry<String, String> kv : kvs.entrySet()) {
                writeBatch.put(kv.getKey().getBytes(), kv.getValue().getBytes());
            }

            this.rocksDB.write(options, writeBatch);
            writeBatch.clear();
        } catch (Exception e) {
            logger.error(String.format("batch write rocks db error kvs:%s", kvs));

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

            if (valueBytes == null || valueBytes.length <= 0) {
                return null;
            }

            return new String(valueBytes);

        } catch (Exception e) {
            logger.error(String.format("get rocks db error key:%s", key));

            throw RocksDBOperateException.ofReadException(e);
        }
    }
}
