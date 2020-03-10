package com.modb.core.meta;

import com.modb.common.utils.TimeUtils;
import com.modb.core.exception.rocksdb.RocksDBOperateException;
import com.modb.core.executors.MetaDBExecutors;
import com.modb.core.rocksdb.RocksDBTunnel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class SequenceIdGenerator {

    private static final Logger logger = LoggerFactory.getLogger(SequenceIdGenerator.class);

    private RocksDBTunnel metaDB;

    private Map<String, AtomicLong> sidSeedMap = null;
    private Map<String, String> sidPathMap = null;

    @Autowired
    public SequenceIdGenerator(RocksDBTunnel metaDB) {
        this.metaDB = metaDB;
    }

    public String genSid(String project, LocalDateTime localDateTime) {
        if (sidSeedMap == null) {
            syncReadMetaDB(project);
        }

        String path = TimeUtils.genPathFromLocalDateTime(localDateTime);

        String sid = null;

        if (sidPathMap.get(project) != null && sidPathMap.get(project).equals(path)) {
            long sidSeed = this.sidSeedMap.get(project).incrementAndGet();
            sid = path + "/" + sidSeed;
        }

        if (sidPathMap.get(project) != null && !sidPathMap.get(project).equals(path)) {
            this.sidPathMap.put(project, path);
            this.sidSeedMap.put(project, new AtomicLong(0));
            long sidSeed = this.sidSeedMap.get(project).incrementAndGet();
            sid = path + "/" + sidSeed;
        }

        if (sidPathMap.get(project) == null) {
            this.sidPathMap.put(project, path);
            this.sidSeedMap.put(project, new AtomicLong(0));
            long sidSeed = this.sidSeedMap.get(project).incrementAndGet();
            sid = path + "/" + sidSeed;

        }

        asyncWriteMetaDB();

        return sid;
    }

    private synchronized boolean syncReadMetaDB(String project) {
        try {
            String path = this.metaDB.getStringKV(project + "path");
            String seed = this.metaDB.getStringKV(project + "seed");

            this.sidPathMap = new ConcurrentHashMap<>();
            this.sidSeedMap = new ConcurrentHashMap<>();

            if (path != null && seed != null) {
                this.sidPathMap.put(project, path);
                this.sidSeedMap.put(project, new AtomicLong(Long.parseLong(seed)));
            }
        } catch (RocksDBOperateException e) {
            // pass
        }

        return true;
    }

    public void asyncWriteMetaDB() {
        MetaDBExecutors.RECORD_SEQUENCE.execute(() -> {
            dumpSidSeedMap();
            dumpSidPathMap();
        });
    }

    private synchronized void dumpSidPathMap() {
        logger.info("dump sid path..." + this.sidPathMap);

        if (this.sidPathMap.isEmpty()) {
            return;
        }


        try {
            metaDB.batchWriteStringKV(this.sidPathMap);
        } catch (RocksDBOperateException e) {
            // pass
        }
    }

    private synchronized void dumpSidSeedMap() {
        logger.info("dump sid seed..." + this.sidSeedMap);

        if (this.sidSeedMap.isEmpty()) {
            return;
        }

        Map<String, String> kvs = new HashMap<>(this.sidSeedMap.size());
        this.sidSeedMap.forEach((k, v) -> {
            kvs.put(k, v.toString());
        });

        try {
            metaDB.batchWriteStringKV(kvs);
        } catch (RocksDBOperateException e) {
            // pass
        }
    }
}
