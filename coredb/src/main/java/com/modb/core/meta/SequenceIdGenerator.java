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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class SequenceIdGenerator {

    private static final Logger logger = LoggerFactory.getLogger(SequenceIdGenerator.class);

    private RocksDBTunnel metaDB;

    private Map<String, AtomicLong> sidSeedMap = new ConcurrentHashMap<>();
    private Map<String, String> sidPathMap = new ConcurrentHashMap<>();

    @Autowired
    public SequenceIdGenerator(RocksDBTunnel metaDB) {
        this.metaDB = metaDB;
        asyncWriteMetaDB();
    }

    public String genSid(String project, LocalDateTime localDateTime) {
        String path = TimeUtils.genPathFromLocalDateTime(localDateTime);
        if (sidPathMap.get(project) != null && sidPathMap.get(project).equals(path)) {
            long sidSeed = this.sidSeedMap.get(project).incrementAndGet();
            return path + "/" + sidSeed;
        }

        if (sidPathMap.get(project) != null && !sidPathMap.get(project).equals(path)) {
            this.sidPathMap.put(project, path);
            this.sidSeedMap.put(project, new AtomicLong(0));
            long sidSeed = this.sidSeedMap.get(project).incrementAndGet();
            return path + "/" + sidSeed;
        }

        boolean read = syncReadMetaDB(project);
        if (!read) {
            this.sidPathMap.put(project, path);
            this.sidSeedMap.put(project, new AtomicLong(0));
            long sidSeed = this.sidSeedMap.get(project).incrementAndGet();
            return path + "/" + sidSeed;
        }

        return genSid(project, localDateTime);
    }

    private boolean syncReadMetaDB(String project) {
        try {
            String path = this.metaDB.getStringKV(project + "path");
            String seed = this.metaDB.getStringKV(project + "seed");

            if (path == null) {
                return false;
            }

            this.sidPathMap.put(project, path);
            this.sidSeedMap.put(project, new AtomicLong(Long.parseLong(seed)));


        } catch (RocksDBOperateException e) {
            // pass
        }

        return true;
    }

    public void asyncWriteMetaDB() {
        MetaDBExecutors.RECORD_SEQUENCE.execute(() -> {
            dumpSidSeedMap();
            dumpSidPathMap();
            try {
                Thread.sleep(500);
            } catch (Exception e) {
                logger.error("sequence id async write meta db sleep error", e);
            }
        });
    }

    private void dumpSidPathMap() {
        if (this.sidPathMap.isEmpty()) {
            return;
        }

        this.sidPathMap.forEach((k, v) -> {
            try {
                metaDB.writeStringKV(k + "path", v);
            } catch (Exception e) {
                // pass
            }
        });
    }

    private void dumpSidSeedMap() {
        if (this.sidSeedMap.isEmpty()) {
            return;
        }

        this.sidSeedMap.forEach((k, v) -> {
            try {
                metaDB.writeStringKV(k + "seed", v.longValue() + "");
            } catch (Exception e) {
                // pass
            }
        });
    }
}
