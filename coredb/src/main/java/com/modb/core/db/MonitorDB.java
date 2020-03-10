package com.modb.core.db;


import com.modb.core.exception.rocksdb.RocksDBOperateException;
import com.modb.core.meta.SequenceIdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class MonitorDB {

    private static final Logger logger = LoggerFactory.getLogger(MonitorDB.class);

    @Value("${monitordb.path}")
    private String rootPath;

    @Autowired
    private SequenceIdGenerator sequenceIdGenerator;

    private Map<String, Bunch> projectMap = new ConcurrentHashMap<>();

    public void write(String project, Map<String, String> tags, Map<String, String> fields, LocalDateTime localDateTime) throws RocksDBOperateException {
        Bunch bunch = getOrInitBunch(projectMap, project, localDateTime);
        String sid = sequenceIdGenerator.genSid(project, localDateTime);

        logger.info("read sid :" + sid);

        bunch.write(sid, tags, fields);
    }

    public Map<String, String> readTag(String project, String sid, LocalDateTime localDateTime) throws RocksDBOperateException {
        Bunch bunch = getOrInitBunch(projectMap, project, localDateTime);

        return bunch.readTags(sid);
    }

    private Bunch getOrInitBunch(Map<String, Bunch> projectMap, String project, LocalDateTime localDateTime) {
        if (this.projectMap.get(project) == null) {
            Bunch bunch = new Bunch(this.rootPath, project, localDateTime);
            this.projectMap.put(project, bunch);
            return bunch;
        }

        return this.projectMap.get(project);
    }
}
