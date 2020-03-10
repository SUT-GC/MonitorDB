package com.modb.test.coredb;

import com.modb.core.db.MonitorDB;
import com.modb.core.exception.BaseException;
import com.modb.core.exception.rocksdb.RocksDBOperateException;
import com.modb.core.rocksdb.RocksDBTunnel;
import com.modb.server.Application;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {Application.class})
public class TestRocksMonitorDB {

    @Autowired
    private MonitorDB monitorDB;

    public static final String stringKey = "GcTestKey";
    public static final String stringValue = "GcTestValue";
    public static final String project = "GcTest";

    @Test
    public void testWriteStringKV() {
        try {
            Map<String, String> tags = new HashMap<>();
            tags.put("tagkey1", "tagvalue1");
            tags.put("tagkey2", "tagvalue2");

            Map<String, String> fields = new HashMap<>();
            fields.put("fieldKey1", "fieldValue1");
            fields.put("fieldKey2", "fieldValue2");

            monitorDB.write(project, tags, fields, LocalDateTime.now());

            Thread.sleep(1000);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testReadTag() throws RocksDBOperateException {
        Map<String, String> v = monitorDB.readTag(project, "/2020/3/10/22/6", LocalDateTime.now());
        System.out.println(v);
    }
}
