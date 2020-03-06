package com.modb.test.coredb;

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

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {Application.class})
public class TestRocksDB {

    @Autowired
    private RocksDBTunnel rocksDBTunnel;

    public static final String stringKey = "GcTestKey";
    public static final String stringValue = "GcTestValue";

    @Test
    public void testWriteStringKV() {
        try {
            rocksDBTunnel.writeStringKV(stringKey, stringValue);
        } catch (RocksDBOperateException e) {
            Assert.fail();
        }
    }

    @Test
    public void testGetStringKV() {
        try {
            String value = rocksDBTunnel.getStringKV(stringKey);

            Assert.assertEquals(value, stringValue);
        } catch (RocksDBOperateException e) {
            Assert.fail();
        }
    }
}
