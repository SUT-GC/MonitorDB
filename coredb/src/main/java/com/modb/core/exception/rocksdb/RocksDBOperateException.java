package com.modb.core.exception.rocksdb;

import com.modb.core.exception.BaseException;
import com.modb.core.exception.BaseRuntimeException;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class RocksDBOperateException extends BaseException {
    private static final String WRITE_EXCEPTION_CODE = "WRITE_EXCEPTION";
    private static final String READ_EXCEPTION_CODE = "READ_EXCEPTION";

    public RocksDBOperateException(String message, String code, Throwable throwable) {
        super(message, code, throwable);
    }

    public static RocksDBOperateException ofWriteException(String message, Throwable throwable) {

        return new RocksDBOperateException(message, WRITE_EXCEPTION_CODE, throwable);
    }

    public static RocksDBOperateException ofWriteException(Throwable throwable) {

        return ofWriteException("write data error", throwable);
    }

    public static RocksDBOperateException ofReadException(String message, Throwable throwable) {

        return new RocksDBOperateException(message, READ_EXCEPTION_CODE, throwable);
    }

    public static RocksDBOperateException ofReadException(Throwable throwable) {

        return ofWriteException("read data error", throwable);
    }
}
