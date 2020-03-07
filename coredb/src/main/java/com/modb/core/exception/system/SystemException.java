package com.modb.core.exception.system;

import com.modb.core.exception.BaseRuntimeException;
import com.modb.core.exception.rocksdb.RocksDBOperateException;

public class SystemException extends BaseRuntimeException {

    private static final String SYSTEM_ERROR = "SYSTEM_ERROR";

    public SystemException(String message, String code, Throwable throwable) {
        super(message, code, throwable);
    }

    public static SystemException of(String message, String code, Throwable throwable) {

        return new SystemException(message, code, throwable);
    }

    public static SystemException of(String message, Throwable throwable) {

        return of(message, SYSTEM_ERROR, throwable);
    }
}
