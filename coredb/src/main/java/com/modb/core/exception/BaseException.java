package com.modb.core.exception;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class BaseException extends Exception {
    private String code;
    private String message;
    private Throwable throwable;

    public BaseException(String message, String code, Throwable throwable) {
        super(message, throwable);
        this.code = code;
        this.message = message;
    }
}
