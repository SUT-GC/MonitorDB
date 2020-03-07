package com.modb.core.executors;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MetaDBExecutors {
    public static ExecutorService RECORD_SEQUENCE = Executors.newFixedThreadPool(1);
}
