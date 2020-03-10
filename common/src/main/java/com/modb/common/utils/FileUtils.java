package com.modb.common.utils;

import org.apache.log4j.Logger;

import java.io.File;
import java.util.List;

public class FileUtils {

    private static final Logger logger = Logger.getLogger(FileUtils.class);

    public static void mkdirs(List<String> paths) {
        for (String path : paths) {
            mkdir(path);
        }
    }

    public static boolean mkdir(String path) {
        logger.info("mkdir " + path);

        File file = new File(path);
        if (file.exists()) {
            return true;
        }

        return file.mkdirs();
    }
}
