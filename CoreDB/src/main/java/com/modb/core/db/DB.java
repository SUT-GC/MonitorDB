package com.modb.core.db;

import java.io.*;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DB {

    public static final String ROOT_PATH = "/Users/gc/modb";
    private static Map<String, String> metaDate = new HashMap<>();
    private static Map<String, long[]> sequenceSeekData = new HashMap<>();

    static {
        loadMetaData();
        loadSequenceSeekData();
    }

    private static void loadSequenceSeekData() {

    }

    private static void loadMetaData() {
        File mateDataFile = new File(ROOT_PATH);

        if (!mateDataFile.exists() || !mateDataFile.isDirectory()) {
            return;
        }

        String[] childFileNames = mateDataFile.list();
//        List<String> allProjects =
    }

    public static void setTagsSeek(String filePath, long seek) {

        metaDate.put(filePath + "tags.data", String.valueOf(seek));
    }

    public static void setFieldsSeek(String filePath, long seek) {

        metaDate.put(filePath + "fields.data", String.valueOf(seek));
    }

    public static long getTagsSeek(String filePath) {
        String tagSeekStr = metaDate.getOrDefault(filePath + "tags.data", "0");

        return Long.parseLong(tagSeekStr);
    }

    public static long getFieldsSeek(String filePath) {
        String fieldSeekStr = metaDate.getOrDefault(filePath + "fields.data", "0");

        return Long.parseLong(fieldSeekStr);
    }

    public static void write(String project, LocalDateTime dateTime, String sequenceId, Map<String, String> tags, Map<String, String> fields) throws Exception {
        int year = dateTime.toLocalDate().getYear();
        int month = dateTime.toLocalDate().getMonthValue();
        int day = dateTime.toLocalDate().getDayOfMonth();
        int hours = dateTime.toLocalTime().getHour();
        String filePath = String.format("%s/%s/%s/%s/%s", project, year, month, day, hours);

        String tagFilePath = String.format("%s/tags.data", filePath);
        String fieldFilePath = String.format("%s/fields.data", filePath);
        String seekFilePath = String.format("%s/seek.data", filePath);

        File fileDir = new File(filePath);
        if (!fileDir.exists()) {
            boolean r = fileDir.mkdirs();
            if (r) {
                System.out.println(fileDir + " create success...");
            }
        }

        File tagFile = new File(tagFilePath);
        File fieldFile = new File(fieldFilePath);
        File seekFile = new File(seekFilePath);
        if (!tagFile.exists()) {
            boolean r = tagFile.createNewFile();
            if (r) {
                System.out.println(tagFilePath + " create success...");
            }
        }
        if (!fieldFile.exists()) {
            boolean r = fieldFile.createNewFile();
            if (r) {
                System.out.println(fieldFilePath + " create success...");
            }
        }
        if (!seekFile.exists()) {
            boolean r = seekFile.createNewFile();
            if (r) {
                System.out.println(seekFilePath + " create success...");
            }
        }

        RandomAccessFile tagFileAccessFile = new RandomAccessFile(tagFile, "rw");
        RandomAccessFile fieldFileAccessFile = new RandomAccessFile(fieldFile, "rw");
        RandomAccessFile seekFileAccessFile = new RandomAccessFile(seekFile, "rw");


        long tagSeek = getTagsSeek(tagFilePath);
        long fieldSeek = getFieldsSeek(fieldFilePath);

        tagFileAccessFile.seek(tagSeek);
        fieldFileAccessFile.seek(fieldSeek);

        StringBuffer tagSB = new StringBuffer();
        tagSB.append(sequenceId);
        tagSB.append("|");
        fields.forEach((k, v) -> {
            tagSB.append(String.format("%s:%s;", k, v));
        });

        StringBuffer fieldSB = new StringBuffer();
        fieldSB.append(sequenceId);
        fieldSB.append("|");
        fields.forEach((k, v) -> {
            fieldSB.append(String.format("%s:%s;", k, v));
        });

        byte[] tagBytes = tagSB.toString().getBytes();
        long tagLength = tagBytes.length;

        byte[] fieldBytes = fieldSB.toString().getBytes();
        long fieldLength = fieldBytes.length;

        tagFileAccessFile.write(tagBytes);
        fieldFileAccessFile.write(fieldBytes);

        setTagsSeek(filePath, tagSeek + tagLength);
        setFieldsSeek(filePath, fieldSeek + fieldLength);
        setSequenceSeek(sequenceId, tagSeek, tagLength, fieldSeek, fieldLength);
    }

    private static void setSequenceSeek(String sequenceId, long tagSeek, long tagLength, long fieldSeek, long fieldLength) {

        sequenceSeekData.put(sequenceId, new long[]{tagSeek, tagLength, fieldSeek, fieldLength});
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> tags = new HashMap<>();
        tags.put("gcTag1", "hahaTag1");
        tags.put("gcTag2", "hahaTag2");
        tags.put("gcTag3", "hahaTag3");

        Map<String, String> fields = new HashMap<>();
        fields.put("gcField1", "hahaField1");
        fields.put("gcField2", "hahaField2");
        fields.put("gcField3", "hahaField3");

        write("gcTest", LocalDateTime.now(), System.currentTimeMillis() + "", tags, fields);
    }
}
