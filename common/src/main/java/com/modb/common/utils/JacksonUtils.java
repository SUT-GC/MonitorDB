package com.modb.common.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.log4j.Logger;

public class JacksonUtils {
    private static final Logger logger = Logger.getLogger(JacksonUtils.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        // 忽略反序列化中json存在但是java不存在的字段
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // 在序列化时日期格式默认为 yyyy-MM-dd'T'HH:mm:ss
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    public static String toJsonString(Object obj) throws JsonProcessingException {

        return objectMapper.writeValueAsString(obj);
    }

    public static String toJsonStringCoverE(Object obj) {
        try {

            return toJsonString(obj);
        } catch (Exception e) {
            logger.error("write object to json str error, default return null", e);
        }

        return null;
    }

    public static <T> T fromJsonString(String jsonStr, Class<T> clazz) throws JsonProcessingException {

        return objectMapper.readValue(jsonStr, clazz);
    }

    public static <T> T fromJsonString(String jsonStr, TypeReference<T> typeReference) throws JsonProcessingException {

        return objectMapper.readValue(jsonStr, typeReference);
    }

    public static <T> T fromJsonStringCoverE(String jsonStr, Class<T> clazz) {
        try {
            return fromJsonString(jsonStr, clazz);
        } catch (Exception e) {
            logger.error("read json string to object error, default return null", e);
        }

        return null;
    }

    public static <T> T fromJsonStringCoverE(String jsonStr, TypeReference<T> typeReference) {
        try {
            return fromJsonString(jsonStr, typeReference);
        } catch (Exception e) {
            logger.error("read json string to object error, default return null", e);
        }

        return null;
    }
}
