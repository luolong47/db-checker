package io.github.luolong47.dbchecker.util;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 原子类型JSON适配器
 * 用于序列化和反序列化AtomicInteger、AtomicLong和AtomicReference<Double>等原子类型
 */
public class AtomicTypeAdapter {

    /**
     * 将原子类型对象转换为JSON可序列化的对象
     * 
     * @param obj 原子类型对象
     * @return JSON可序列化的对象
     */
    public static Object toSerializable(Object obj) {
        if (obj instanceof AtomicInteger) {
            return ((AtomicInteger) obj).get();
        } else if (obj instanceof AtomicLong) {
            return ((AtomicLong) obj).get();
        } else if (obj instanceof AtomicReference) {
            return ((AtomicReference<?>) obj).get();
        } else if (obj instanceof Map) {
            JSONObject jsonObject = new JSONObject();
            ((Map<?, ?>) obj).forEach((key, value) -> 
                jsonObject.put(key.toString(), toSerializable(value)));
            return jsonObject;
        } else if (obj instanceof Iterable) {
            JSONArray jsonArray = new JSONArray();
            ((Iterable<?>) obj).forEach(item -> 
                jsonArray.add(toSerializable(item)));
            return jsonArray;
        }
        return obj;
    }
    
    /**
     * 从JSON反序列化为原子类型对象
     * 
     * @param obj JSON数据
     * @param type 原子类型的Class对象
     * @return 原子类型对象
     */
    @SuppressWarnings("unchecked")
    public static <T> T fromJSON(Object obj, Class<T> type) {
        if (type == AtomicInteger.class) {
            AtomicInteger result = new AtomicInteger();
            if (obj instanceof Number) {
                result.set(((Number) obj).intValue());
            }
            return (T) result;
        } else if (type == AtomicLong.class) {
            AtomicLong result = new AtomicLong();
            if (obj instanceof Number) {
                result.set(((Number) obj).longValue());
            }
            return (T) result;
        } else if (type == AtomicReference.class) {
            AtomicReference<Double> result = new AtomicReference<>(0.0);
            if (obj instanceof Number) {
                result.set(((Number) obj).doubleValue());
            }
            return (T) result;
        } else if (type == ConcurrentHashMap.class && obj instanceof JSONObject) {
            ConcurrentHashMap<String, Object> result = new ConcurrentHashMap<>();
            JSONObject jsonObject = (JSONObject) obj;
            jsonObject.forEach((key, value) -> {
                if (value instanceof JSONObject) {
                    result.put(key, fromJSON(value, ConcurrentHashMap.class));
                } else {
                    result.put(key, value);
                }
            });
            return (T) result;
        }
        return null;
    }
} 