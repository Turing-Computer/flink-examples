package com.turing.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * @descri json处理工具类
 *
 * @author lj.michale
 * @date 2023-03-06
 */
public class PipelineJsonUtils {

    /**
     * 单例objectMapper，提高性能
     */
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 单例 饿汉式
     */
    private static final PipelineJsonUtils INSTANCE = new PipelineJsonUtils();

    private PipelineJsonUtils() { }

    /**
     * 单例模式，暴露一个单例的工具类实例获取
     */
    public static PipelineJsonUtils getInstance() {
        return INSTANCE;
    }

    /**
     * 通过key路径取到最后一级key对应的jsonNode
     *
     * @param jsonStr 原始的json字符串
     * @param keyPath xx.xxx.xxx格式的key路径
     * @return
     */
    public JsonNode getValueByKeyPath(String jsonStr, String keyPath) throws IOException {
        JsonNode jsonNode = objectMapper.readTree(jsonStr);
        String[] paths = keyPath.split("\\.");

        // 遍历key路径，直到最后一层的key
        JsonNode currentNode = jsonNode;
        for (String key : paths) {
            currentNode = currentNode.get(key);
            if (currentNode == null) {
                return null;
            }
        }

        return currentNode;
    }

    /**
     * 通过key路径取到最后一级key对应的value值
     *
     * @param jsonStr 原始json字符串
     * @param keyPath xx.xxx.xxx格式的key路径
     * @param cls     值的对象类型
     */
    public <T> T getValueByKeyPath(String jsonStr,
                                   String keyPath, Class<T> cls) throws IOException {
        JsonNode jsonNode = this.getValueByKeyPath(jsonStr, keyPath);
        if (jsonNode == null) {
            return null;
        }

        return objectMapper.treeToValue(jsonNode, cls);
    }

}