package com.turing.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * @descri PropertiesUtils
 *
 * @author lj.michale
 * @date 2023-03-07
 */
public class PropertiesUtils {

    private static final Logger logger = LoggerFactory.getLogger(PropertiesUtils.class);

    /**
     * @descri 获取某一个properties文件的properties对象，以方便或得文件里面的值
     *
     * @param filePath：properties文件所在路径
     * @return Properties对象
     */
    public static Properties getProperties(String filePath) {
        final Properties properties = new Properties();
        try {
            properties.load(PropertiesUtils.class.getClassLoader().getResourceAsStream(filePath));
        } catch (IOException e) {
            logger.error("获取某一个properties文件的properties对象失败", e.getMessage());
        }

        return properties;
    }

}
