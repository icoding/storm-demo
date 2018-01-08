package com.jndemo.storm.demo.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;


/**
 * 从classes目录或jar包装载.properties文件，并提供获取属性值的方法。
 * （适应于在spark、storm等分布式环境执行的把配置文件打进jar包的程序）
 * <p/>
 * Created by HuQingmiao on 2016-6-20.
 */
public class ClassesConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(ClassesConfigLoader.class);

    // 存放配置信息的文件
    private static final String CONFIG_FILE = "/config.properties";

    private static Properties configProps = new Properties();

    static {
        try {
            loadConfig();
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public static String getProperty(String key) {
        String value = configProps.getProperty(key);
        if (value != null) {
            return value.trim();
        }
        return value;
    }

    public static Properties getProperties() {
        Properties bakProps = new Properties();
        bakProps.putAll(configProps);
        return bakProps;
    }

    private static void loadConfig() throws Exception {
        log.info("Load {} begin... ", CONFIG_FILE);
        InputStream is = null;
        BufferedReader reader = null;
        try {
            //is = ClassesConfigLoader.class.getClassLoader().getResourceAsStream(CONFIG_FILE);

            // 以下写法，即使将属性文件打到jar包中，引用该jar包的客户端仍可正常载入属性文件
            is = ClassesConfigLoader.class.getResourceAsStream(CONFIG_FILE);
            reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            configProps.load(reader);

        } catch (Exception e) {
            log.error("Load {} fail! ", CONFIG_FILE, e);
            throw e;
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
                if (is != null) {
                    is.close();
                }
            } catch (IOException e) {
                log.error("", e);
            }
        }
        log.info("Load {} success! ", CONFIG_FILE);
    }


    public static void main(String[] args) {

        String host = ClassesConfigLoader.getProperty("jdbcUrl");
        System.out.println("host: " + host);

        String port = ClassesConfigLoader.getProperty("redis.port");
        System.out.println("port: " + port);
    }
}
