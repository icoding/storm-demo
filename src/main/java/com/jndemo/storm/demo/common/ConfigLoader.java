package com.jndemo.storm.demo.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;


/**
 * 装载properties文件，并提供取值方法
 * <p/>
 * Created by HuQingmiao on 2016-8-13.
 */
public class ConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(ConfigLoader.class);

    // 存放配置目录的文件
    private static final String CONFDIR_FILE = "/confdir.properties";

    // 存放配置信息的文件
    private static final String CONFIG_FILE = "/config.properties";

    private static Properties configProps = new Properties();


    // 配置目录的路径
    public static final String CONF_DIR_PATH = loadConfDirPath();

    static {
        try {
            log.info("CONF_DIR_PATH: " + CONF_DIR_PATH);
            loadConfig(new File(CONF_DIR_PATH, CONFIG_FILE));

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

    private static String loadConfDirPath() {
        log.info("Load {} begin... ", CONFDIR_FILE);
        Properties properties = new Properties();
        InputStream is = null;
        BufferedReader reader = null;
        try {
            //is = ConfigLoader.class.getClassLoader().getResourceAsStream(CONFDIR_FILE);

            // 以下写法，即使将属性文件打到jar包中，引用该jar包的客户端仍可正常载入属性文件
            is = ConfigLoader.class.getResourceAsStream(CONFDIR_FILE);
            reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            configProps.load(reader);

            properties.load(reader);
        } catch (Exception e) {
            log.error("Load {} fail! ", CONFDIR_FILE, e);
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
        log.info("Load {} success! ", CONFDIR_FILE);

        String confDirPath = (String) properties.get("conf.dir");
        log.info("confDirPath: " + confDirPath);
        properties.clear();
        return confDirPath;
    }

    private static void loadConfig(File file) throws Exception {
        log.info("Load {} begin... ", file.getCanonicalPath());
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        try {
            fis = new FileInputStream(file.getCanonicalPath());
            isr = new InputStreamReader(fis, "UTF-8");
            br = new BufferedReader(isr);

            configProps.load(br);

        } catch (Exception e) {
            log.error("Load {} fail! ", file.getName(), e);
            throw e;
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (isr != null) {
                    isr.close();
                }
                if (fis != null) {
                    fis.close();
                }
            } catch (IOException e) {
                log.error("", e);
            }
        }
        log.info("Load {} success! ", file.getName());
    }


    public static void main(String[] args) {

        String host = ClassesConfigLoader.getProperty("jdbcDriver");
        System.out.println("jdbcUrl: " + host);

        String port = ClassesConfigLoader.getProperty("redis.port");
        System.out.println("port: " + port);
    }
}

