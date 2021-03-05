package om;

import Utils.PropertiesUtils;

import java.io.IOException;
import java.util.Properties;

/**
 * @author xiazhonghai
 * @date 2021/3/3 16:40
 * @description:启动类
 *
 */
public class Main {
    /**获取配置文件**/
    static Properties properties;

    static {
        try {
            properties = PropertiesUtils.readProperties();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        /**获取配置文件启动类**/
        String mainClassStr =(String) properties.get("mainClass");
        Class<?> mainClass = Class.forName(mainClassStr);
        Thread t = (Thread)mainClass.newInstance();
        t.run();

    }
}
