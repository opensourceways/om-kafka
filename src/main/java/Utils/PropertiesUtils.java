package Utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author xiazhonghai
 * @date 2021/3/3 16:43
 * @description:
 */
public class PropertiesUtils {
   private static Properties properties=null;
    public static Properties readProperties() throws IOException {
        synchronized (Properties.class){
            if(properties==null){
                properties=new Properties();
                InputStream resourceAsStream = new FileInputStream(System.getenv("CONFIG_PATH"));
                properties.load(resourceAsStream);
            }
        }
        return properties;
    }
    private PropertiesUtils(){}
}
