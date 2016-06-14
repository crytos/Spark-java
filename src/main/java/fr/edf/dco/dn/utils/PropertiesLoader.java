package fr.edf.dco.dn.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by AM376E1N on 24/05/2016.
 */
public class PropertiesLoader {


    private Properties dbProp;

    public PropertiesLoader(String dbPropertiesFileName) throws IOException {
        this.dbProp = (dbPropertiesFileName != null) ? this.loadPropertiesFile(dbPropertiesFileName) : null;
    }

    public Properties getDbProp() {
        return this.dbProp;
    }


    public Properties loadPropertiesFile(String propFileName) throws IOException {

        Properties properties = new Properties();
        InputStream inputStream = getClass().getResourceAsStream(propFileName);

        if (inputStream != null) {
            properties.load(inputStream);
        } else {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }
        return properties;
    }
}
