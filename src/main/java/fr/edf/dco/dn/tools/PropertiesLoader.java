package fr.edf.dco.dn.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by AM376E1N on 24/05/2016.
 */
public class PropertiesLoader {


    private Properties dbProp;
    private Properties imagesProp;

    public PropertiesLoader(String dbPropertiesFileName, String imagesPropertiesFileName) throws IOException {
        this.dbProp = (dbPropertiesFileName != null) ? this.loadPropertiesFile(dbPropertiesFileName) : null;
        this.imagesProp = (imagesPropertiesFileName != null) ? this.loadPropertiesFile(imagesPropertiesFileName) : null;
    }

    public Properties getDbProp() {
        return this.dbProp;
    }

    public Properties getImagesProp() {
        return this.imagesProp;
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
