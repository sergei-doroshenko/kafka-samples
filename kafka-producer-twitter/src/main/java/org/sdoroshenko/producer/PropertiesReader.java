package org.sdoroshenko.producer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesReader {
    public static void main(String[] args) throws IOException {
        PropertiesReader reader = new PropertiesReader();
        Properties properties = reader.read();
        properties.forEach((k, v) -> System.out.println("Kye: " + k + ", \nValue: " + v + "\n"));
    }

    public Properties read() throws IOException {
        try (InputStream is = getClass().getResourceAsStream("/twitter-app.properties")) {
            Properties properties = new Properties();
            properties.load(is);
            return properties;
        }
    }
}
