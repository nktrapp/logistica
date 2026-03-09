package br.furb.pedido.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class AppProperties {

    private static final Properties PROPERTIES = load();

    private AppProperties() {
    }

    private static Properties load() {
        Properties properties = new Properties();
        try (InputStream input = AppProperties.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException e) {
            throw new RuntimeException("Falha ao carregar application.properties", e);
        }
        return properties;
    }

    public static String get(String key, String defaultValue) {
        String systemValue = System.getProperty(key);
        if (systemValue != null && !systemValue.isBlank()) {
            return systemValue;
        }
        return PROPERTIES.getProperty(key, defaultValue);
    }
}
