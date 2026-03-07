package br.furb.pedido.config;

import org.flywaydb.core.Flyway;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class HibernateConfig {

    private static SessionFactory sessionFactory;

    private static Properties loadHibernateProperties() throws IOException {
        Properties properties = new Properties();
        try (InputStream input = HibernateConfig.class.getClassLoader()
                .getResourceAsStream("hibernate.properties")) {
            if (input == null) {
                throw new IOException("Arquivo hibernate.properties não encontrado no classpath");
            }
            properties.load(input);
        }
        return properties;
    }

    public static void initFlyway() {
        try {
            Properties props = loadHibernateProperties();
            String url = props.getProperty("hibernate.connection.url");
            String username = props.getProperty("hibernate.connection.username");
            String password = props.getProperty("hibernate.connection.password", "");

            Flyway flyway = Flyway.configure()
                    .dataSource(url, username, password)
                    .load();

            flyway.migrate();
            System.out.println("Flyway migrations executed");
        } catch (IOException e) {
            System.err.println("Error loading Hibernate configuration: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void initSessionFactory() {
        try {
            Properties props = loadHibernateProperties();
            Configuration configuration = new Configuration();
            configuration.addProperties(props);
            sessionFactory = configuration.buildSessionFactory();
            System.out.println("Hibernate SessionFactory initialized");
        } catch (Exception e) {
            System.err.println("Error initializing SessionFactory: " + e.getMessage());
            e.printStackTrace();
            throw new ExceptionInInitializerError(e);
        }
    }

    public static SessionFactory getSessionFactory() {
        if (sessionFactory == null) {
            initSessionFactory();
        }
        return sessionFactory;
    }

    public static void closeSessionFactory() {
        if (sessionFactory != null && sessionFactory.isOpen()) {
            sessionFactory.close();
            System.out.println("SessionFactory closed");
        }
    }

    public static void init() {
        initFlyway();
        initSessionFactory();
    }
}

