package br.furb.pedido;

import br.furb.pedido.config.HibernateConfig;

public class Main {

    public static void main(String[] args) {
        try {
            HibernateConfig.init();
        } catch (Exception e) {
            System.err.println("Error initializing application: " + e.getMessage());
            e.printStackTrace();
        } finally {
            HibernateConfig.closeSessionFactory();
        }
    }
}

