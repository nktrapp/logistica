package br.furb.pagamento;

import br.furb.pagamento.config.HibernateConfig;

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

