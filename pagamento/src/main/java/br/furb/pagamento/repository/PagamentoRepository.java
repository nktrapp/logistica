package br.furb.pagamento.repository;

import br.furb.pagamento.domain.Pagamento;
import org.hibernate.Session;

public class PagamentoRepository {

    public void save(Session session, Pagamento pagamento) {
        session.persist(pagamento);
    }
}

