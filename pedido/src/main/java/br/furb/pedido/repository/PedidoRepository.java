package br.furb.pedido.repository;

import br.furb.pedido.domain.Pedido;
import org.hibernate.Session;

import java.util.Optional;
import java.util.UUID;

public class PedidoRepository {

    public void save(Session session, Pedido pedido) {
        session.persist(pedido);
    }

    public Optional<Pedido> findById(Session session, UUID id) {
        return Optional.ofNullable(session.get(Pedido.class, id));
    }
}

