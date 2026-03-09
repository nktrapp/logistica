package br.furb.pedido.service;

import br.furb.pedido.cluster.RingElectionManager;
import br.furb.pedido.config.HibernateConfig;
import br.furb.pedido.domain.Pedido;
import br.furb.pedido.domain.PedidoStatus;
import br.furb.pedido.repository.PedidoRepository;
import org.hibernate.Session;
import org.hibernate.Transaction;

import java.util.UUID;

public class PedidoService {

    private final PedidoRepository pedidoRepository;
    private final RingElectionManager electionManager;

    public PedidoService(PedidoRepository pedidoRepository, RingElectionManager electionManager) {
        this.pedidoRepository = pedidoRepository;
        this.electionManager = electionManager;
    }

    public Pedido criarPedido() {
        UUID id = UUID.randomUUID();
        Pedido pedido = new Pedido(id, PedidoStatus.CRIADO);

        try (Session session = HibernateConfig.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            pedidoRepository.save(session, pedido);
            tx.commit();
            return pedido;
        }
    }

    public boolean atualizarStatus(UUID pedidoId, PedidoStatus status) {
        if (!electionManager.isSelfLeader()) {
            System.out.println("[write-blocked] node=" + electionManager.selfNodeId()
                    + " nao e master. leaderAtual=" + electionManager.currentLeaderId()
                    + " pedidoId=" + pedidoId
                    + " statusSolicitado=" + status);
            throw new IllegalStateException("Somente o master pode atualizar pedidos");
        }

        try (Session session = HibernateConfig.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            var pedidoOpt = pedidoRepository.findById(session, pedidoId);
            if (pedidoOpt.isEmpty()) {
                tx.commit();
                return false;
            }

            Pedido pedido = pedidoOpt.get();
            pedido.setStatus(status);
            session.merge(pedido);
            tx.commit();
            return true;
        }
    }

    public int currentLeaderId() {
        return electionManager.currentLeaderId();
    }
}
