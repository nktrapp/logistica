package br.furb.pagamento.service;

import br.furb.pagamento.config.HibernateConfig;
import br.furb.pagamento.domain.Pagamento;
import br.furb.pagamento.domain.PagamentoStatus;
import br.furb.pagamento.repository.PagamentoRepository;
import br.furb.pagamento.rpc.MasterUnavailableException;
import br.furb.pagamento.rpc.PedidoRpcClient;
import org.hibernate.Session;
import org.hibernate.Transaction;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class PagamentoService {

    private final PagamentoRepository pagamentoRepository;
    private final PedidoRpcClient pedidoRpcClient;
    private final Set<UUID> pendencias = ConcurrentHashMap.newKeySet();

    public PagamentoService(PagamentoRepository pagamentoRepository, PedidoRpcClient pedidoRpcClient) {
        this.pagamentoRepository = pagamentoRepository;
        this.pedidoRpcClient = pedidoRpcClient;
    }

    public Pagamento processarPagamento(UUID pedidoId) {
        System.out.println("[pagamento] iniciando processamento. pedidoId=" + pedidoId);
        Pagamento pagamento = new Pagamento(UUID.randomUUID(), pedidoId, PagamentoStatus.APROVADO);

        try (Session session = HibernateConfig.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            pagamentoRepository.save(session, pagamento);
            tx.commit();
            System.out.println("[pagamento] persistido com sucesso. pagamentoId=" + pagamento.getId()
                    + " pedidoId=" + pedidoId + " status=" + pagamento.getStatus());
        } catch (RuntimeException e) {
            System.err.println("[pagamento][erro] falha ao persistir pagamento. pedidoId=" + pedidoId
                    + " motivo=" + e.getMessage());
            throw e;
        }

        try {
            pedidoRpcClient.marcarPedidoComoPago(pedidoId);
            pendencias.remove(pedidoId);
            System.out.println("[pagamento] pedido atualizado para PAGO no servico de pedido. pedidoId=" + pedidoId);
        } catch (MasterUnavailableException e) {
            pendencias.add(pedidoId);
            System.err.println("[pagamento][warn] master de pedido indisponivel. pedido marcado como pendente. pedidoId="
                    + pedidoId + " pendencias=" + pendencias.size() + " motivo=" + e.getMessage());
        }

        return pagamento;
    }

    public int atualizarMasterPedido(int newMasterId) {
        System.out.println("[pagamento] recebida atualizacao de master de pedido. novoMaster=" + newMasterId);
        pedidoRpcClient.atualizarMaster(newMasterId);
        int reenviados = reenviarPendencias();
        System.out.println("[pagamento] atualizacao de master concluida. novoMaster=" + newMasterId
                + " pendenciasReenviadas=" + reenviados + " pendenciasRestantes=" + pendencias.size());
        return reenviados;
    }

    public int reenviarPendencias() {
        int sucesso = 0;
        UUID[] snapshot = pendencias.toArray(UUID[]::new);
        if (snapshot.length > 0) {
            System.out.println("[pagamento] iniciando reenvio de pendencias. total=" + snapshot.length);
        }

        for (UUID pedidoId : snapshot) {
            try {
                pedidoRpcClient.marcarPedidoComoPago(pedidoId);
                pendencias.remove(pedidoId);
                sucesso++;
                System.out.println("[pagamento] pendencia reenviada com sucesso. pedidoId=" + pedidoId);
            } catch (MasterUnavailableException e) {
                System.err.println("[pagamento][warn] falha ao reenviar pendencia. pedidoId=" + pedidoId
                        + " motivo=" + e.getMessage());
            }
        }
        return sucesso;
    }
}
