package br.furb.pedido;

import br.furb.pedido.cluster.ClusterConfig;
import br.furb.pedido.cluster.ClusterRpcClient;
import br.furb.pedido.cluster.PagamentoMasterNotifier;
import br.furb.pedido.cluster.RingElectionManager;
import br.furb.pedido.config.HibernateConfig;
import br.furb.pedido.repository.PedidoRepository;
import br.furb.pedido.rpc.PedidoRpcServer;
import br.furb.pedido.service.PedidoService;
import br.furb.pedido.util.AppProperties;

public class Main {

    public static void main(String[] args) {
        HibernateConfig.init();

        int selfId = Integer.parseInt(AppProperties.get("pedido.instance.id", "1"));
        String nodes = AppProperties.get("pedido.cluster.nodes", "1@localhost:8081,2@localhost:8083,3@localhost:8084");
        int initialLeaderId = Integer.parseInt(AppProperties.get("pedido.cluster.initial.master.id", "1"));
        String pagamentoHost = AppProperties.get("pagamento.rpc.host", "localhost");
        int pagamentoPort = Integer.parseInt(AppProperties.get("pagamento.rpc.port", "8082"));

        ClusterConfig config = new ClusterConfig(selfId, nodes);

        try (ClusterRpcClient clusterRpcClient = new ClusterRpcClient(config);
             PagamentoMasterNotifier pagamentoNotifier = new PagamentoMasterNotifier(pagamentoHost, pagamentoPort);
             RingElectionManager electionManager = new RingElectionManager(config, clusterRpcClient, pagamentoNotifier, initialLeaderId)) {

            PedidoService pedidoService = new PedidoService(new PedidoRepository(), electionManager);
            PedidoRpcServer rpcServer = new PedidoRpcServer(pedidoService, electionManager, config.selfNode().port());

            try {
                rpcServer.start();
                electionManager.start();
                System.out.println("Servico gRPC de pedido no ar em " + config.selfNode().target() + " (node=" + selfId + ")");
                rpcServer.blockUntilShutdown();
            } catch (Exception e) {
                System.err.println("Erro ao iniciar servico de pedido: " + e.getMessage());
                e.printStackTrace();
            } finally {
                rpcServer.stop();
            }
        } catch (Exception e) {
            System.err.println("Erro no bootstrap do cluster pedido: " + e.getMessage());
            e.printStackTrace();
        } finally {
            HibernateConfig.closeSessionFactory();
        }
    }
}
