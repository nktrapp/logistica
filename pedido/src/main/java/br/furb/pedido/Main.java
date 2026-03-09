package br.furb.pedido;

import br.furb.pedido.cluster.ClusterConfig;
import br.furb.pedido.cluster.ClusterNode;
import br.furb.pedido.cluster.ClusterRpcClient;
import br.furb.pedido.cluster.PagamentoMasterNotifier;
import br.furb.pedido.cluster.RingElectionManager;
import br.furb.pedido.config.HibernateConfig;
import br.furb.pedido.repository.PedidoRepository;
import br.furb.pedido.rpc.PedidoRpcServer;
import br.furb.pedido.service.PedidoService;
import br.furb.pedido.util.AppProperties;
import br.furb.rpc.pedido.HeartbeatResponse;

public class Main {

    public static void main(String[] args) {
        HibernateConfig.init();

        int selfId = Integer.parseInt(AppProperties.get("pedido.instance.id", "1"));
        String nodes = AppProperties.get("pedido.cluster.nodes", "1@localhost:8081,2@localhost:8083,3@localhost:8084");
        String pagamentoHost = AppProperties.get("pagamento.rpc.host", "localhost");
        int pagamentoPort = Integer.parseInt(AppProperties.get("pagamento.rpc.port", "8082"));

        ClusterConfig config = new ClusterConfig(selfId, nodes);

        try (ClusterRpcClient clusterRpcClient = new ClusterRpcClient(config);
             PagamentoMasterNotifier pagamentoNotifier = new PagamentoMasterNotifier(pagamentoHost, pagamentoPort)) {

            int initialLeaderId = discoverInitialLeaderId(config, clusterRpcClient);
            RingElectionManager electionManager = new RingElectionManager(config, clusterRpcClient, pagamentoNotifier, initialLeaderId);

            try (electionManager) {
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
            }
        } catch (Exception e) {
            System.err.println("Erro no bootstrap do cluster pedido: " + e.getMessage());
            e.printStackTrace();
        } finally {
            HibernateConfig.closeSessionFactory();
        }
    }

    private static int discoverInitialLeaderId(ClusterConfig config, ClusterRpcClient clusterRpcClient) {
        for (ClusterNode node : config.ring()) {
            if (node.nodeId() == config.selfNodeId()) {
                continue;
            }

            HeartbeatResponse response = clusterRpcClient.heartbeatInfo(node.nodeId(), config.selfNodeId());
            if (response != null && response.getOk()) {
                int discoveredLeader = response.getLeaderId() > 0 ? response.getLeaderId() : response.getNodeId();
                System.out.println("[bootstrap] node=" + config.selfNodeId()
                        + " detectou cluster ativo. leaderAtual=" + discoveredLeader
                        + " viaNode=" + node.nodeId());
                return discoveredLeader;
            }
        }

        System.out.println("[bootstrap] node=" + config.selfNodeId()
                + " nao encontrou nodes ativos. assumindo MASTER inicial por ordem de subida.");
        return config.selfNodeId();
    }
}
