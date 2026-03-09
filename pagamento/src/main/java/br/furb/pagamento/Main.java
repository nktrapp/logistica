package br.furb.pagamento;

import br.furb.pagamento.config.HibernateConfig;
import br.furb.pagamento.repository.PagamentoRepository;
import br.furb.pagamento.rpc.PagamentoRpcServer;
import br.furb.pagamento.rpc.PedidoRpcClient;
import br.furb.pagamento.service.PagamentoService;
import br.furb.pagamento.util.AppProperties;

public class Main {

    public static void main(String[] args) {
        HibernateConfig.init();

        String pedidoClusterNodes = AppProperties.get("pedido.cluster.nodes", "1@localhost:8081,2@localhost:8083,3@localhost:8084");
        int pedidoMasterInicial = Integer.parseInt(AppProperties.get("pedido.cluster.initial.master.id", "1"));
        int pagamentoPort = Integer.parseInt(AppProperties.get("pagamento.rpc.port", "8082"));

        PagamentoRpcServer pagamentoRpcServer = null;
        try (PedidoRpcClient pedidoRpcClient = new PedidoRpcClient(pedidoClusterNodes, pedidoMasterInicial)) {
            PagamentoService pagamentoService = new PagamentoService(new PagamentoRepository(), pedidoRpcClient);
            pagamentoRpcServer = new PagamentoRpcServer(pagamentoService, pagamentoPort);
            pagamentoRpcServer.start();
            System.out.println("Servico gRPC de pagamento no ar em localhost:" + pagamentoPort + " (HTTP/2)");
            pagamentoRpcServer.blockUntilShutdown();
        } catch (Exception e) {
            System.err.println("Erro ao iniciar servico de pagamento: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (pagamentoRpcServer != null) {
                pagamentoRpcServer.stop();
            }
            HibernateConfig.closeSessionFactory();
        }
    }
}
