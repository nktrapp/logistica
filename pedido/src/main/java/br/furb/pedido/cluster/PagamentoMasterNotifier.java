package br.furb.pedido.cluster;

import br.furb.rpc.pagamento.AtualizarMasterPedidoRequest;
import br.furb.rpc.pagamento.PagamentoRpcServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

public class PagamentoMasterNotifier implements AutoCloseable {

    private final ManagedChannel channel;
    private final PagamentoRpcServiceGrpc.PagamentoRpcServiceBlockingStub stub;

    public PagamentoMasterNotifier(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.stub = PagamentoRpcServiceGrpc.newBlockingStub(channel);
    }

    public void notifyNewMaster(int leaderId) {
        stub.withDeadlineAfter(2, TimeUnit.SECONDS).atualizarMasterPedido(
                AtualizarMasterPedidoRequest.newBuilder().setMasterNodeId(leaderId).build()
        );
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(2, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

