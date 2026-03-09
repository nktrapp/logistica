package br.furb.pagamento.rpc;

import br.furb.rpc.pedido.MarcarPedidoComoPagoRequest;
import br.furb.rpc.pedido.PedidoResponse;
import br.furb.rpc.pedido.PedidoRpcServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PedidoRpcClient implements AutoCloseable {

    private final Map<Integer, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<Integer, PedidoRpcServiceGrpc.PedidoRpcServiceBlockingStub> stubs = new ConcurrentHashMap<>();
    private final AtomicInteger currentMasterId;

    public PedidoRpcClient(String nodesProperty, int initialMasterId) {
        List<NodeAddress> nodes = parseNodes(nodesProperty);
        for (NodeAddress node : nodes) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(node.host(), node.port())
                    .usePlaintext()
                    .build();
            channels.put(node.nodeId(), channel);
            stubs.put(node.nodeId(), PedidoRpcServiceGrpc.newBlockingStub(channel));
        }
        if (!stubs.containsKey(initialMasterId)) {
            throw new IllegalArgumentException("master inicial nao existe no cluster de pedidos");
        }
        this.currentMasterId = new AtomicInteger(initialMasterId);
    }

    public void atualizarMaster(int newMasterId) {
        if (!stubs.containsKey(newMasterId)) {
            throw new IllegalArgumentException("master recebido nao existe no cluster configurado: " + newMasterId);
        }
        int previous = currentMasterId.getAndSet(newMasterId);
        System.out.println("[pedido-rpc-client] master atualizado. old=" + previous + " new=" + newMasterId);
    }

    public int currentMasterId() {
        return currentMasterId.get();
    }

    public void marcarPedidoComoPago(UUID pedidoId) {
        int masterId = currentMasterId.get();
        PedidoRpcServiceGrpc.PedidoRpcServiceBlockingStub stub = stubs.get(masterId);
        if (stub == null) {
            throw new RuntimeException("Stub do master nao encontrada para node " + masterId);
        }

        System.out.println("[pedido-rpc-client] enviando MarcarPedidoComoPago. pedidoId=" + pedidoId + " master=" + masterId);
        MarcarPedidoComoPagoRequest request = MarcarPedidoComoPagoRequest.newBuilder()
                .setPedidoId(pedidoId.toString())
                .build();

        try {
            PedidoResponse response = stub.withDeadlineAfter(2, TimeUnit.SECONDS).marcarPedidoComoPago(request);
            if (!"PAGO".equals(response.getStatus())) {
                throw new RuntimeException("Pedido retornou status inesperado: " + response.getStatus());
            }
            System.out.println("[pedido-rpc-client] retorno OK de pedido. pedidoId=" + pedidoId + " status=" + response.getStatus());
        } catch (StatusRuntimeException e) {
            System.err.println("[pedido-rpc-client][erro] falha gRPC para pedido. pedidoId=" + pedidoId
                    + " master=" + masterId + " status=" + e.getStatus() + " descricao=" + e.getStatus().getDescription());
            if (e.getStatus().getCode() == Status.Code.FAILED_PRECONDITION) {
                throw new MasterUnavailableException("Follower recebeu update; aguardando atualizacao de master", e);
            }
            if (e.getStatus().getCode() == Status.Code.UNAVAILABLE || e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                throw new MasterUnavailableException("Master indisponivel no momento", e);
            }
            throw new RuntimeException("Falha na chamada gRPC para pedido: " + e.getStatus(), e);
        }
    }

    @Override
    public void close() {
        for (ManagedChannel channel : channels.values()) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private static List<NodeAddress> parseNodes(String nodesProperty) {
        if (nodesProperty == null || nodesProperty.isBlank()) {
            throw new IllegalArgumentException("pedido.cluster.nodes obrigatorio no pagamento");
        }

        List<NodeAddress> nodes = new ArrayList<>();
        for (String entry : nodesProperty.split(",")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String[] idAndAddr = trimmed.split("@");
            if (idAndAddr.length != 2) {
                throw new IllegalArgumentException("Formato invalido de node: " + trimmed);
            }
            int nodeId = Integer.parseInt(idAndAddr[0].trim());

            String[] hostPort = idAndAddr[1].trim().split(":");
            if (hostPort.length != 2) {
                throw new IllegalArgumentException("Formato host:porta invalido: " + trimmed);
            }
            nodes.add(new NodeAddress(nodeId, hostPort[0].trim(), Integer.parseInt(hostPort[1].trim())));
        }

        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("Nenhum node de pedido configurado");
        }
        return nodes;
    }

    private record NodeAddress(int nodeId, String host, int port) {
    }
}
