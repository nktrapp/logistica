package br.furb.pedido.cluster;

import br.furb.rpc.pedido.ClusterAck;
import br.furb.rpc.pedido.CoordinatorRequest;
import br.furb.rpc.pedido.ElectionRequest;
import br.furb.rpc.pedido.HeartbeatRequest;
import br.furb.rpc.pedido.HeartbeatResponse;
import br.furb.rpc.pedido.PedidoRpcServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ClusterRpcClient implements AutoCloseable {

    private final Map<Integer, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<Integer, PedidoRpcServiceGrpc.PedidoRpcServiceBlockingStub> stubs = new ConcurrentHashMap<>();
    private final ClusterConfig config;

    public ClusterRpcClient(ClusterConfig config) {
        this.config = config;
        for (ClusterNode node : config.ring()) {
            if (node.nodeId() == config.selfNodeId()) {
                continue;
            }
            ManagedChannel channel = ManagedChannelBuilder.forAddress(node.host(), node.port())
                    .usePlaintext()
                    .build();
            channels.put(node.nodeId(), channel);
            stubs.put(node.nodeId(), PedidoRpcServiceGrpc.newBlockingStub(channel));
        }
    }

    public boolean heartbeat(int targetNodeId, int fromNodeId) {
        return heartbeatInfo(targetNodeId, fromNodeId) != null;
    }

    public HeartbeatResponse heartbeatInfo(int targetNodeId, int fromNodeId) {
        try {
            System.out.println("[rpc-heartbeat] from=" + fromNodeId + " to=" + targetNodeId + " status=ENVIANDO");
            var stub = stub(targetNodeId);
            HeartbeatResponse response = stub.withDeadlineAfter(1200, TimeUnit.MILLISECONDS)
                    .heartbeat(HeartbeatRequest.newBuilder().setFromNodeId(fromNodeId).build());
            System.out.println("[rpc-heartbeat] from=" + fromNodeId + " to=" + targetNodeId
                    + " status=OK leader=" + response.getLeaderId()
                    + " node=" + response.getNodeId());
            return response;
        } catch (StatusRuntimeException e) {
            if (fromNodeId < targetNodeId) {
                System.out.println("[rpc-heartbeat] from=" + fromNodeId + " to=" + targetNodeId
                        + " status=INDISPONIVEL");
                }
            return null;
        }
    }

    public boolean sendElection(int targetNodeId, int initiatorId, int candidateId) {
        try {
            System.out.println("[rpc-election] from=" + config.selfNodeId()
                + " to=" + targetNodeId
                + " initiator=" + initiatorId
                + " candidate=" + candidateId
                + " status=ENVIANDO");
            var stub = stub(targetNodeId);
            ClusterAck ack = stub.withDeadlineAfter(2, TimeUnit.SECONDS)
                    .election(ElectionRequest.newBuilder()
                            .setInitiatorId(initiatorId)
                            .setCandidateId(candidateId)
                            .build());
            System.out.println("[rpc-election] from=" + config.selfNodeId()
                + " to=" + targetNodeId
                + " status=" + (ack.getOk() ? "OK" : "NOK"));
            return ack.getOk();
        } catch (StatusRuntimeException e) {
            System.out.println("[rpc-election] from=" + config.selfNodeId()
                + " to=" + targetNodeId
                + " status=FALHA grpcStatus=" + e.getStatus());
            return false;
        }
    }

    public boolean sendCoordinator(int targetNodeId, int initiatorId, int leaderId) {
        try {
            System.out.println("[rpc-coordinator] from=" + config.selfNodeId()
                + " to=" + targetNodeId
                + " initiator=" + initiatorId
                + " leader=" + leaderId
                + " status=ENVIANDO");
            var stub = stub(targetNodeId);
            ClusterAck ack = stub.withDeadlineAfter(2, TimeUnit.SECONDS)
                    .coordinator(CoordinatorRequest.newBuilder()
                            .setInitiatorId(initiatorId)
                            .setLeaderId(leaderId)
                            .build());
            System.out.println("[rpc-coordinator] from=" + config.selfNodeId()
                + " to=" + targetNodeId
                + " status=" + (ack.getOk() ? "OK" : "NOK"));
            return ack.getOk();
        } catch (StatusRuntimeException e) {
            System.out.println("[rpc-coordinator] from=" + config.selfNodeId()
                + " to=" + targetNodeId
                + " status=FALHA grpcStatus=" + e.getStatus());
            return false;
        }
    }

    private PedidoRpcServiceGrpc.PedidoRpcServiceBlockingStub stub(int nodeId) {
        var stub = stubs.get(nodeId);
        if (stub == null) {
            throw new IllegalArgumentException("Node sem stub: " + nodeId);
        }
        return stub;
    }

    @Override
    public void close() {
        for (ManagedChannel channel : channels.values()) {
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
}

