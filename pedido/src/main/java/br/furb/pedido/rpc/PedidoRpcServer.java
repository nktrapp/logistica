package br.furb.pedido.rpc;

import br.furb.pedido.cluster.RingElectionManager;
import br.furb.pedido.domain.Pedido;
import br.furb.pedido.domain.PedidoStatus;
import br.furb.pedido.service.PedidoService;
import br.furb.rpc.pedido.ClusterAck;
import br.furb.rpc.pedido.CoordinatorRequest;
import br.furb.rpc.pedido.CriarPedidoRequest;
import br.furb.rpc.pedido.ElectionRequest;
import br.furb.rpc.pedido.HeartbeatRequest;
import br.furb.rpc.pedido.HeartbeatResponse;
import br.furb.rpc.pedido.MarcarPedidoComoPagoRequest;
import br.furb.rpc.pedido.PedidoResponse;
import br.furb.rpc.pedido.PedidoRpcServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.UUID;

public class PedidoRpcServer {

    private final Server server;

    public PedidoRpcServer(PedidoService pedidoService, RingElectionManager electionManager, int port) {
        this.server = ServerBuilder.forPort(port)
                .addService(new PedidoRpcGrpcService(pedidoService, electionManager))
                .build();
    }

    public void start() throws IOException {
        server.start();
    }

    public void blockUntilShutdown() throws InterruptedException {
        server.awaitTermination();
    }

    public void stop() {
        server.shutdown();
    }

    private static final class PedidoRpcGrpcService extends PedidoRpcServiceGrpc.PedidoRpcServiceImplBase {

        private final PedidoService pedidoService;
        private final RingElectionManager electionManager;

        private PedidoRpcGrpcService(PedidoService pedidoService, RingElectionManager electionManager) {
            this.pedidoService = pedidoService;
            this.electionManager = electionManager;
        }

        @Override
        public void criarPedido(CriarPedidoRequest request, StreamObserver<PedidoResponse> responseObserver) {
            Pedido pedido = pedidoService.criarPedido();
            PedidoResponse response = PedidoResponse.newBuilder()
                    .setPedidoId(pedido.getId().toString())
                    .setStatus(pedido.getStatus().name())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void marcarPedidoComoPago(MarcarPedidoComoPagoRequest request,
                                         StreamObserver<PedidoResponse> responseObserver) {
            UUID pedidoId;
            try {
                pedidoId = UUID.fromString(request.getPedidoId());
            } catch (IllegalArgumentException e) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("pedido_id invalido")
                        .asRuntimeException());
                return;
            }

            final boolean atualizado;
            try {
                atualizado = pedidoService.atualizarStatus(pedidoId, PedidoStatus.PAGO);
            } catch (IllegalStateException e) {
                responseObserver.onError(Status.FAILED_PRECONDITION
                        .withDescription("not-master leader=" + pedidoService.currentLeaderId())
                        .asRuntimeException());
                return;
            }

            if (!atualizado) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("pedido nao encontrado")
                        .asRuntimeException());
                return;
            }

            PedidoResponse response = PedidoResponse.newBuilder()
                    .setPedidoId(pedidoId.toString())
                    .setStatus(PedidoStatus.PAGO.name())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
            HeartbeatResponse response = HeartbeatResponse.newBuilder()
                    .setOk(true)
                    .setNodeId(electionManager.selfNodeId())
                    .setLeaderId(electionManager.currentLeaderId())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void election(ElectionRequest request, StreamObserver<ClusterAck> responseObserver) {
            electionManager.onElection(request.getInitiatorId(), request.getCandidateId());
            responseObserver.onNext(ClusterAck.newBuilder().setOk(true).build());
            responseObserver.onCompleted();
        }

        @Override
        public void coordinator(CoordinatorRequest request, StreamObserver<ClusterAck> responseObserver) {
            electionManager.onCoordinator(request.getInitiatorId(), request.getLeaderId());
            responseObserver.onNext(ClusterAck.newBuilder().setOk(true).build());
            responseObserver.onCompleted();
        }
    }
}
