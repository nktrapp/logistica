package br.furb.pagamento.rpc;

import br.furb.pagamento.domain.Pagamento;
import br.furb.pagamento.service.PagamentoService;
import br.furb.rpc.pagamento.AtualizarMasterPedidoRequest;
import br.furb.rpc.pagamento.AtualizarMasterPedidoResponse;
import br.furb.rpc.pagamento.PagamentoResponse;
import br.furb.rpc.pagamento.PagamentoRpcServiceGrpc;
import br.furb.rpc.pagamento.ProcessarPagamentoRequest;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.UUID;

public class PagamentoRpcServer {

    private final Server server;

    public PagamentoRpcServer(PagamentoService pagamentoService, int port) {
        this.server = ServerBuilder.forPort(port)
                .addService(new PagamentoRpcGrpcService(pagamentoService))
                .addService(ProtoReflectionService.newInstance())
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

    private static final class PagamentoRpcGrpcService extends PagamentoRpcServiceGrpc.PagamentoRpcServiceImplBase {

        private final PagamentoService pagamentoService;

        private PagamentoRpcGrpcService(PagamentoService pagamentoService) {
            this.pagamentoService = pagamentoService;
        }

        @Override
        public void processarPagamento(ProcessarPagamentoRequest request,
                                       StreamObserver<PagamentoResponse> responseObserver) {
            System.out.println("[pagamento-rpc] request ProcessarPagamento recebida. pedidoIdRaw=" + request.getPedidoId());
            UUID pedidoId;
            try {
                pedidoId = UUID.fromString(request.getPedidoId());
            } catch (IllegalArgumentException e) {
                System.err.println("[pagamento-rpc][erro] pedido_id invalido. ID=" + request.getPedidoId() + " motivo=" + e.getMessage());
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("pedido_id invalido")
                        .asRuntimeException());
                return;
            }

            try {
                Pagamento pagamento = pagamentoService.processarPagamento(pedidoId);
                PagamentoResponse response = PagamentoResponse.newBuilder()
                        .setPagamentoId(pagamento.getId().toString())
                        .setPedidoId(pagamento.getPedidoId().toString())
                        .setStatus(pagamento.getStatus().name())
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                System.out.println("[pagamento-rpc] ProcessarPagamento concluido com sucesso. pedidoId=" + pedidoId
                        + " pagamentoId=" + pagamento.getId() + " status=" + pagamento.getStatus());
            } catch (RuntimeException e) {
                System.err.println("[pagamento-rpc][erro] falha ao processar pagamento. pedidoId=" + pedidoId
                        + " excecao=" + e.getClass().getSimpleName() + " motivo=" + e.getMessage());
                responseObserver.onError(Status.INTERNAL
                        .withDescription("falha ao processar pagamento: " + e.getMessage())
                        .withCause(e)
                        .asRuntimeException());
            }
        }

        @Override
        public void atualizarMasterPedido(AtualizarMasterPedidoRequest request,
                                          StreamObserver<AtualizarMasterPedidoResponse> responseObserver) {
            System.out.println("[pagamento-rpc] request AtualizarMasterPedido recebida. novoMaster=" + request.getMasterNodeId());
            try {
                int reenviados = pagamentoService.atualizarMasterPedido(request.getMasterNodeId());
                AtualizarMasterPedidoResponse response = AtualizarMasterPedidoResponse.newBuilder()
                        .setOk(true)
                        .setPendenciasReenviadas(reenviados)
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                System.out.println("[pagamento-rpc] AtualizarMasterPedido concluido. novoMaster="
                        + request.getMasterNodeId() + " reenviados=" + reenviados);
            } catch (RuntimeException e) {
                System.err.println("[pagamento-rpc][erro] falha ao atualizar master de pedido. novoMaster="
                        + request.getMasterNodeId() + " excecao=" + e.getClass().getSimpleName()
                        + " motivo=" + e.getMessage());
                responseObserver.onError(Status.INTERNAL
                        .withDescription("falha ao atualizar master de pedido: " + e.getMessage())
                        .withCause(e)
                        .asRuntimeException());
            }
        }
    }
}
