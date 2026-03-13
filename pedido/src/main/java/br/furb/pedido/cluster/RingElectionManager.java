package br.furb.pedido.cluster;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import br.furb.rpc.pedido.HeartbeatResponse;

public class RingElectionManager implements AutoCloseable {

    private record NeighborProbe(HeartbeatResponse response, int neighborNodeId, boolean leaderUnreachableInRing) {
    }

    private final ClusterConfig config;
    private final ClusterRpcClient clusterRpcClient;
    private final PagamentoMasterNotifier pagamentoNotifier;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final AtomicInteger leaderId;
    private final AtomicInteger leaderMisses = new AtomicInteger(0);
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);

    public RingElectionManager(ClusterConfig config,
                               ClusterRpcClient clusterRpcClient,
                               PagamentoMasterNotifier pagamentoNotifier,
                               int initialLeaderId) {
        this.config = config;
        this.clusterRpcClient = clusterRpcClient;
        this.pagamentoNotifier = pagamentoNotifier;
        this.leaderId = new AtomicInteger(initialLeaderId);
    }

    public void start() {
        System.out.println("[cluster] node=" + config.selfNodeId() + " iniciou monitoramento. leader inicial=" + leaderId.get());
        scheduler.scheduleAtFixedRate(this::heartbeatLoop, 1200, 1200, TimeUnit.MILLISECONDS);
    }

    public boolean isSelfLeader() {
        return leaderId.get() == config.selfNodeId();
    }

    public int currentLeaderId() {
        return leaderId.get();
    }

    public int selfNodeId() {
        return config.selfNodeId();
    }

    public void onElection(int initiatorId, int candidateId) {
        int bestCandidate = Math.max(candidateId, config.selfNodeId());
        System.out.println("[election] node=" + config.selfNodeId()
                + " recebeu token: initiator=" + initiatorId
                + " candidateAtual=" + candidateId
                + " candidateEncaminhado=" + bestCandidate);

        if (initiatorId == config.selfNodeId()) {
            System.out.println("[election-decision] node=" + config.selfNodeId()
                    + " recebeu token de volta. leaderEscolhido=" + bestCandidate);
            applyLeader(bestCandidate, true);
            forwardCoordinator(config.nextNodeId(config.selfNodeId()), initiatorId, bestCandidate);
            return;
        }

        forwardElection(config.nextNodeId(config.selfNodeId()), initiatorId, bestCandidate);
    }

    public void onCoordinator(int initiatorId, int newLeaderId) {
        System.out.println("[coordinator] node=" + config.selfNodeId()
                + " recebeu coordenador: initiator=" + initiatorId
                + " newLeader=" + newLeaderId);
        applyLeader(newLeaderId, false);

        if (initiatorId == config.selfNodeId()) {
            electionInProgress.set(false);
            return;
        }
        forwardCoordinator(config.nextNodeId(config.selfNodeId()), initiatorId, newLeaderId);
    }

    private void heartbeatLoop() {
        if (isSelfLeader() || electionInProgress.get()) {
            return;
        }

        NeighborProbe probe = heartbeatNextActiveNeighbor();
        HeartbeatResponse neighbor = probe.response();
        int localLeader = leaderId.get();

        if (neighbor != null) {
            int observedLeader = neighbor.getLeaderId() > 0 ? neighbor.getLeaderId() : neighbor.getNodeId();
            if (probe.leaderUnreachableInRing() && localLeader != config.selfNodeId()) {
                int misses = leaderMisses.incrementAndGet();
                System.out.println("[heartbeat] node=" + config.selfNodeId()
                        + " detectou lider inalcançavel no anel. leaderAtual=" + localLeader
                        + " viaVizinho=" + probe.neighborNodeId()
                        + " leaderObservado=" + observedLeader
                        + " falhasConsecutivas=" + misses);

                if (misses >= 2) {
                    System.out.println("[heartbeat] node=" + config.selfNodeId()
                            + " confirmou queda do lider=" + localLeader
                            + ". iniciando eleicao em anel.");
                    triggerElection();
                }
                return;
            }

            if (observedLeader != localLeader) {
                System.out.println("[ring-sync] node=" + config.selfNodeId()
                        + " atualizando lider por gossip do anel. leaderLocal=" + localLeader
                        + " leaderObservado=" + observedLeader);
                applyLeader(observedLeader, false);
            }
            leaderMisses.set(0);
            return;
        }

        int misses = leaderMisses.incrementAndGet();
        System.out.println("[heartbeat] node=" + config.selfNodeId()
                + " nao encontrou sucessor ativo no anel"
                + " falhasConsecutivas=" + misses);
        if (misses >= 2) {
            System.out.println("[heartbeat] node=" + config.selfNodeId()
                    + " detectou anel degradado. iniciando eleicao em anel.");
            triggerElection();
        }
    }

    private NeighborProbe heartbeatNextActiveNeighbor() {
        int nextNodeId = config.nextNodeId(config.selfNodeId());
        int nodeId = nextNodeId;
        int currentLeader = leaderId.get();
        boolean leaderUnreachableInRing = false;

        for (int i = 0; i < config.ring().size() - 1; i++) {
            if (nodeId == config.selfNodeId()) {
                return new NeighborProbe(null, -1, leaderUnreachableInRing);
            }

            HeartbeatResponse response = clusterRpcClient.heartbeatInfo(nodeId, config.selfNodeId());
            if (response != null && response.getOk()) {
                if (nodeId != nextNodeId) {
                    System.out.println("[ring-skip] node=" + config.selfNodeId()
                            + " ignorou inativos e usou sucessor ativo=" + nodeId);
                }
                return new NeighborProbe(response, nodeId, leaderUnreachableInRing);
            }

            if (nodeId == currentLeader) {
                leaderUnreachableInRing = true;
                System.out.println("[heartbeat] node=" + config.selfNodeId()
                        + " nao conseguiu contato com liderAtual=" + currentLeader
                        + " durante varredura do anel");
            }

            System.out.println("[ring-skip] node=" + config.selfNodeId()
                    + " sucessor inativo=" + nodeId
                    + ". testando proximo no anel.");
            nodeId = config.nextNodeId(nodeId);
        }
        return new NeighborProbe(null, -1, leaderUnreachableInRing);
    }

    private void triggerElection() {
        if (!electionInProgress.compareAndSet(false, true)) {
            System.out.println("[election] node=" + config.selfNodeId()
                    + " ignorou trigger: eleicao ja em progresso");
            return;
        }

        int initiator = config.selfNodeId();
        int firstTarget = config.nextNodeId(initiator);
        System.out.println("[election] node=" + initiator + " iniciou eleicao. primeiroDestino=" + firstTarget);
        boolean sent = forwardElection(firstTarget, initiator, initiator);

        if (!sent) {
            System.out.println("[election-fallback] node=" + initiator
                    + " sem sucessor ativo no anel. assumindo master local (unico no ativo)." );
            applyLeader(initiator, true);
            electionInProgress.set(false);
        }
    }

    private boolean forwardElection(int startNodeId, int initiatorId, int candidateId) {
        int nodeId = startNodeId;
        for (int i = 0; i < config.ring().size() - 1; i++) {
            if (nodeId == config.selfNodeId()) {
                break;
            }
            System.out.println("[election-forward] from=" + config.selfNodeId()
                    + " to=" + nodeId
                    + " initiator=" + initiatorId
                    + " candidate=" + candidateId
                    + " tentativa=" + (i + 1));
            if (clusterRpcClient.sendElection(nodeId, initiatorId, candidateId)) {
                System.out.println("[election-forward] from=" + config.selfNodeId()
                        + " to=" + nodeId
                        + " status=ENTREGUE");
                return true;
            }
            System.out.println("[election-forward] from=" + config.selfNodeId()
                    + " to=" + nodeId
                    + " status=FALHA. tentando proximo no anel");
            nodeId = config.nextNodeId(nodeId);
        }
        return false;
    }

    private void forwardCoordinator(int startNodeId, int initiatorId, int newLeaderId) {
        int nodeId = startNodeId;
        for (int i = 0; i < config.ring().size() - 1; i++) {
            if (nodeId == config.selfNodeId()) {
                System.out.println("[coordinator-forward] from=" + config.selfNodeId()
                        + " sem destino remoto restante no anel. encerrando propagacao");
                return;
            }
            System.out.println("[coordinator-forward] from=" + config.selfNodeId()
                    + " to=" + nodeId
                    + " initiator=" + initiatorId
                    + " leader=" + newLeaderId
                    + " tentativa=" + (i + 1));
            if (clusterRpcClient.sendCoordinator(nodeId, initiatorId, newLeaderId)) {
                System.out.println("[coordinator-forward] from=" + config.selfNodeId()
                        + " to=" + nodeId
                        + " status=ENTREGUE");
                return;
            }
            System.out.println("[coordinator-forward] from=" + config.selfNodeId()
                    + " to=" + nodeId
                    + " status=FALHA. tentando proximo no anel");
            nodeId = config.nextNodeId(nodeId);
        }
    }

    private void applyLeader(int newLeaderId, boolean electedBySelf) {
        int previous = leaderId.getAndSet(newLeaderId);
        leaderMisses.set(0);
        electionInProgress.set(false);

        if (previous != newLeaderId) {
            System.out.println("[leader] node=" + config.selfNodeId()
                    + " atualizou lider: old=" + previous + " new=" + newLeaderId);
        }

        if (newLeaderId == config.selfNodeId() && (electedBySelf || previous != newLeaderId)) {
            if (previous != config.selfNodeId()) {
                System.out.println("[leader] node=" + config.selfNodeId()
                        + " se elegeu MASTER apos queda/troca do master anterior=" + previous);
            } else {
                System.out.println("[leader] node=" + config.selfNodeId() + " confirmou manutencao como MASTER");
            }
            pagamentoNotifier.notifyNewMaster(newLeaderId);
            System.out.println("[leader] node=" + config.selfNodeId()
                    + " notificou servico de pagamento sobre novo master=" + newLeaderId);
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
