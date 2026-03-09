package br.furb.pedido.cluster;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RingElectionManager implements AutoCloseable {

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

        int leader = leaderId.get();
        boolean leaderAlive = clusterRpcClient.heartbeat(leader, config.selfNodeId());

        int followerPeer = nextFollowerId();
        if (followerPeer != config.selfNodeId()) {
            clusterRpcClient.heartbeat(followerPeer, config.selfNodeId());
        }

        if (leaderAlive) {
            leaderMisses.set(0);
            return;
        }

        int misses = leaderMisses.incrementAndGet();
        System.out.println("[heartbeat] node=" + config.selfNodeId()
                + " nao recebeu heartbeat do master=" + leader
                + " falhasConsecutivas=" + misses);
        if (misses >= 2) {
            System.out.println("[heartbeat] node=" + config.selfNodeId()
                    + " detectou possivel queda do master=" + leader
                    + ". iniciando eleicao em anel.");
            triggerElection();
        }
    }

    private int nextFollowerId() {
        int candidate = config.nextNodeId(config.selfNodeId());
        int attempts = 0;
        while (attempts < config.ring().size()) {
            if (candidate != leaderId.get() && candidate != config.selfNodeId()) {
                return candidate;
            }
            candidate = config.nextNodeId(candidate);
            attempts++;
        }
        return config.selfNodeId();
    }

    private void triggerElection() {
        if (!electionInProgress.compareAndSet(false, true)) {
            return;
        }

        int initiator = config.selfNodeId();
        int firstTarget = config.nextNodeId(initiator);
        System.out.println("[election] node=" + initiator + " iniciou eleicao. primeiroDestino=" + firstTarget);
        boolean sent = forwardElection(firstTarget, initiator, initiator);

        if (!sent) {
            // Sem pares ativos, vira lider localmente.
            System.out.println("[election] node=" + initiator + " sem pares ativos. assumindo master local.");
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
            if (clusterRpcClient.sendElection(nodeId, initiatorId, candidateId)) {
                return true;
            }
            nodeId = config.nextNodeId(nodeId);
        }
        return false;
    }

    private void forwardCoordinator(int startNodeId, int initiatorId, int newLeaderId) {
        int nodeId = startNodeId;
        for (int i = 0; i < config.ring().size() - 1; i++) {
            if (nodeId == config.selfNodeId()) {
                return;
            }
            if (clusterRpcClient.sendCoordinator(nodeId, initiatorId, newLeaderId)) {
                return;
            }
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
