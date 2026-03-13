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

        if (newLeaderId < config.selfNodeId() && !electionInProgress.get()) {
            System.out.println("[coordinator-correction] node=" + config.selfNodeId()
                    + " detectou coordenador inferior ao proprio ID. leaderRecebido=" + newLeaderId
                    + " self=" + config.selfNodeId()
                    + ". iniciando nova eleicao para convergir no maior ID.");
            triggerElection();
        }
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
            System.out.println("[election] node=" + config.selfNodeId()
                    + " ignorou trigger: eleicao ja em progresso");
            return;
        }

        int initiator = config.selfNodeId();
        int firstTarget = config.nextNodeId(initiator);
        System.out.println("[election] node=" + initiator + " iniciou eleicao. primeiroDestino=" + firstTarget);
        boolean sent = forwardElection(firstTarget, initiator, initiator);

        if (!sent) {
            int fallbackLeader = highestReachableNodeId();
            System.out.println("[election-fallback] node=" + initiator
                    + " nao conseguiu entregar token de eleicao. maiorNoAlcancavel=" + fallbackLeader);
            applyLeader(fallbackLeader, fallbackLeader == initiator);
            if (fallbackLeader != initiator) {
                forwardCoordinator(config.nextNodeId(initiator), initiator, fallbackLeader);
            }
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

    private int highestReachableNodeId() {
        int best = config.selfNodeId();
        for (ClusterNode node : config.ring()) {
            if (node.nodeId() == config.selfNodeId()) {
                continue;
            }
            boolean alive = clusterRpcClient.heartbeat(node.nodeId(), config.selfNodeId());
            System.out.println("[election-fallback] node=" + config.selfNodeId()
                    + " probe node=" + node.nodeId()
                    + " alive=" + alive);
            if (alive && node.nodeId() > best) {
                best = node.nodeId();
            }
        }
        return best;
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
