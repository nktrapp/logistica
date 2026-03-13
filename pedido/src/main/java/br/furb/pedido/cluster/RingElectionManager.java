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
    private final AtomicBoolean participant = new AtomicBoolean(false);

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
        int selfId = config.selfNodeId();
        System.out.println("[ring-election] node=" + selfId
                + " recebeu token: initiator=" + initiatorId
                + " candidate=" + candidateId
                + " participant=" + participant.get());

        if (candidateId > selfId) {
            participant.set(true);
            forwardElection(config.nextNodeId(selfId), initiatorId, candidateId);
            return;
        }

        if (candidateId < selfId) {
            if (participant.compareAndSet(false, true)) {
                System.out.println("[ring-election] node=" + selfId
                        + " substituiu candidato menor por selfId=" + selfId);
                forwardElection(config.nextNodeId(selfId), selfId, selfId);
            } else {
                System.out.println("[ring-election] node=" + selfId
                        + " descartou candidato menor=" + candidateId
                        + " por ja estar participando");
            }
            return;
        }

        System.out.println("[ring-election-decision] node=" + selfId
                + " recebeu o proprio ID de volta e se tornou coordenador");
        participant.set(false);
        applyLeader(selfId, true);
        forwardCoordinator(config.nextNodeId(selfId), selfId, selfId);
    }

    public void onCoordinator(int initiatorId, int newLeaderId) {
        System.out.println("[coordinator] node=" + config.selfNodeId()
                + " recebeu coordenador: initiator=" + initiatorId
                + " newLeader=" + newLeaderId);
        applyLeader(newLeaderId, false);
        participant.set(false);

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

    private void triggerElection() {
        if (!electionInProgress.compareAndSet(false, true)) {
            System.out.println("[election] node=" + config.selfNodeId()
                    + " ignorou trigger: eleicao ja em progresso");
            return;
        }

        if (isSelfLeader()) {
            electionInProgress.set(false);
            return;
        }

        if (!participant.compareAndSet(false, true)) {
            System.out.println("[ring-election] node=" + config.selfNodeId()
                    + " ja participa de eleicao em andamento. trigger ignorado");
            electionInProgress.set(false);
            return;
        }

        int initiator = config.selfNodeId();
        int firstTarget = config.nextNodeId(initiator);
        System.out.println("[ring-election] node=" + initiator
                + " iniciou eleicao em anel. primeiroDestino=" + firstTarget
                + " candidateInicial=" + initiator);
        boolean sent = forwardElection(firstTarget, initiator, initiator);

        if (!sent) {
            System.out.println("[ring-election] node=" + initiator
                    + " nao encontrou proximo vivo para encaminhar token. assumindo coordenacao local");
            participant.set(false);
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
