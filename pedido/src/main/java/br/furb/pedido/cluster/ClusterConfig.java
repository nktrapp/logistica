package br.furb.pedido.cluster;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ClusterConfig {

    private final int selfNodeId;
    private final List<ClusterNode> ring;
    private final Map<Integer, ClusterNode> byId;

    public ClusterConfig(int selfNodeId, String nodesProperty) {
        this.selfNodeId = selfNodeId;
        this.ring = parse(nodesProperty);
        this.byId = ring.stream().collect(Collectors.toMap(ClusterNode::nodeId, Function.identity()));
        if (!byId.containsKey(selfNodeId)) {
            throw new IllegalArgumentException("pedido.instance.id nao existe em pedido.cluster.nodes");
        }
    }

    public int selfNodeId() {
        return selfNodeId;
    }

    public ClusterNode selfNode() {
        return byId.get(selfNodeId);
    }

    public List<ClusterNode> ring() {
        return ring;
    }

    public ClusterNode byId(int nodeId) {
        return byId.get(nodeId);
    }

    public int defaultLeaderId() {
        return ring.get(0).nodeId();
    }

    public int nextNodeId(int nodeId) {
        int currentIndex = -1;
        for (int i = 0; i < ring.size(); i++) {
            if (ring.get(i).nodeId() == nodeId) {
                currentIndex = i;
                break;
            }
        }
        if (currentIndex < 0) {
            return defaultLeaderId();
        }
        return ring.get((currentIndex + 1) % ring.size()).nodeId();
    }

    private static List<ClusterNode> parse(String nodesProperty) {
        if (nodesProperty == null || nodesProperty.isBlank()) {
            throw new IllegalArgumentException("pedido.cluster.nodes obrigatorio");
        }

        List<ClusterNode> nodes = new ArrayList<>();
        String[] entries = nodesProperty.split(",");
        for (String entry : entries) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }

            String[] idAndAddress = trimmed.split("@");
            if (idAndAddress.length != 2) {
                throw new IllegalArgumentException("Formato invalido em pedido.cluster.nodes: " + trimmed);
            }
            int nodeId = Integer.parseInt(idAndAddress[0].trim());

            String[] hostPort = idAndAddress[1].trim().split(":");
            if (hostPort.length != 2) {
                throw new IllegalArgumentException("Formato host:porta invalido: " + trimmed);
            }
            String host = hostPort[0].trim();
            int port = Integer.parseInt(hostPort[1].trim());
            nodes.add(new ClusterNode(nodeId, host, port));
        }

        if (nodes.size() < 3) {
            throw new IllegalArgumentException("Cluster de pedido precisa de no minimo 3 instancias");
        }

        nodes.sort(Comparator.comparingInt(ClusterNode::nodeId));
        return List.copyOf(nodes);
    }
}

