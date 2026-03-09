package br.furb.pedido.cluster;

public record ClusterNode(int nodeId, String host, int port) {

    public String target() {
        return host + ":" + port;
    }
}

