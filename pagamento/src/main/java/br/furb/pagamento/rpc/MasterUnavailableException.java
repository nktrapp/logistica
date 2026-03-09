package br.furb.pagamento.rpc;

public class MasterUnavailableException extends RuntimeException {

    public MasterUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}

