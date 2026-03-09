# logistica

projeto de faculdade. bem simples.

## testar

1) criar bancos no postgres

```sql
CREATE DATABASE pedido_db;
CREATE DATABASE pagamento_db;
```

2) subir 3 instancias do pedido (3 terminais)

```powershell
cd C:\Users\nikolas.trapp\tmp\logistica\pedido
mvn -Dexec.mainClass=br.furb.pedido.Main -Dpedido.instance.id=1 exec:java
```

```powershell
cd C:\Users\nikolas.trapp\tmp\logistica\pedido
mvn -Dexec.mainClass=br.furb.pedido.Main -Dpedido.instance.id=2 exec:java
```

```powershell
cd C:\Users\nikolas.trapp\tmp\logistica\pedido
mvn -Dexec.mainClass=br.furb.pedido.Main -Dpedido.instance.id=3 exec:java
```

3) subir pagamento

```powershell
cd C:\Users\nikolas.trapp\tmp\logistica\pagamento
mvn -Dexec.mainClass=br.furb.pagamento.Main exec:java
```

4) criar pedido

```powershell
grpcurl -plaintext -d "{}" localhost:8081 pedido.PedidoRpcService/CriarPedido
```

5) pagar pedido (trocar pelo UUID)

```powershell
grpcurl -plaintext -d "{\"pedido_id\":\"<pedido-uuid>\"}" localhost:8082 pagamento.PagamentoRpcService/ProcessarPagamento
```

6) testar eleicao: derrubar instancia master atual e pagar de novo. quando novo master assumir, ele notifica pagamento e pendencias sao reenviadas.
