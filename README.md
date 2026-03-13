# logistica

## Autores
Nikolas Trapp
<br>
Paulo Ricardo Machado

projeto de faculdade. demonstração do sistema de eleição em anel.

Java: 25

## testar

1) criar bancos no postgres

```sql
CREATE DATABASE pedido_db;
CREATE DATABASE pagamento_db;
```

2) subir 5 instancias do pedido (5 terminais)

As portas padrões são 9091, 9092, 9093, 9094 e 9095 para os serviços de pedido

```powershell
cd ...\logistica\pedido
mvn -Dexec.mainClass=br.furb.pedido.Main -Dpedido.instance.id=1 exec:java
mvn -Dexec.mainClass=br.furb.pedido.Main -Dpedido.instance.id=2 exec:java
mvn -Dexec.mainClass=br.furb.pedido.Main -Dpedido.instance.id=3 exec:java
mvn -Dexec.mainClass=br.furb.pedido.Main -Dpedido.instance.id=4 exec:java
mvn -Dexec.mainClass=br.furb.pedido.Main -Dpedido.instance.id=5 exec:java
```

3) subir pagamento

```powershell
cd ...\logistica\pagamento
mvn -Dexec.mainClass=br.furb.pagamento.Main exec:java
```


4) pagar pedido (trocar pelo UUID) 

Já inserimos dados de teste com uuids de 00000000-0000-0000-0000-000000000000 até dddddddd-dddd-dddd-dddd-dddddddddddd

```powershell
grpcurl -plaintext -d "{\"pedido_id\":\"<pedido-uuid>\"}" localhost:9080 pagamento.PagamentoRpcService/ProcessarPagamento
```

5) testar eleicao: derrubar instancia master atual e pagar de novo. quando novo master assumir, ele notifica pagamento e pendencias sao reenviadas.

6) cenário recomendado para validar o anel (P1..P5)

- Suba as instâncias de pedido nos 5 terminais.
- Neste cenário, considere que o líder inicial é o P2 (Suba ele primeiro).
- Após tudo subir, derrube o processo do P2.
- Aguarde os followers detectarem a indisponibilidade do líder e iniciarem eleição.

7) como a eleição em anel funciona nesse cenário (P2 caiu, P5 vira líder)

Exemplo conceitual com anel lógico: `P1 -> P2 -> P3 -> P4 -> P5 -> P1`

- Situação inicial: `leader = P2`.
- Falha: `P2` é interrompido.
- Detecção: um nó ativo detecta que o líder está inalcançável no anel e inicia a eleição.
- Token de eleição: a mensagem percorre os próximos nós ativos, ignorando inativos. O maior ID visto é mantido no token.
	- Exemplo: inicia em `P3` com candidato `3`.
	- `P3` envia para `P4` (candidato vira `4`).
	- `P4` envia para `P5` (candidato vira `5`).
	- `P5` envia adiante no anel (pulando `P2` se estiver inativo), até a mensagem voltar ao iniciador.
- Decisão: quando o token retorna ao iniciador, o maior ID coletado é `5`, então `P5` é eleito novo coordenador.
- Notificação do coordenador: o iniciador envia a mensagem de coordinator no anel para todos atualizarem `leader = 5`.
- Integração com pagamento: quando `P5` assume como novo líder, ele chama o serviço de pagamento (`AtualizarMasterPedido`) para informar a troca de master.
- Efeito final: o pagamento passa a enviar escritas para `P5`, e pendências são reenviadas para o novo líder.
