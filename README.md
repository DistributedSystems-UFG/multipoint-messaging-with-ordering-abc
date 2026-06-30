# Evolução do Modelo de Mensagens e Consistência (CRDT)

Este documento regista a evolução da arquitetura do projeto no que tange ao formato das mensagens trafegadas e ao modelo de consistência adotado pelo sistema distribuído.

## 1. O Novo Modelo de Mensagens

Para viabilizar a replicação de dados sem coordenação central, o formato de mensagens base de texto estruturado mudou de índices inteiros locais para **Identificadores Globais Absolutos baseados em Floats**. 

Cada operação de edição (`INSERT` ou `DELETE`) encapsula uma coordenada de ponto flutuante que dita a sua posição definitiva na linha do tempo lógica do documento:

```python
# Estrutura de Payload aceita pelo motor CRDT
op = {
    "user": self.my_id,          # ID único do nó criador
    "type": "INSERT",            # Tipo da operação (INSERT ou DELETE)
    "char": "a",                 # Caractere operado
    "pos": 0.47201,              # Posição flutuante absoluta (Chave)
    "timestamp": 1782785686.665  # Timestamp físico para desempate
}
```
Operações de Inserção: Geram um float estável. Se nós concorrentes gerarem a mesma base, o ID do utilizador é considerado como um infinitesimal de desempate, impedindo colisões.

Operações de Deleção: Em vez de apontarem para um índice dinâmico (como a remoção tradicional em vetores), carregam o identificador pos (float) cirúrgico e exato do caractere alvo, tornando a exclusão imune a variações de estado local do nó receptor.

## 2. Modelo de Consistência: CRDT (Strong Eventual Consistency)
O sistema abandonou verificações sequenciais rígidas de ordem de chegada e adotou um modelo de Consistência Eventual Forte (SEC) implementado através de um CRDT (Conflict-free Replicated Data Type) baseado em sequências lineares ordinais.

Propriedades Garantidas pelo Código:
Comutatividade: A aplicação das operações utiliza ordenação matemática explícita das posições físicas dos elementos (.sort(key=lambda x: (x["pos"], x["ts"]))). Disso decorre que, independentemente da ordem em que os pacotes de dados chegam através da rede assíncrona, o estado final convergido das réplicas será matematicamente idêntico.

Idempotência: A máquina de estados local blinda o documento contra retransmissões através de verificações de duplicidade antes de inserir o elemento no estado estável do CRDT.

A verificação de sucesso e validação de convergência total das réplicas passa a ser avaliada exclusivamente sobre o estado estável consolidado final (get_log_state()), que é garantido como idêntico em 100% dos nós ao término das transmissões concorrentes.
