# Mini gêmeo digital de rede com sockets

Projeto de estudo de Redes de Computadores 1. É uma aplicação cliente/servidor
em Python que monitora uma rede simulada. Os **nós** se registram num
**servidor** central e mandam sinais de vida. O servidor detecta falhas e
calcula rotas de menor custo, e um **operador** consulta o estado da rede e
intervém nela pelo terminal.

Usa só a biblioteca padrão e o módulo `socket` chamado etapa a etapa:
`bind`, `listen`, `accept`, `connect`, `send`, `recv`, `sendto`, `recvfrom`.
Nada de `sendall`, `makefile`, `socketserver`, `asyncio` ou frameworks.

## Como rodar

Requer Python 3.8 ou mais novo (testado no 3.14) e Linux. O `demo.sh` usa
bash 4 ou mais novo; no macOS, rode as peças à mão ou instale um bash novo.

**Tudo de uma vez:**

```bash
./demo.sh                # servidor + 5 nós; Ctrl+C encerra tudo
python3 operador.py      # em outro terminal
```

O `demo.sh` mostra o pid de cada nó, guarda os logs em `logs/` e acompanha o
log do servidor.

**Peça por peça**, cada comando num terminal:

```bash
python3 servidor.py
python3 no.py A B:4 C:1          # nó A, vizinhos B (custo 4) e C (custo 1)
python3 no.py B A:4 D:2
python3 operador.py
```

### Comandos do operador

| Comando | Resposta |
|---|---|
| `TOPOLOGIA` | nós e enlaces, cada enlace como `UTILIZAVEL`, `DERRUBADO` ou `INDISPONIVEL` |
| `STATUS [id]` | estado, último heartbeat, número de sequência, vizinhos |
| `CAMINHO <origem> <destino>` | rota de menor custo pelo estado atual |
| `DERRUBAR <nó> <nó>` | tira um enlace do roteamento |
| `RESTAURAR <nó> <nó>` | devolve o enlace |
| `ASSINAR` | passa a receber eventos (nó caiu, voltou, enlace mudou) |
| `AJUDA`, `SAIR` | (locais; Ctrl+D e Ctrl+C também saem) |

### Simulando falhas

| Ação no nó | O que o sistema operacional faz | O servidor conclui |
|---|---|---|
| Ctrl+C ou `kill <pid>` | o nó trata o sinal, envia `DESCONECTAR` e fecha a conexão | `DESCONECTADO`, na hora |
| `kill -9 <pid>` | o kernel mata o processo e fecha seus sockets (FIN) | `INATIVO`, na hora |
| Ctrl+Z ou `kill -STOP <pid>` | o processo para, mas a conexão continua aberta | `INATIVO` em 4 a 7 s (heartbeat parou) |
| `fg` ou `kill -CONT <pid>` | o processo volta a rodar | `ATIVO` no próximo heartbeat |

## Arquivos

| Arquivo | Papel |
|---|---|
| `protocolo.py` | constantes, tipos de mensagem, enquadramento TCP, envio parcial, UDP, validação de id e custo |
| `servidor.py` | tabela de nós com lock, uma thread por cliente, heartbeat, monitor, grafo e Dijkstra, comandos e eventos |
| `no.py` | registro TCP, heartbeat UDP, escuta do TCP, saída limpa |
| `operador.py` | terminal: comandos, respostas e eventos |
| `demo.sh` | sobe o servidor e 5 nós |
| `teste_protocolo.py` | enquadramento (mensagens coladas e partidas, UTF-8 cortado), UDP, validações |
| `teste_rotas.py` | grafo de enlaces utilizáveis e Dijkstra, sem rede |

## Protocolo

- **TCP 127.0.0.1:5000**: registro dos nós, comandos e eventos do operador.
- **UDP 127.0.0.1:5001**: heartbeats.

Todas as mensagens são objetos JSON em UTF-8 com um campo `"tipo"`.

**Enquadramento no TCP.** O TCP entrega um fluxo de bytes, não mensagens. Dois
`send()` podem chegar num único `recv()`, e uma mensagem pode chegar em
pedaços. Por isso cada mensagem termina em `\n`. O `ReceptorTCP` acumula bytes
num buffer e só recorta uma mensagem quando encontra o delimitador. O JSON
nunca contém um `\n` literal (dentro de strings ele vira `\\n`), então o
delimitador é inequívoco. A decodificação UTF-8 acontece só na mensagem
inteira, porque um caractere de 2 bytes pode vir cortado entre dois `recv()`.

**Envio parcial.** `send()` devolve quantos bytes o kernel aceitou, que podem
ser menos que o pedido. O `enviar_tcp` repete o `send()` sobre o restante.

**UDP.** Cada `sendto()` é um datagrama e cada `recvfrom()` devolve um
datagrama inteiro, então não há enquadramento a fazer. Mas pode haver perda e
troca de ordem, e por isso o heartbeat leva um número de sequência.

**NaN e Infinity** são recusados nos dois sentidos. O `json` do Python os
aceita, mas eles não são JSON padrão, e um custo `NaN` passaria por
`custo > 0`.

| Mensagem | Sentido | Exemplo |
|---|---|---|
| `REGISTRO` | nó → servidor | `{"tipo":"REGISTRO","id":"A","vizinhos":{"B":4,"C":1}}` |
| `RESPOSTA` | servidor → cliente | `{"tipo":"RESPOSTA","ok":true,"id":"A"}` |
| `ERRO` | servidor → cliente | `{"tipo":"ERRO","motivo":"id 'A' já está em uso por outra conexão"}` |
| `DESCONECTAR` | nó → servidor | `{"tipo":"DESCONECTAR","id":"A"}` |
| `HEARTBEAT` (UDP) | nó → servidor | `{"tipo":"HEARTBEAT","id":"A","seq":17}` |
| `OPERADOR` | operador → servidor | `{"tipo":"OPERADOR"}` |
| `COMANDO` | operador → servidor | `{"tipo":"COMANDO","comando":"CAMINHO","args":["A","D"]}` |
| `RESPOSTA` | servidor → operador | `{"tipo":"RESPOSTA","ok":true,"comando":"CAMINHO","dados":{"custo":4.5,"caminho":["A","C","E","D"],...}}` |
| `EVENTO` | servidor → operador | `{"tipo":"EVENTO","evento":"NO","hora":"21:00:43","id":"C","status":"INATIVO","motivo":"sem heartbeat há 6.6 s"}` |

A primeira mensagem de uma conexão TCP diz quem está do outro lado: `REGISTRO`
para um nó, `OPERADOR` para um operador. Cada `COMANDO` recebe exatamente uma
`RESPOSTA` ou um `ERRO`, na ordem em que foi enviado. Os `EVENTO`s podem
aparecer entre elas.

Ids de nó são textos sem espaços (o operador separa os argumentos por espaço).
Custos são números positivos e finitos.

## Como o servidor funciona

**Threads**

- **principal:** `accept()` em loop. Cada conexão aceita ganha um socket novo
  e uma thread própria, então um `recv()` parado esperando um cliente não trava
  os outros.
- **uma por cliente TCP:** lê as mensagens de um nó ou de um operador.
- **receptor UDP:** um único socket recebe os heartbeats de todos os nós.
- **monitor:** a cada 1 s, marca como `INATIVO` quem está há mais de 6 s sem
  heartbeat.
- **uma de envio por operador:** veja a seção sobre eventos.

**Estado compartilhado.** A tabela de nós, os enlaces derrubados e os
assinantes de eventos só são acessados com o lock adquirido. Sem ele, duas
threads poderiam ver "id livre" ao mesmo tempo e registrar o mesmo id duas
vezes. Nada é enviado pela rede com o lock na mão: um `send()` lento travaria
todas as outras threads. Consultas como o Dijkstra copiam o estado com o lock
e calculam fora dele.

**Por que heartbeat, se já existe a conexão TCP?** A conexão só denuncia a
morte do *processo*: quando ele termina, o kernel fecha os sockets e manda FIN.
Um processo suspenso ou travado mantém a conexão aberta, e o kernel dele
continua confirmando os segmentos sozinho. Só um sinal periódico gerado pelo
próprio programa prova que ele está executando.

O heartbeat vai por UDP porque é periódico e substituível. Retransmitir um
aviso velho, como o TCP faria, só atrasaria a informação.

**Tempos.** Heartbeat a cada 2 s e tempo limite de 6 s (três intervalos), o
que tolera dois datagramas perdidos seguidos. O monitor verifica a cada 1 s.
Como o último heartbeat saiu até 2 s antes da falha, a detecção leva de 4 a
7 s. O servidor descarta um heartbeat com `seq` menor ou igual ao último
recebido, porque um datagrama atrasado não pode fazer um nó parecer vivo, e
registra os saltos na numeração como perdas.

**Eventos e operadores lentos.** Um evento pode ser publicado por qualquer
thread (o monitor, o receptor UDP, a thread de um nó). Se várias escrevessem
no mesmo socket, os `send()` parciais de mensagens diferentes se intercalariam
e o enquadramento quebraria. Por isso cada operador tem *uma* thread de envio,
alimentada por uma fila.

Cada tipo de mensagem tem sua cota na fila:

- **Eventos:** até 100 pendentes. Sem vaga, o operador é desconectado, porque
  quem publica (monitor, receptor UDP) não pode esperar.
- **Respostas:** até 10 pendentes. A thread do próprio operador espera vaga,
  o que freia só aquele cliente. O TCP repassa o freio até o `send()` dele.

## Rotas

- Um enlace existe se pelo menos uma das pontas o declarou. Se as duas pontas
  declaram custos diferentes, vale o maior.
- Um enlace é **utilizável** só se as duas pontas estão `ATIVO` e ele não foi
  derrubado pelo operador. Só esses entram no grafo.
- O caminho é calculado com Dijkstra usando `heapq`. Como não há operação de
  "diminuir prioridade", uma distância melhor é empilhada de novo e as
  entradas antigas são puladas.
- As rotas são recalculadas a cada consulta, a partir do estado atual. Uma
  falha detectada já muda o próximo `CAMINHO`.

## Testes

```bash
python3 teste_protocolo.py   # enquadramento, UDP, validações
python3 teste_rotas.py       # grafo e Dijkstra
```

O resto foi testado com processos de verdade:

- 5 nós simultâneos, id repetido recusado;
- Ctrl+C, `kill -9`, `kill -STOP` e `kill -CONT` num nó;
- todos os comandos do operador e seus erros;
- 500 respostas e 500 eventos intercalados no mesmo socket sem corromper o
  enquadramento;
- um operador que assina eventos e nunca lê é desconectado sem afetar os
  outros;
- mensagens malformadas por TCP e UDP não derrubam nenhuma thread.

## Limitações conhecidas

- Não há autenticação: qualquer processo da máquina pode mandar um heartbeat
  em nome de outro nó ou se conectar como operador.
- Tudo roda em 127.0.0.1, e o estado vive só na memória do servidor. Se ele
  reiniciar, os nós saem e precisam ser iniciados de novo (não há reconexão
  automática).
- Um enlace derrubado continua derrubado mesmo que os nós saiam e voltem, até
  um `RESTAURAR`.
