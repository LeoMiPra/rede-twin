# Mini gêmeo digital de rede com sockets

Projeto de estudo de Redes de Computadores 1: aplicação cliente/servidor em
Python que monitora uma rede simulada. Nós reportam estado a um servidor
central, que detecta falhas e recalcula rotas; um operador consulta e intervém.

Responda sempre em português do Brasil. Prefira explicações de causa e efeito
(o que acontece no sistema operacional e na rede) a explicações genéricas.

## Regras obrigatórias

- Somente a biblioteca padrão do Python. Nada de `pip install`.
- Toda a comunicação usa o módulo `socket` chamado etapa a etapa.
- Proibido: `socketserver`, `asyncio` com streams, `http.server`, `xmlrpc`,
  ZeroMQ, Twisted, Flask e afins.
- Proibido dentro de `socket`: `makefile()`, `sendall()`, `create_server()`,
  `create_connection()`. Use sempre `protocolo.enviar_tcp` e
  `protocolo.ReceptorTCP`.
- `threading` para concorrência e `json` para serialização são permitidos.
- Todo acesso ao estado compartilhado do servidor acontece com o lock adquirido;
  nunca envie pela rede segurando o lock.
- Código comentado explicando o porquê de cada decisão de rede.

## Decisões fixas

- Endereço 127.0.0.1; TCP 5000 (registro, comandos, operador); UDP 5001 (heartbeat).
- JSON UTF-8, uma mensagem por linha (`\n` como delimitador) no TCP; um datagrama
  por mensagem no UDP.
- Heartbeat a cada 2 s, tempo limite 6 s, monitor a cada 1 s.
- Constantes e tipos de mensagem ficam em `protocolo.py`; os outros arquivos importam de lá.

## Arquivos

- `protocolo.py` — enquadramento TCP, envio parcial, UDP. Testado em `teste_protocolo.py`.
- `servidor.py` — tabela de nós com lock, thread por cliente, detecção de falha.
- `no.py` — um nó: registro TCP, heartbeat UDP, escuta de comandos.
- `operador.py` — cliente de terminal com comandos e eventos.
- `demo.sh` — sobe servidor e nós de uma vez.

## Roteiro

0. Ambiente — feito
1. Protocolo — feito (`python teste_protocolo.py`)
2. Servidor mínimo com registro — feito (testar com `nc 127.0.0.1 5000`)
3. Nó mínimo: argumentos, conexão, registro, escuta TCP.
   Teste: 5 nós juntos; id repetido recusado; Ctrl+C num nó detectado na hora.
4. Heartbeat UDP no nó; receptor UDP e thread monitor no servidor.
   Teste: Ctrl+Z num nó vira INATIVO em ~6 s; `fg` o traz de volta a ATIVO.
5. Grafo e Dijkstra (`heapq`) só sobre enlaces utilizáveis
   (ambos os nós ATIVO e enlace não derrubado manualmente).
6. Operador: TOPOLOGIA, STATUS, CAMINHO, DERRUBAR, RESTAURAR, ASSINAR.
7. Acabamento: erros tratados, `demo.sh`, README.

Só avance de etapa quando o teste da etapa passar. Faça um commit por etapa.
