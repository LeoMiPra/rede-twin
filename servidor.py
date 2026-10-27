"""
servidor.py — Servidor central do mini gêmeo digital de rede.

Etapa 2: aceita conexões TCP, atende cada cliente numa thread própria e
mantém a tabela de nós registrados, protegida por um lock.

Etapa 4: recebe heartbeats UDP e roda uma thread monitor que marca como
INATIVO o nó que ficou TEMPO_LIMITE segundos sem dar sinal.

Por que heartbeat se já existe a conexão TCP? A conexão só denuncia a morte
do PROCESSO: quando ele termina, o kernel fecha os sockets e manda FIN/RST.
Um processo travado ou suspenso (Ctrl+Z) continua com a conexão aberta, e o
kernel dele continua respondendo ACK sozinho. Só um sinal periódico gerado
pelo próprio programa prova que ele ainda está trabalhando.

Etapa 5: monta o grafo da rede só com os enlaces utilizáveis e calcula o
caminho de menor custo com Dijkstra. As rotas são recalculadas a cada
consulta a partir do estado atual, então uma falha detectada já muda o
próximo caminho pedido.

Etapa 6: atende operadores na mesma porta TCP. A primeira mensagem separa
os papéis: REGISTRO é um nó, OPERADOR é um operador. O operador consulta
(TOPOLOGIA, STATUS, CAMINHO), intervém (DERRUBAR, RESTAURAR) e pode ASSINAR
os eventos, que o servidor empurra sem ter sido perguntado.

Threads do servidor:
  - principal: accept() em loop;
  - uma por cliente TCP (nó ou operador);
  - uma de envio por operador (ver ConexaoOperador);
  - receptor UDP: recvfrom() em loop na porta de heartbeat;
  - monitor: a cada INTERVALO_MONITOR, procura nós com heartbeat vencido.

Fluxo de uma conexão de nó:
  1. accept() devolve um socket NOVO, exclusivo daquele cliente. O socket
     de escuta continua livre para aceitar os próximos.
  2. Uma thread é criada para atender esse socket. Assim, um recv()
     bloqueado esperando o nó A não impede o servidor de atender o nó B.
  3. A primeira mensagem diz quem o cliente é. Se for REGISTRO, valida e
     grava o nó na tabela.
  4. A thread fica lendo o socket até o nó pedir DESCONECTAR (saída limpa)
     ou a conexão cair (falha detectada pelo TCP).

Rode com:  python servidor.py
"""

import heapq
import queue
import socket
import threading
import time

import protocolo as p

# Estados possíveis de um nó na tabela
ATIVO = "ATIVO"
INATIVO = "INATIVO"            # falhou: conexão caiu ou heartbeat parou
DESCONECTADO = "DESCONECTADO"  # saiu de forma limpa, avisando o servidor

# Estados de um enlace na TOPOLOGIA
UTILIZAVEL = "UTILIZAVEL"      # entra no Dijkstra
DERRUBADO = "DERRUBADO"        # o operador derrubou
INDISPONIVEL = "INDISPONIVEL"  # alguma ponta não está ATIVO

# Limites da fila de envio de cada operador (ver ConexaoOperador).
MAX_EVENTOS_PENDENTES = 100
MAX_RESPOSTAS_PENDENTES = 10
TEMPO_ESPERA_RESPOSTA = 5.0   # quanto uma resposta espera vaga antes de desistir

_lock_log = threading.Lock()


def log(texto):
    """print com horário. O lock evita que duas threads misturem linhas."""
    with _lock_log:
        print(f"[{time.strftime('%H:%M:%S')}] {texto}", flush=True)


# ----------------------------------------------------------------------
# Grafo e rotas (funções puras: recebem uma cópia do estado, sem lock)
# ----------------------------------------------------------------------

def enlace(a, b):
    """Chave de um enlace sem direção: frozenset({'A','B'}) == frozenset({'B','A'})."""
    return frozenset((a, b))


def enlaces_declarados(nos):
    """
    {enlace(a, b): custo} com todo enlace citado em algum registro, esteja
    utilizável ou não. Basta uma ponta declarar; custos divergentes: vale
    o maior (ver montar_grafo).
    """
    enlaces = {}
    for a, n in nos.items():
        for b, custo in n["vizinhos"].items():
            chave = enlace(a, b)
            enlaces[chave] = max(enlaces.get(chave, custo), custo)
    return enlaces


def montar_grafo(nos, derrubados):
    """
    Devolve {id: {vizinho: custo}} só com os enlaces utilizáveis.

    nos: {id: {"status", "vizinhos"}}; derrubados: conjunto de enlace(a, b).

    Regras:
      - Um enlace existe se pelo menos uma das pontas o declarou no registro;
        assim basta escrever `no.py A B:4`, sem repetir `A:4` no B.
      - Ele é utilizável só se as duas pontas estão ATIVO e o enlace não foi
        derrubado manualmente. Um vizinho que nunca se registrou não está
        ATIVO, então o enlace para ele não entra.
      - Se as duas pontas declaram custos diferentes, vale o maior: na dúvida,
        o caminho é considerado mais caro, não mais barato.
    O grafo sai simétrico (A->B e B->A com o mesmo custo).
    """
    grafo = {i: {} for i, n in nos.items() if n["status"] == ATIVO}
    for chave, custo in enlaces_declarados(nos).items():
        a, b = chave
        if a in grafo and b in grafo and chave not in derrubados:
            grafo[a][b] = grafo[b][a] = custo
    return grafo


def dijkstra(grafo, origem, destino):
    """
    Caminho de menor custo de origem a destino.

    Devolve (custo_total, [origem, ..., destino]) ou None se não houver
    caminho (ou se uma das pontas não está no grafo, isto é, não está ATIVO).

    A fila de prioridade (heapq) sempre entrega o nó ainda não fechado com a
    menor distância conhecida. Como os custos são positivos, essa distância
    já é definitiva: nenhum desvio posterior pode ficar mais barato.
    """
    if origem not in grafo or destino not in grafo:
        return None

    distancia = {origem: 0}
    anterior = {}            # para reconstruir o caminho de trás para frente
    fechados = set()
    fila = [(0, origem)]     # (distância, id): o heap ordena pela distância,
                             # e o id desempata de forma determinística
    while fila:
        d, u = heapq.heappop(fila)
        if u in fechados:
            # heapq não tem "diminuir prioridade": quando achamos uma distância
            # melhor, empilhamos de novo. As entradas antigas são puladas aqui.
            continue
        fechados.add(u)
        if u == destino:
            break
        for v, custo in grafo[u].items():
            nova = d + custo
            if v not in distancia or nova < distancia[v]:
                distancia[v] = nova
                anterior[v] = u
                heapq.heappush(fila, (nova, v))

    if destino not in fechados:
        return None
    caminho = [destino]
    while caminho[-1] != origem:
        caminho.append(anterior[caminho[-1]])
    caminho.reverse()
    return distancia[destino], caminho


def listar_enlaces(grafo):
    """[(a, b, custo), ...] com cada enlace uma vez só, a < b."""
    return sorted((a, b, c) for a, viz in grafo.items() for b, c in viz.items() if a < b)


class ErroComando(Exception):
    """Comando do operador recusado; o texto vai para o campo 'motivo' do ERRO."""


class ConexaoOperador:
    """
    Lado de envio da conexão com um operador.

    Duas threads podem querer escrever no mesmo socket: a que atende o
    operador (respostas) e qualquer uma que publique um evento (monitor,
    receptor UDP, thread de outro nó). enviar_tcp faz vários send() quando o
    envio é parcial; se duas threads fizessem isso ao mesmo tempo, os pedaços
    das duas mensagens se intercalariam no fluxo e o enquadramento por '\n'
    quebraria do outro lado.

    Solução: só UMA thread escreve no socket. As outras põem a mensagem numa
    fila (queue.Queue já é segura entre threads) e seguem em frente.

    E se o operador parar de ler? O buffer de recepção dele enche, o nosso
    buffer de envio enche, o send() da thread de envio bloqueia e a fila
    cresce. Para não crescer sem fim, cada tipo de mensagem tem sua cota,
    controlada por um semáforo (cada vaga é adquirida ao enfileirar e
    devolvida depois do envio):
      - eventos: quem publica é o monitor ou o receptor UDP, que não podem
        parar por causa de um operador. Sem vaga, o evento é recusado e o
        operador, desconectado.
      - respostas: quem enfileira é a thread do próprio operador. Ela pode
        esperar uma vaga, o que freia só aquele operador: ela para de ler
        comandos, o buffer do kernel enche e o TCP repassa o freio até o
        send() do cliente.
    Cotas separadas impedem que uma rajada de respostas tome o lugar dos
    eventos, e vice-versa.
    """

    def __init__(self, sock, endereco):
        self.sock = sock
        self.endereco = endereco
        # Sem limite próprio: os limites ficam nos semáforos.
        self.fila = queue.Queue()
        self.vagas_eventos = threading.BoundedSemaphore(MAX_EVENTOS_PENDENTES)
        self.vagas_respostas = threading.BoundedSemaphore(MAX_RESPOSTAS_PENDENTES)
        self.thread = threading.Thread(target=self._enviar_em_loop, daemon=True)
        self.thread.start()

    def enfileirar_evento(self, msg):
        """Não bloqueia. Devolve False se a cota de eventos acabou."""
        if not self.vagas_eventos.acquire(blocking=False):
            return False
        self.fila.put((msg, self.vagas_eventos))
        return True

    def enfileirar_resposta(self, msg):
        """Espera até TEMPO_ESPERA_RESPOSTA por uma vaga. Devolve False se não houve."""
        if not self.vagas_respostas.acquire(timeout=TEMPO_ESPERA_RESPOSTA):
            return False
        self.fila.put((msg, self.vagas_respostas))
        return True

    def _enviar_em_loop(self):
        while True:
            item = self.fila.get()
            if item is None:         # sentinela: encerrar
                return
            msg, vaga = item
            try:
                p.enviar_tcp(self.sock, msg)
            except OSError:
                # Conexão morta. A thread que lê o socket vai perceber e
                # fazer a limpeza; aqui só paramos de escrever.
                return
            finally:
                vaga.release()

    def interromper(self):
        """
        Corta a conexão nos dois sentidos. shutdown() acorda as threads
        bloqueadas em recv()/send() neste socket; close() não faria isso.
        """
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def encerrar(self):
        """Envia o que ainda está na fila e para a thread de envio."""
        self.fila.put(None)
        # Espera pouco: se o operador não lê, o send() ficaria bloqueado.
        self.thread.join(timeout=2)
        if self.thread.is_alive():
            self.interromper()


class Servidor:
    def __init__(self, host=p.HOST, porta_tcp=p.PORTA_TCP, porta_udp=p.PORTA_UDP):
        self.host = host
        self.porta_tcp = porta_tcp
        self.porta_udp = porta_udp

        # Estado compartilhado entre todas as threads.
        # Regra: TODO acesso a self.nos acontece com self.lock adquirido.
        # Sem isso, duas threads poderiam ver "id livre" ao mesmo tempo e
        # registrar o mesmo id duas vezes (condição de corrida clássica:
        # verificar-e-depois-agir sem atomicidade).
        self.lock = threading.Lock()
        # id -> {"sock", "endereco", "status", "vizinhos", "desde",
        #        "ultimo_heartbeat", "ultimo_seq"}
        # "sock" é None quando a conexão TCP do nó já acabou.
        self.nos = {}
        # Enlaces derrubados pelo operador (etapa 6), como enlace(a, b).
        # Também é estado compartilhado: só se acessa com self.lock.
        self.enlaces_derrubados = set()
        # Operadores que pediram ASSINAR (objetos ConexaoOperador).
        self.assinantes = set()

    # ------------------------------------------------------------------
    # Socket de escuta
    # ------------------------------------------------------------------

    def iniciar(self):
        escuta = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Ao fechar um servidor, a porta fica alguns segundos em TIME_WAIT e
        # um novo bind() falharia com "Address already in use". SO_REUSEADDR
        # permite reutilizá-la imediatamente ao reiniciar o servidor.
        escuta.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        escuta.bind((self.host, self.porta_tcp))
        escuta.listen()   # fila de conexões pendentes com o tamanho padrão do sistema
        log(f"Servidor ouvindo TCP em {self.host}:{self.porta_tcp}")

        # O socket UDP é criado aqui, na thread principal, para que um erro de
        # bind() (porta ocupada) derrube o servidor logo na partida. Não há
        # listen()/accept(): UDP não tem conexão, um único socket recebe os
        # datagramas de todos os nós. Também não precisa de SO_REUSEADDR,
        # porque sem conexão não existe TIME_WAIT.
        udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp.bind((self.host, self.porta_udp))
        log(f"Servidor ouvindo UDP em {self.host}:{self.porta_udp}")
        threading.Thread(target=self.receber_heartbeats, args=(udp,), daemon=True).start()
        threading.Thread(target=self.monitorar, daemon=True).start()

        try:
            while True:
                conexao, endereco = escuta.accept()
                # daemon=True: a thread não impede o programa de encerrar.
                threading.Thread(
                    target=self.atender_cliente,
                    args=(conexao, endereco),
                    daemon=True,
                ).start()
        except KeyboardInterrupt:
            log("Encerrando servidor (Ctrl+C)")
        finally:
            escuta.close()
            udp.close()

    # ------------------------------------------------------------------
    # Atendimento de uma conexão (roda numa thread por cliente)
    # ------------------------------------------------------------------

    def atender_cliente(self, conexao, endereco):
        log(f"Conexão TCP de {endereco[0]}:{endereco[1]}")
        receptor = p.ReceptorTCP(conexao)
        try:
            primeira = receptor.receber()
            if primeira is None:
                log(f"{endereco[1]} fechou antes de se identificar")
            elif primeira["tipo"] == p.REGISTRO:
                self.atender_no(conexao, endereco, receptor, primeira)
            elif primeira["tipo"] == p.OPERADOR:
                self.atender_operador(conexao, endereco, receptor)
            else:
                self.responder(conexao, p.criar_mensagem(
                    p.ERRO, motivo="primeira mensagem deve ser REGISTRO ou OPERADOR"))
        except p.ErroProtocolo as e:
            log(f"{endereco[1]} violou o protocolo: {e}")
            self.responder(conexao, p.criar_mensagem(p.ERRO, motivo=str(e)))
        except OSError as e:
            # ConnectionResetError e similares: a conexão caiu de forma abrupta.
            log(f"{endereco[1]} conexão perdida: {e}")
        finally:
            conexao.close()

    def atender_no(self, conexao, endereco, receptor, registro):
        # --- 1. Validar o registro --------------------------------------
        id_no = registro.get("id")
        vizinhos = registro.get("vizinhos", {})
        problema = self.validar_registro(id_no, vizinhos)
        if problema:
            self.responder(conexao, p.criar_mensagem(p.ERRO, motivo=problema))
            log(f"Registro recusado de {endereco[1]}: {problema}")
            return

        # --- 2. Gravar na tabela (seção crítica) ------------------------
        with self.lock:
            existente = self.nos.get(id_no)
            # O id está ocupado enquanto houver uma conexão TCP viva para ele,
            # mesmo que o nó esteja INATIVO por falta de heartbeat: o processo
            # suspenso ainda existe e pode voltar com `fg`.
            if existente and existente["sock"] is not None:
                recusado = True
            else:
                # Id novo, ou um nó que caiu/saiu e está voltando.
                recusado = False
                self.nos[id_no] = {
                    "sock": conexao,
                    "endereco": endereco,
                    "status": ATIVO,
                    "vizinhos": vizinhos,
                    "desde": time.time(),
                    # Prazo do primeiro heartbeat conta a partir do registro.
                    # monotonic() não volta no tempo se alguém acertar o relógio
                    # do sistema, então serve para medir intervalos.
                    "ultimo_heartbeat": time.monotonic(),
                    # Processo novo recomeça a numeração em 1.
                    "ultimo_seq": 0,
                }
        # A resposta vai pela rede FORA do lock: um send() lento não pode
        # travar as outras threads que querem consultar a tabela.
        if recusado:
            self.responder(conexao, p.criar_mensagem(
                p.ERRO, motivo=f"id '{id_no}' já está em uso por outra conexão"))
            log(f"Registro recusado: id '{id_no}' duplicado")
            return

        self.responder(conexao, p.criar_mensagem(p.RESPOSTA, ok=True, id=id_no))
        log(f"Nó {id_no} registrado (vizinhos: {vizinhos})")
        self.publicar_no(id_no, ATIVO, "registrado")
        self.mostrar_tabela()

        # --- 3. Ouvir o nó até ele sair ou cair -------------------------
        motivo_fim = INATIVO
        causa = "conexão TCP encerrada"
        try:
            while True:
                msg = receptor.receber()
                if msg is None:
                    # recv() devolveu 0 bytes: o outro lado fechou (FIN).
                    # Acontece quando o processo do nó morre (Ctrl+C, kill):
                    # o sistema operacional dele fecha os sockets.
                    log(f"Nó {id_no}: conexão TCP encerrada -> {INATIVO}")
                    break
                if msg["tipo"] == p.DESCONECTAR:
                    motivo_fim = DESCONECTADO
                    causa = "saída limpa"
                    log(f"Nó {id_no} saiu de forma limpa -> {DESCONECTADO}")
                    break
                log(f"Nó {id_no}: mensagem ignorada: {msg}")
        except OSError:
            # RST: queda abrupta. Mesmo efeito que o FIN para nós.
            causa = "conexão TCP resetada"
            log(f"Nó {id_no}: conexão TCP resetada -> {INATIVO}")
        finally:
            with self.lock:
                atual = self.nos.get(id_no)
                # Só altera se a entrada ainda for desta conexão. Se o nó já
                # se reconectou por outro socket, não podemos derrubá-lo.
                mudou = atual is not None and atual["sock"] is conexao
                if mudou:
                    atual["status"] = motivo_fim
                    atual["sock"] = None
            if mudou:
                self.publicar_no(id_no, motivo_fim, causa)
            self.mostrar_tabela()

    # ------------------------------------------------------------------
    # Operador
    # ------------------------------------------------------------------

    def atender_operador(self, conexao, endereco, receptor):
        """
        Laço pedido-resposta. Cada COMANDO recebe exatamente uma RESPOSTA ou
        um ERRO, na ordem em que chegou: como o TCP entrega em ordem e esta
        thread trata um comando por vez, o operador não precisa de um número
        de pedido para casar resposta com pergunta. Os EVENTOs podem chegar
        entre elas; o operador os separa pelo campo "tipo".
        """
        op = ConexaoOperador(conexao, endereco)
        op.enfileirar_resposta(p.criar_mensagem(p.RESPOSTA, ok=True))
        log(f"Operador conectado ({endereco[1]})")
        try:
            while True:
                try:
                    msg = receptor.receber()
                except p.ErroProtocolo as e:
                    # O ERRO vai pela fila: escrever direto aqui disputaria o
                    # socket com a thread de envio.
                    op.enfileirar_resposta(p.criar_mensagem(p.ERRO, motivo=str(e)))
                    log(f"Operador {endereco[1]} violou o protocolo: {e}")
                    break
                if msg is None:
                    log(f"Operador {endereco[1]} desconectou")
                    break
                resposta = self.executar_comando(op, msg)
                if not op.enfileirar_resposta(resposta):
                    log(f"Operador {endereco[1]} não lê as respostas; desconectando")
                    break
        finally:
            with self.lock:
                self.assinantes.discard(op)
            # Antes do close() em atender_cliente: dá tempo de a última
            # resposta sair e garante que ninguém mais escreve no socket.
            op.encerrar()

    def executar_comando(self, op, msg):
        """Devolve a mensagem de resposta (RESPOSTA ou ERRO) para um COMANDO."""
        if msg["tipo"] != p.COMANDO:
            return p.criar_mensagem(p.ERRO, motivo=f"esperava {p.COMANDO}, veio {msg['tipo']}")
        comando = str(msg.get("comando", "")).upper()
        args = msg.get("args", [])
        if not isinstance(args, list) or not all(isinstance(a, str) for a in args):
            return p.criar_mensagem(p.ERRO, motivo="'args' deve ser uma lista de textos")

        # comando -> (função, mínimo de argumentos, máximo de argumentos)
        tratadores = {
            p.TOPOLOGIA: (self.cmd_topologia, 0, 0),
            p.STATUS: (self.cmd_status, 0, 1),
            p.CAMINHO: (self.cmd_caminho, 2, 2),
            p.DERRUBAR: (self.cmd_derrubar, 2, 2),
            p.RESTAURAR: (self.cmd_restaurar, 2, 2),
            p.ASSINAR: (self.cmd_assinar, 0, 0),
        }
        if comando not in tratadores:
            return p.criar_mensagem(p.ERRO, motivo=f"comando desconhecido: '{comando}'")
        funcao, minimo, maximo = tratadores[comando]
        if not minimo <= len(args) <= maximo:
            return p.criar_mensagem(p.ERRO, motivo=f"uso: {p.USO[comando]}")
        try:
            dados = funcao(op, *args)
        except ErroComando as e:
            return p.criar_mensagem(p.ERRO, comando=comando, motivo=str(e))
        return p.criar_mensagem(p.RESPOSTA, ok=True, comando=comando, dados=dados)

    def cmd_topologia(self, _op):
        nos, derrubados = self.foto()
        grafo = montar_grafo(nos, derrubados)
        enlaces = []
        for chave, custo in enlaces_declarados(nos).items():
            a, b = sorted(chave)
            if chave in derrubados:
                estado = DERRUBADO
            elif b in grafo.get(a, {}):
                estado = UTILIZAVEL
            else:
                estado = INDISPONIVEL
            enlaces.append({"a": a, "b": b, "custo": custo, "estado": estado})
        enlaces.sort(key=lambda e: (e["a"], e["b"]))
        return {"nos": {i: n["status"] for i, n in sorted(nos.items())}, "enlaces": enlaces}

    def cmd_status(self, _op, id_no=None):
        agora = time.monotonic()
        with self.lock:
            if id_no is not None and id_no not in self.nos:
                erro = f"nó '{id_no}' desconhecido"
            else:
                erro = None
                ids = [id_no] if id_no is not None else sorted(self.nos)
                dados = {i: {
                    "status": self.nos[i]["status"],
                    "vizinhos": dict(self.nos[i]["vizinhos"]),
                    "porta_tcp": self.nos[i]["endereco"][1],
                    "desde": time.strftime("%H:%M:%S", time.localtime(self.nos[i]["desde"])),
                    "ultimo_seq": self.nos[i]["ultimo_seq"],
                    "silencio": round(agora - self.nos[i]["ultimo_heartbeat"], 1),
                } for i in ids}
        if erro:
            raise ErroComando(erro)
        return {"nos": dados}

    def cmd_caminho(self, _op, origem, destino):
        nos, derrubados = self.foto()
        for ponta in (origem, destino):
            if ponta not in nos:
                raise ErroComando(f"nó '{ponta}' desconhecido")
            if nos[ponta]["status"] != ATIVO:
                raise ErroComando(f"nó '{ponta}' está {nos[ponta]['status']}")
        rota = dijkstra(montar_grafo(nos, derrubados), origem, destino)
        if rota is None:
            raise ErroComando(f"sem rota entre {origem} e {destino}")
        custo, caminho = rota
        return {"origem": origem, "destino": destino, "custo": custo, "caminho": caminho}

    def cmd_derrubar(self, op, a, b):
        return self.mudar_enlace(op, a, b, derrubar=True)

    def cmd_restaurar(self, op, a, b):
        return self.mudar_enlace(op, a, b, derrubar=False)

    def mudar_enlace(self, op, a, b, derrubar):
        chave = enlace(a, b)
        with self.lock:
            declarado = ((a in self.nos and b in self.nos[a]["vizinhos"])
                         or (b in self.nos and a in self.nos[b]["vizinhos"]))
            if a == b or not declarado:
                erro = f"enlace {a}-{b} não existe"
            elif derrubar and chave in self.enlaces_derrubados:
                erro = f"enlace {a}-{b} já está derrubado"
            elif not derrubar and chave not in self.enlaces_derrubados:
                erro = f"enlace {a}-{b} não está derrubado"
            else:
                erro = None
                if derrubar:
                    self.enlaces_derrubados.add(chave)
                else:
                    self.enlaces_derrubados.discard(chave)
        if erro:
            raise ErroComando(erro)

        a, b = sorted(chave)
        estado = DERRUBADO if derrubar else "RESTAURADO"
        log(f"Operador {op.endereco[1]}: enlace {a}-{b} {estado}")
        self.publicar(p.EVENTO_ENLACE, a=a, b=b, estado=estado)
        self.mostrar_tabela()
        return {"a": a, "b": b, "estado": estado}

    def cmd_assinar(self, op):
        with self.lock:
            ja_assinava = op in self.assinantes
            self.assinantes.add(op)
        if ja_assinava:
            raise ErroComando("já assinava os eventos")
        log(f"Operador {op.endereco[1]} assinou os eventos")
        return {"assinado": True}

    # ------------------------------------------------------------------
    # Eventos
    # ------------------------------------------------------------------

    def publicar(self, evento, **campos):
        """
        Entrega um evento a todos os assinantes. A lista é copiada sob o lock
        e a entrega acontece fora dele; enfileirar não bloqueia, então quem
        publica (monitor, receptor UDP...) nunca espera a rede.
        """
        msg = p.criar_mensagem(p.EVENTO, evento=evento,
                               hora=time.strftime("%H:%M:%S"), **campos)
        with self.lock:
            destinos = list(self.assinantes)
        for op in destinos:
            if not op.enfileirar_evento(msg):
                # Fila cheia: o operador parou de ler. Cortar a conexão faz o
                # recv() da thread dele falhar, e ela faz a limpeza.
                log(f"Operador {op.endereco[1]} não acompanha os eventos; desconectando")
                op.interromper()

    def publicar_no(self, id_no, status, motivo):
        self.publicar(p.EVENTO_NO, id=id_no, status=status, motivo=motivo)

    # ------------------------------------------------------------------
    # Heartbeat UDP e detecção de falha
    # ------------------------------------------------------------------

    def receber_heartbeats(self, udp):
        """Thread receptora: cada recvfrom() devolve exatamente um datagrama."""
        while True:
            try:
                msg, _endereco = p.receber_udp(udp)
            except p.ErroProtocolo as e:
                # Um datagrama ruim não afeta os outros: não há fluxo para
                # dessincronizar, basta descartá-lo.
                log(f"Datagrama UDP descartado: {e}")
                continue
            except OSError as e:
                if udp.fileno() == -1:
                    return   # socket fechado no encerramento do servidor
                log(f"Erro no socket UDP: {e}")
                continue
            if msg["tipo"] != p.HEARTBEAT:
                log(f"Datagrama UDP ignorado (tipo {msg['tipo']})")
                continue
            self.registrar_heartbeat(msg)

    def registrar_heartbeat(self, msg):
        id_no = msg.get("id")
        seq = msg.get("seq")
        if not isinstance(seq, int) or isinstance(seq, bool):
            log(f"Heartbeat de {id_no} sem número de sequência válido")
            return

        voltou = False
        perdidos = 0
        ignorado = None
        with self.lock:
            no = self.nos.get(id_no)
            if no is None or no["sock"] is None:
                # Sem conexão TCP o nó não está registrado: pode ser um
                # datagrama atrasado de um processo que já morreu.
                ignorado = "nó sem registro ativo"
            elif seq <= no["ultimo_seq"]:
                # O UDP não garante ordem: um datagrama antigo chegando depois
                # de um mais novo não pode "rejuvenescer" o nó.
                ignorado = f"seq {seq} fora de ordem ou repetido"
            else:
                # Saltos na numeração = datagramas que a rede perdeu.
                perdidos = seq - no["ultimo_seq"] - 1
                no["ultimo_seq"] = seq
                no["ultimo_heartbeat"] = time.monotonic()
                if no["status"] == INATIVO:
                    no["status"] = ATIVO
                    voltou = True

        # Log e tabela fora do lock (mostrar_tabela adquire o lock sozinho).
        if ignorado:
            log(f"Heartbeat de {id_no} ignorado: {ignorado}")
        if perdidos:
            log(f"Nó {id_no}: {perdidos} heartbeat(s) perdido(s) antes do seq {seq}")
        if voltou:
            log(f"Nó {id_no}: heartbeat voltou -> {ATIVO}")
            self.publicar_no(id_no, ATIVO, "heartbeat voltou")
            self.mostrar_tabela()

    def monitorar(self):
        """
        Thread monitor: varre a tabela a cada INTERVALO_MONITOR segundos.

        Um nó é detectado entre TEMPO_LIMITE e TEMPO_LIMITE + INTERVALO_MONITOR
        depois do seu último heartbeat. Como esse último heartbeat saiu até
        INTERVALO_HEARTBEAT antes da falha, o atraso total fica entre 4 e 7 s.
        O limite é 3 intervalos para tolerar até 2 datagramas perdidos seguidos
        sem alarme falso.
        """
        while True:
            time.sleep(p.INTERVALO_MONITOR)
            agora = time.monotonic()
            caidos = []
            with self.lock:
                for id_no, no in self.nos.items():
                    silencio = agora - no["ultimo_heartbeat"]
                    if no["status"] == ATIVO and silencio > p.TEMPO_LIMITE:
                        no["status"] = INATIVO
                        caidos.append((id_no, silencio))
            for id_no, silencio in caidos:
                log(f"Nó {id_no}: sem heartbeat há {silencio:.1f} s -> {INATIVO}")
                self.publicar_no(id_no, INATIVO, f"sem heartbeat há {silencio:.1f} s")
            if caidos:
                self.mostrar_tabela()

    # ------------------------------------------------------------------
    # Auxiliares
    # ------------------------------------------------------------------

    @staticmethod
    def validar_registro(id_no, vizinhos):
        """Devolve um texto descrevendo o problema, ou None se estiver tudo certo."""
        if not isinstance(id_no, str) or not id_no.strip():
            return "campo 'id' ausente ou vazio"
        if not isinstance(vizinhos, dict):
            return "campo 'vizinhos' deve ser um objeto {id: custo}"
        for vizinho, custo in vizinhos.items():
            if vizinho == id_no:
                return "um nó não pode ser vizinho de si mesmo"
            if not isinstance(custo, (int, float)) or isinstance(custo, bool) or custo <= 0:
                return f"custo inválido para o vizinho '{vizinho}'"
        return None

    @staticmethod
    def responder(conexao, msg):
        """Envia sem deixar uma falha de envio derrubar a thread."""
        try:
            p.enviar_tcp(conexao, msg)
        except OSError:
            pass

    def foto(self):
        """
        Copia, sob o lock, o que o cálculo de rotas precisa. O cálculo roda
        depois, fora do lock: as outras threads (heartbeat, monitor, clientes)
        não ficam esperando o Dijkstra terminar.
        """
        with self.lock:
            nos = {i: {"status": n["status"], "vizinhos": dict(n["vizinhos"])}
                   for i, n in self.nos.items()}
            derrubados = set(self.enlaces_derrubados)
        return nos, derrubados

    def grafo_atual(self):
        return montar_grafo(*self.foto())

    def caminho(self, origem, destino):
        """(custo, [nós]) pelo estado atual da rede, ou None se não há rota."""
        return dijkstra(self.grafo_atual(), origem, destino)

    def mostrar_tabela(self):
        nos, derrubados = self.foto()
        linhas = [f"  {i}: {n['status']}" for i, n in sorted(nos.items())]
        enlaces = listar_enlaces(montar_grafo(nos, derrubados))
        texto_enlaces = ", ".join(f"{a}-{b}({c})" for a, b, c in enlaces) or "(nenhum)"
        log("Tabela de nós:\n" + ("\n".join(linhas) if linhas else "  (vazia)")
            + f"\n  Enlaces utilizáveis: {texto_enlaces}")


if __name__ == "__main__":
    Servidor().iniciar()
