"""
servidor.py — Servidor central do mini gêmeo digital de rede.

Etapa 2: aceita conexões TCP, atende cada cliente numa thread própria e
mantém a tabela de nós registrados, protegida por um lock.

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

import socket
import threading
import time

import protocolo as p

# Estados possíveis de um nó na tabela
ATIVO = "ATIVO"
INATIVO = "INATIVO"            # falhou: conexão caiu (ou, na etapa 4, heartbeat parou)
DESCONECTADO = "DESCONECTADO"  # saiu de forma limpa, avisando o servidor

_lock_log = threading.Lock()


def log(texto):
    """print com horário. O lock evita que duas threads misturem linhas."""
    with _lock_log:
        print(f"[{time.strftime('%H:%M:%S')}] {texto}", flush=True)


class Servidor:
    def __init__(self, host=p.HOST, porta_tcp=p.PORTA_TCP):
        self.host = host
        self.porta_tcp = porta_tcp

        # Estado compartilhado entre todas as threads.
        # Regra: TODO acesso a self.nos acontece com self.lock adquirido.
        # Sem isso, duas threads poderiam ver "id livre" ao mesmo tempo e
        # registrar o mesmo id duas vezes (condição de corrida clássica:
        # verificar-e-depois-agir sem atomicidade).
        self.lock = threading.Lock()
        self.nos = {}   # id -> {"sock", "endereco", "status", "vizinhos", "desde"}

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
            else:
                self.responder(conexao, p.criar_mensagem(
                    p.ERRO, motivo="primeira mensagem deve ser REGISTRO"))
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
            if existente and existente["status"] == ATIVO:
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
                }
        # A resposta vai pela rede FORA do lock: um send() lento não pode
        # travar as outras threads que querem consultar a tabela.
        if recusado:
            self.responder(conexao, p.criar_mensagem(
                p.ERRO, motivo=f"id '{id_no}' já está ativo"))
            log(f"Registro recusado: id '{id_no}' duplicado")
            return

        self.responder(conexao, p.criar_mensagem(p.RESPOSTA, ok=True, id=id_no))
        log(f"Nó {id_no} registrado (vizinhos: {vizinhos})")

        # --- 3. Ouvir o nó até ele sair ou cair -------------------------
        motivo_fim = INATIVO
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
                    log(f"Nó {id_no} saiu de forma limpa -> {DESCONECTADO}")
                    break
                log(f"Nó {id_no}: mensagem ignorada nesta etapa: {msg}")
        except OSError:
            # RST: queda abrupta. Mesmo efeito que o FIN para nós.
            log(f"Nó {id_no}: conexão TCP resetada -> {INATIVO}")
        finally:
            with self.lock:
                atual = self.nos.get(id_no)
                # Só altera se a entrada ainda for desta conexão. Se o nó já
                # se reconectou por outro socket, não podemos derrubá-lo.
                if atual and atual["sock"] is conexao:
                    atual["status"] = motivo_fim
                    atual["sock"] = None
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

    def mostrar_tabela(self):
        with self.lock:
            linhas = [f"  {i}: {n['status']}" for i, n in sorted(self.nos.items())]
        log("Tabela de nós:\n" + ("\n".join(linhas) if linhas else "  (vazia)"))


if __name__ == "__main__":
    Servidor().iniciar()
