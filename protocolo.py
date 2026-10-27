"""
protocolo.py — Contrato de comunicação compartilhado por servidor, nós e operador.

Responsabilidades deste módulo:
  1. Definir as constantes do projeto (endereço, portas, tempos).
  2. Definir os tipos de mensagem.
  3. Enviar mensagens pelo TCP tratando envios parciais (sem sendall).
  4. Receber mensagens do TCP separando-as num fluxo de bytes (enquadramento).
  5. Enviar e receber datagramas UDP.

Por que o enquadramento existe:
  O TCP entrega um FLUXO de bytes, não mensagens. Se o remetente faz dois
  send() seguidos, o destinatário pode recebê-los num único recv() (coladas),
  ou receber metade de uma mensagem agora e o resto depois (partida). O TCP
  só garante que os bytes chegam inteiros e na ordem. Cabe à aplicação marcar
  onde cada mensagem termina. Aqui usamos o caractere '\n' como delimitador.

  Isso é seguro com JSON porque json.dumps nunca emite uma quebra de linha
  literal: um '\n' dentro de uma string vira os dois caracteres '\' e 'n'.
  Logo, o único '\n' real no fluxo é o que nós colocamos no fim da mensagem.
"""

import json
import math

# ---------------------------------------------------------------------------
# Constantes do projeto
# ---------------------------------------------------------------------------

HOST = "127.0.0.1"
PORTA_TCP = 5000            # registro dos nós, comandos e operador
PORTA_UDP = 5001            # heartbeats dos nós

INTERVALO_HEARTBEAT = 2.0   # segundos entre heartbeats de um nó
TEMPO_LIMITE = 6.0          # sem heartbeat por esse tempo -> nó inativo (3x o intervalo)
INTERVALO_MONITOR = 1.0     # frequência com que o servidor verifica os tempos limite

DELIMITADOR = b"\n"
TAM_LEITURA = 4096          # quantos bytes pedir a cada recv()
TAM_MAX_MENSAGEM = 64 * 1024  # proteção: uma mensagem TCP maior que isso é erro
TAM_MAX_DATAGRAMA = 65507   # maior payload UDP possível sobre IPv4

# ---------------------------------------------------------------------------
# Tipos de mensagem (campo "tipo" de todo JSON trocado)
# ---------------------------------------------------------------------------

REGISTRO = "REGISTRO"           # nó -> servidor (TCP): quem sou e meus vizinhos
DESCONECTAR = "DESCONECTAR"     # nó -> servidor (TCP): saída limpa
HEARTBEAT = "HEARTBEAT"         # nó -> servidor (UDP): estou vivo + métricas
OPERADOR = "OPERADOR"           # operador -> servidor (TCP): identifica a conexão
COMANDO = "COMANDO"             # operador -> servidor (TCP): pedido
RESPOSTA = "RESPOSTA"           # servidor -> cliente (TCP): resultado de um pedido
EVENTO = "EVENTO"               # servidor -> operador (TCP): aviso espontâneo
ERRO = "ERRO"                   # servidor -> cliente (TCP): pedido recusado

# Comandos do operador (campo "comando" de uma mensagem COMANDO; os
# argumentos vão numa lista de strings no campo "args").
TOPOLOGIA = "TOPOLOGIA"
STATUS = "STATUS"
CAMINHO = "CAMINHO"
DERRUBAR = "DERRUBAR"
RESTAURAR = "RESTAURAR"
ASSINAR = "ASSINAR"

# Forma de uso de cada comando: o servidor cita nos erros, o operador na ajuda.
USO = {
    TOPOLOGIA: "TOPOLOGIA",
    STATUS: "STATUS [id]",
    CAMINHO: "CAMINHO <origem> <destino>",
    DERRUBAR: "DERRUBAR <nó> <nó>",
    RESTAURAR: "RESTAURAR <nó> <nó>",
    ASSINAR: "ASSINAR",
}

# Tipos de evento (campo "evento" de uma mensagem EVENTO)
EVENTO_NO = "NO"            # um nó mudou de estado
EVENTO_ENLACE = "ENLACE"    # um enlace foi derrubado ou restaurado


class ErroProtocolo(Exception):
    """Dados recebidos não seguem o protocolo (JSON inválido, mensagem gigante...)."""


def criar_mensagem(tipo, **campos):
    """Monta o dicionário de uma mensagem. Ex.: criar_mensagem(ERRO, motivo='id duplicado')."""
    return {"tipo": tipo, **campos}


def id_valido(texto):
    """
    Um id de nó é um texto não vazio e sem espaços. Sem espaços porque o
    operador separa os argumentos dos comandos por espaço: um nó "A B" nunca
    poderia ser citado num CAMINHO.
    """
    return isinstance(texto, str) and texto != "" and not any(c.isspace() for c in texto)


def custo_valido(custo):
    """Número positivo e finito (bool é subclasse de int em Python, por isso a exclusão)."""
    return (isinstance(custo, (int, float)) and not isinstance(custo, bool)
            and math.isfinite(custo) and custo > 0)


def _rejeitar_constante(nome):
    # O json do Python aceita NaN, Infinity e -Infinity por padrão, mas eles
    # não fazem parte do JSON padrão (RFC 8259). Um custo NaN passaria por
    # "custo > 0" (toda comparação com NaN é falsa) e estragaria o Dijkstra.
    raise ErroProtocolo(f"valor não permitido em JSON: {nome}")


def _serializar(msg):
    """dict -> bytes JSON em UTF-8, sem espaços desnecessários."""
    # allow_nan=False: também nunca ENVIAMOS NaN/Infinity.
    return json.dumps(msg, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _desserializar(dados):
    """bytes JSON -> dict. Converte falhas em ErroProtocolo."""
    try:
        msg = json.loads(dados.decode("utf-8"), parse_constant=_rejeitar_constante)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ErroProtocolo(f"mensagem malformada: {e}") from e
    if not isinstance(msg, dict) or "tipo" not in msg:
        raise ErroProtocolo("mensagem sem campo 'tipo'")
    return msg


# ---------------------------------------------------------------------------
# TCP
# ---------------------------------------------------------------------------

def enviar_tcp(sock, msg):
    """
    Envia uma mensagem completa pelo socket TCP.

    send() devolve QUANTOS bytes o sistema operacional aceitou copiar para o
    buffer de envio do kernel, e esse número pode ser menor que o pedido (buffer
    cheio, interrupção por sinal). Por isso repetimos send() sobre o que sobrou
    até tudo ter sido aceito. É exatamente o que sendall() faria escondido.
    """
    dados = _serializar(msg) + DELIMITADOR
    total_enviado = 0
    while total_enviado < len(dados):
        enviados = sock.send(dados[total_enviado:])
        if enviados == 0:
            # O kernel não aceitou nenhum byte: a conexão foi encerrada.
            raise ConnectionError("conexão encerrada durante o envio")
        total_enviado += enviados


class ReceptorTCP:
    """
    Transforma o fluxo de bytes de um socket TCP numa sequência de mensagens.

    Mantém um buffer com os bytes já recebidos e ainda não consumidos. A cada
    pedido de mensagem:
      - se o buffer já contém um delimitador, recorta a primeira mensagem;
      - senão, chama recv() e acrescenta o que chegar, repetindo até achar
        um delimitador.

    Um único recv() pode trazer várias mensagens (ficam no buffer para as
    próximas chamadas) ou um pedaço de uma (esperamos o resto).

    Cada conexão precisa do SEU receptor: o buffer pertence àquele fluxo.
    """

    def __init__(self, sock):
        self.sock = sock
        self.buffer = bytearray()

    def receber(self):
        """
        Devolve a próxima mensagem (dict), ou None se o outro lado fechou a
        conexão de forma ordenada.

        recv() bloqueia até haver pelo menos 1 byte. Quando ele devolve b''
        (zero bytes), significa que o outro lado fechou a conexão (FIN): não
        há mais dados vindo, nunca. Uma queda abrupta (RST) aparece como
        exceção ConnectionResetError, que deixamos subir para quem chamou.
        """
        while True:
            posicao = self.buffer.find(DELIMITADOR)
            if posicao != -1:
                bruto = bytes(self.buffer[:posicao])
                del self.buffer[: posicao + len(DELIMITADOR)]
                return _desserializar(bruto)

            if len(self.buffer) > TAM_MAX_MENSAGEM:
                # Sem delimitador e já grande demais: o outro lado não segue
                # o protocolo. Sem esse limite, o buffer cresceria sem fim.
                raise ErroProtocolo("mensagem excede o tamanho máximo")

            pedaco = self.sock.recv(TAM_LEITURA)
            if not pedaco:
                return None
            self.buffer.extend(pedaco)


# ---------------------------------------------------------------------------
# UDP
# ---------------------------------------------------------------------------

def enviar_udp(sock, msg, destino):
    """
    Envia uma mensagem como um único datagrama UDP para destino = (host, porta).

    No UDP não há enquadramento a fazer: cada sendto() vira exatamente um
    datagrama, e cada recvfrom() devolve exatamente um datagrama inteiro.
    Também não há envio parcial: ou o datagrama inteiro sai, ou ocorre erro.
    O que o UDP NÃO garante é a entrega: o datagrama pode se perder ou chegar
    fora de ordem, e ninguém avisa. Por isso os heartbeats levam número de
    sequência.
    """
    dados = _serializar(msg)
    if len(dados) > TAM_MAX_DATAGRAMA:
        raise ErroProtocolo("mensagem grande demais para um datagrama")
    sock.sendto(dados, destino)


def receber_udp(sock):
    """Espera um datagrama e devolve (mensagem, endereço_do_remetente)."""
    dados, endereco = sock.recvfrom(TAM_MAX_DATAGRAMA)
    return _desserializar(dados), endereco
