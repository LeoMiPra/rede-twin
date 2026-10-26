"""
no.py — Um nó da rede simulada.

Etapa 3: o nó se registra no servidor por TCP e mantém essa conexão aberta,
escutando o que o servidor mandar. A própria conexão serve de sinal de vida:
enquanto ela existir, o servidor sabe que o processo do nó existe.

Etapa 4: uma segunda thread envia um heartbeat UDP a cada INTERVALO_HEARTBEAT.
Isso prova mais que a conexão TCP: prova que o programa está executando.
Um processo suspenso (Ctrl+Z) mantém a conexão aberta, mas para de enviar.

Ciclo de vida:
  1. Lê id e vizinhos da linha de comando.
  2. connect(): o kernel faz o handshake de três vias (SYN, SYN-ACK, ACK).
  3. Envia REGISTRO e espera a resposta: RESPOSTA ok=True ou ERRO.
  4. Inicia a thread de heartbeat.
  5. Fica em recv() esperando mensagens do servidor (comandos, nas próximas
     etapas). recv() bloqueado não gasta CPU: o kernel acorda o processo
     quando chegam bytes.
  6. Saída limpa (Ctrl+C ou kill): para o heartbeat, envia DESCONECTAR, fecha o lado de envio
     e espera o servidor fechar o dele.

Rode com:  python no.py A B:3 C:1
           (nó A, vizinho B com custo 3 e vizinho C com custo 1)
"""

import argparse
import signal
import socket
import sys
import threading
import time

import protocolo as p

# Quanto esperar o servidor fechar a conexão depois do DESCONECTAR.
TEMPO_ESPERA_FECHAMENTO = 2.0


_lock_log = threading.Lock()


def log(id_no, texto):
    """print com horário. O lock evita que as duas threads misturem linhas."""
    with _lock_log:
        print(f"[{time.strftime('%H:%M:%S')}] [{id_no}] {texto}", flush=True)


# ----------------------------------------------------------------------
# Argumentos
# ----------------------------------------------------------------------

def vizinho_custo(texto):
    """Converte 'B:3' em ('B', 3). Usado como type= do argparse."""
    nome, sep, custo = texto.partition(":")
    if not sep or not nome:
        raise argparse.ArgumentTypeError(f"'{texto}' não está no formato VIZINHO:CUSTO")
    try:
        valor = int(custo)
    except ValueError:
        try:
            valor = float(custo)
        except ValueError:
            raise argparse.ArgumentTypeError(f"custo '{custo}' não é um número") from None
    if valor <= 0:
        raise argparse.ArgumentTypeError(f"custo de '{nome}' deve ser positivo")
    return nome, valor


def ler_argumentos():
    parser = argparse.ArgumentParser(description="Nó da rede simulada.")
    parser.add_argument("id", help="identificador do nó (ex.: A)")
    parser.add_argument("vizinhos", nargs="*", type=vizinho_custo,
                        metavar="VIZINHO:CUSTO", help="enlaces do nó (ex.: B:3 C:1)")
    args = parser.parse_args()

    vizinhos = {}
    for nome, custo in args.vizinhos:
        if nome == args.id:
            parser.error("um nó não pode ser vizinho de si mesmo")
        if nome in vizinhos:
            parser.error(f"vizinho '{nome}' repetido")
        vizinhos[nome] = custo
    return args.id, vizinhos


# ----------------------------------------------------------------------
# Rede
# ----------------------------------------------------------------------

def conectar_e_registrar(id_no, vizinhos):
    """
    Abre a conexão TCP e registra o nó. Devolve (socket, receptor) se o
    servidor aceitou; encerra o programa caso contrário.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # Não chamamos bind(): o kernel escolhe uma porta efêmera de origem.
        # Se ninguém escuta na 5000, o kernel do servidor responde ao SYN com
        # RST e connect() falha com ConnectionRefusedError.
        sock.connect((p.HOST, p.PORTA_TCP))
    except ConnectionRefusedError:
        sock.close()
        log(id_no, f"servidor não está ouvindo em {p.HOST}:{p.PORTA_TCP}")
        sys.exit(1)

    # O receptor guarda o buffer DESTE fluxo; usamos o mesmo até o fim, senão
    # bytes já lidos do kernel (e ainda não consumidos) se perderiam.
    receptor = p.ReceptorTCP(sock)
    try:
        p.enviar_tcp(sock, p.criar_mensagem(p.REGISTRO, id=id_no, vizinhos=vizinhos))
        resposta = receptor.receber()
    except (OSError, p.ErroProtocolo) as e:
        sock.close()
        log(id_no, f"falha durante o registro: {e}")
        sys.exit(1)

    if resposta is None:
        # recv() devolveu 0 bytes: o servidor fechou sem responder.
        sock.close()
        log(id_no, "servidor fechou a conexão sem responder ao registro")
        sys.exit(1)
    if resposta["tipo"] == p.ERRO:
        # Depois de um ERRO o servidor fecha a conexão; só nos resta sair.
        sock.close()
        log(id_no, f"registro recusado: {resposta.get('motivo')}")
        sys.exit(1)
    if resposta["tipo"] != p.RESPOSTA or not resposta.get("ok"):
        sock.close()
        log(id_no, f"resposta inesperada ao registro: {resposta}")
        sys.exit(1)

    local = sock.getsockname()
    log(id_no, f"registrado no servidor (porta local {local[1]}, vizinhos {vizinhos})")
    return sock, receptor


def enviar_heartbeats(id_no, parar):
    """
    Thread de heartbeat: um datagrama a cada INTERVALO_HEARTBEAT segundos.

    Por que UDP: o heartbeat é periódico e substituível. Se um se perder, o
    próximo vem em 2 s; retransmitir um aviso velho, como o TCP faria, só
    atrasaria a informação. Também não há conexão a manter: basta sendto().

    O número de sequência permite ao servidor descartar datagramas fora de
    ordem e contar os perdidos.
    """
    # Socket próprio, separado do TCP. Sem bind(): no primeiro sendto() o
    # kernel atribui uma porta efêmera de origem.
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    destino = (p.HOST, p.PORTA_UDP)
    seq = 0
    try:
        while True:
            seq += 1
            try:
                p.enviar_udp(udp, p.criar_mensagem(p.HEARTBEAT, id=id_no, seq=seq), destino)
            except OSError as e:
                # sendto() quase nunca falha: o datagrama é entregue ao kernel
                # e pronto. Se ninguém ouvir na porta UDP, o ICMP "port
                # unreachable" não é relatado num socket UDP não conectado:
                # o datagrama some em silêncio. Por isso só registramos.
                log(id_no, f"falha ao enviar heartbeat {seq}: {e}")
            # wait() em vez de sleep(): dorme o intervalo, mas acorda na hora
            # se a saída limpa sinalizar o evento.
            if parar.wait(p.INTERVALO_HEARTBEAT):
                break
    finally:
        udp.close()


def escutar(id_no, receptor):
    """
    Lê mensagens do servidor até a conexão acabar. Devolve True se terminou
    porque o servidor fechou (o que é uma falha do ponto de vista do nó).
    """
    while True:
        msg = receptor.receber()
        if msg is None:
            # FIN do servidor: o processo dele terminou ou fechou o socket.
            return True
        # Ainda não há comandos para o nó; nas próximas etapas eles chegam aqui.
        log(id_no, f"mensagem do servidor: {msg}")


def desconectar(id_no, sock, receptor):
    """
    Saída limpa em três passos:
      1. DESCONECTAR avisa a intenção: o servidor marca DESCONECTADO, não INATIVO.
      2. shutdown(SHUT_WR) manda FIN: "não envio mais nada", mas ainda leio.
      3. Lemos até o servidor fechar o lado dele (recv() devolve b'').

    Por que não só close()? Se o nosso socket fechar com bytes não lidos no
    buffer de recepção, o kernel envia RST em vez de FIN, e um RST pode fazer
    o outro lado descartar dados que ainda não leu, inclusive o DESCONECTAR.
    Esperar o fechamento do servidor garante que ele processou a mensagem.
    """
    try:
        p.enviar_tcp(sock, p.criar_mensagem(p.DESCONECTAR, id=id_no))
        sock.shutdown(socket.SHUT_WR)
        # Tempo limite: se o servidor travar, não ficamos presos para sempre.
        sock.settimeout(TEMPO_ESPERA_FECHAMENTO)
        while receptor.receber() is not None:
            pass
        log(id_no, "desconectado de forma limpa")
    except (OSError, p.ErroProtocolo) as e:
        # socket.timeout é subclasse de OSError.
        log(id_no, f"saída sem confirmação do servidor: {e}")


def ao_receber_sigterm(_sinal, _quadro):
    # kill <pid> manda SIGTERM, que por padrão mata o processo sem rodar
    # nenhum código Python. Convertendo em exceção, `kill` também passa pela
    # saída limpa. kill -9 (SIGKILL) não pode ser capturado: o kernel mata o
    # processo e fecha os sockets sozinho, e o servidor vê só o FIN.
    raise KeyboardInterrupt


def main():
    id_no, vizinhos = ler_argumentos()
    signal.signal(signal.SIGTERM, ao_receber_sigterm)

    sock, receptor = conectar_e_registrar(id_no, vizinhos)

    # Só começa depois do registro aceito: antes disso o servidor ignoraria
    # os heartbeats, pois o id ainda não está na tabela.
    # daemon=True: se o nó sair por erro, a thread não segura o processo.
    parar_heartbeat = threading.Event()
    threading.Thread(target=enviar_heartbeats, args=(id_no, parar_heartbeat),
                     daemon=True).start()
    try:
        # O Ctrl+C gera SIGINT, entregue à thread principal. Se ela estiver
        # bloqueada em recv(), a chamada é interrompida e o Python levanta
        # KeyboardInterrupt aqui.
        servidor_caiu = escutar(id_no, receptor)
        if servidor_caiu:
            log(id_no, "o servidor encerrou a conexão; saindo")
            sys.exit(1)
    except KeyboardInterrupt:
        log(id_no, "encerrando (sinal recebido)")
        parar_heartbeat.set()
        desconectar(id_no, sock, receptor)
    except (OSError, p.ErroProtocolo) as e:
        # RST do servidor (ConnectionResetError) ou dados fora do protocolo.
        log(id_no, f"conexão com o servidor perdida: {e}")
        sys.exit(1)
    finally:
        sock.close()


if __name__ == "__main__":
    main()
