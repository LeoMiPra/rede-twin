"""
operador.py — Terminal do operador da rede simulada.

Conecta-se à mesma porta TCP dos nós e se identifica com OPERADOR. Depois
disso, cada linha digitada vira um COMANDO e o servidor devolve uma RESPOSTA
ou um ERRO. Com ASSINAR, o servidor passa também a empurrar EVENTOs (nó
caiu, voltou, enlace derrubado...) sem ser perguntado.

Duas threads:
  - principal: lê o teclado com input() e envia os comandos;
  - receptora: lê o socket. RESPOSTA e ERRO vão para uma fila, onde a
    principal os espera; EVENTO é impresso na hora.

Por que duas: input() bloqueia esperando o teclado e recv() bloqueia
esperando a rede. Uma thread só ficaria presa numa das esperas; um evento
não apareceria enquanto o operador não apertasse Enter.

Só a thread principal escreve no socket e só a receptora lê. Um socket TCP
aguenta um leitor e um escritor simultâneos sem conflito, porque os dois
sentidos do fluxo são independentes.

Rode com:  python operador.py
"""

import os
import queue
import signal
import socket
import sys
import threading

import protocolo as p

# Quanto esperar a resposta de um comando. Se estourar, desistimos da
# conexão: uma resposta atrasada chegaria depois e seria confundida com a
# resposta do comando seguinte.
TEMPO_RESPOSTA = 5.0
PROMPT = "> "

_lock_tela = threading.Lock()


def mostrar(texto):
    with _lock_tela:
        print(texto, flush=True)


# ----------------------------------------------------------------------
# Formatação
# ----------------------------------------------------------------------

def num(valor):
    """4 -> '4', 4.5 -> '4.5' (sem '.0' sobrando)."""
    return f"{valor:g}"


def formatar_resposta(comando, dados):
    if comando == p.TOPOLOGIA:
        linhas = ["Nós:"]
        linhas += [f"  {i:<6} {status}" for i, status in dados["nos"].items()] or ["  (nenhum)"]
        linhas.append("Enlaces:")
        linhas += [f"  {e['a']}-{e['b']:<6} custo {num(e['custo']):<5} {e['estado']}"
                   for e in dados["enlaces"]] or ["  (nenhum)"]
        return "\n".join(linhas)
    if comando == p.STATUS:
        if not dados["nos"]:
            return "(nenhum nó registrado)"
        linhas = []
        for i, n in dados["nos"].items():
            vizinhos = " ".join(f"{v}:{num(c)}" for v, c in n["vizinhos"].items()) or "-"
            linhas.append(
                f"{i:<6} {n['status']:<13} desde {n['desde']}  "
                f"último heartbeat há {num(n['silencio'])} s (seq {n['ultimo_seq']})  "
                f"vizinhos {vizinhos}  porta {n['porta_tcp']}")
        return "\n".join(linhas)
    if comando == p.CAMINHO:
        return f"{' -> '.join(dados['caminho'])}  (custo {num(dados['custo'])})"
    if comando in (p.DERRUBAR, p.RESTAURAR):
        return f"enlace {dados['a']}-{dados['b']} {dados['estado']}"
    if comando == p.ASSINAR:
        return "eventos assinados"
    return str(dados)


def formatar_evento(msg):
    if msg.get("evento") == p.EVENTO_NO:
        texto = f"nó {msg['id']} -> {msg['status']} ({msg['motivo']})"
    elif msg.get("evento") == p.EVENTO_ENLACE:
        texto = f"enlace {msg['a']}-{msg['b']} {msg['estado']}"
    else:
        texto = str(msg)
    return f"[evento {msg.get('hora', '')}] {texto}"


def mostrar_evento(msg):
    # O evento chega a qualquer momento, talvez com o prompt na tela. \r volta
    # ao início da linha e \033[K apaga o prompt; depois o reescrevemos.
    with _lock_tela:
        print(f"\r\033[K{formatar_evento(msg)}\n{PROMPT}", end="", flush=True)


def ajuda():
    comandos = list(p.USO.values()) + ["AJUDA", "SAIR (ou Ctrl+D / Ctrl+C)"]
    return "Comandos:\n" + "\n".join(f"  {c}" for c in comandos)


# ----------------------------------------------------------------------
# Rede
# ----------------------------------------------------------------------

def conectar():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((p.HOST, p.PORTA_TCP))
    except ConnectionRefusedError:
        sock.close()
        mostrar(f"servidor não está ouvindo em {p.HOST}:{p.PORTA_TCP}")
        sys.exit(1)

    # A identificação acontece antes de a thread receptora existir, então
    # aqui ainda podemos ler a resposta direto do socket.
    receptor = p.ReceptorTCP(sock)
    try:
        p.enviar_tcp(sock, p.criar_mensagem(p.OPERADOR))
        resposta = receptor.receber()
    except (OSError, p.ErroProtocolo) as e:
        sock.close()
        mostrar(f"falha ao se identificar: {e}")
        sys.exit(1)
    if resposta is None or resposta["tipo"] != p.RESPOSTA:
        sock.close()
        mostrar(f"servidor recusou o operador: {resposta}")
        sys.exit(1)
    return sock, receptor


def receber_em_loop(receptor, respostas, saindo, caiu):
    """Thread receptora: separa respostas (para a fila) de eventos (para a tela)."""
    motivo = "o servidor fechou a conexão"
    try:
        while True:
            msg = receptor.receber()
            if msg is None:
                break
            if msg["tipo"] == p.EVENTO:
                mostrar_evento(msg)
            else:
                respostas.put(msg)
    except (OSError, p.ErroProtocolo) as e:
        motivo = f"conexão perdida: {e}"

    if saindo.is_set():
        return   # fomos nós que encerramos; o fechamento era esperado
    caiu.set()
    mostrar(f"\n{motivo}")
    # A thread principal está parada em input() ou esperando a fila. Um sinal
    # de verdade (não só uma variável) faz o kernel interromper o read() do
    # teclado; o Python então levanta KeyboardInterrupt na thread principal.
    os.kill(os.getpid(), signal.SIGINT)


def encerrar(sock, receptora, saindo):
    """
    Saída limpa: shutdown(SHUT_WR) manda FIN. O recv() do servidor devolve
    b'', ele encerra a conexão do lado dele, e a nossa thread receptora vê o
    fechamento e termina. Só então fechamos o socket.
    """
    saindo.set()
    try:
        sock.shutdown(socket.SHUT_WR)
    except OSError:
        pass
    receptora.join(timeout=2)
    sock.close()


def main():
    sock, receptor = conectar()
    mostrar(f"Conectado ao servidor {p.HOST}:{p.PORTA_TCP}. Digite AJUDA para ver os comandos.")

    respostas = queue.Queue()
    saindo = threading.Event()   # nós pedimos para sair
    caiu = threading.Event()     # o servidor sumiu
    receptora = threading.Thread(target=receber_em_loop,
                                 args=(receptor, respostas, saindo, caiu), daemon=True)
    receptora.start()

    try:
        while True:
            try:
                linha = input(PROMPT)
            except EOFError:     # Ctrl+D
                mostrar("")
                break
            partes = linha.split()
            if not partes:
                continue
            comando, args = partes[0].upper(), partes[1:]
            if comando == "SAIR":
                break
            if comando == "AJUDA":
                mostrar(ajuda())
                continue

            p.enviar_tcp(sock, p.criar_mensagem(p.COMANDO, comando=comando, args=args))
            try:
                resposta = respostas.get(timeout=TEMPO_RESPOSTA)
            except queue.Empty:
                mostrar(f"o servidor não respondeu em {num(TEMPO_RESPOSTA)} s; encerrando")
                break
            if resposta["tipo"] == p.ERRO:
                mostrar(f"erro: {resposta.get('motivo')}")
            else:
                mostrar(formatar_resposta(resposta.get("comando"), resposta.get("dados")))
    except KeyboardInterrupt:
        if caiu.is_set():
            sock.close()
            sys.exit(1)
        mostrar("")
    except OSError as e:
        # send() falhou: a conexão já tinha caído.
        mostrar(f"conexão perdida: {e}")
        sock.close()
        sys.exit(1)

    encerrar(sock, receptora, saindo)


if __name__ == "__main__":
    main()
