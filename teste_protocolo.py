"""
teste_protocolo.py — Verifica o enquadramento do protocolo.py sem precisar de rede.

socket.socketpair() cria dois sockets já conectados entre si, dentro do mesmo
processo. Os bytes escritos num saem no outro, como num fluxo TCP. Assim
podemos forçar os casos difíceis (mensagens coladas, partidas) de propósito.

Rode com:  python teste_protocolo.py
"""

import socket

import protocolo as p


def caso(nome):
    print(f"[ok] {nome}")


def teste_mensagem_simples():
    a, b = socket.socketpair()
    receptor = p.ReceptorTCP(b)
    p.enviar_tcp(a, p.criar_mensagem(p.REGISTRO, id="A"))
    assert receptor.receber() == {"tipo": "REGISTRO", "id": "A"}
    a.close(); b.close()
    caso("mensagem simples")


def teste_mensagens_coladas():
    # Duas mensagens enviadas num único send(): chegam juntas no mesmo recv().
    a, b = socket.socketpair()
    receptor = p.ReceptorTCP(b)
    colado = b'{"tipo":"HEARTBEAT","seq":1}\n{"tipo":"HEARTBEAT","seq":2}\n'
    a.send(colado)
    assert receptor.receber()["seq"] == 1
    assert receptor.receber()["seq"] == 2
    a.close(); b.close()
    caso("mensagens coladas são separadas")


def teste_mensagem_partida():
    # Uma mensagem enviada byte a byte: o receptor precisa juntar os pedaços.
    a, b = socket.socketpair()
    receptor = p.ReceptorTCP(b)
    for byte in b'{"tipo":"COMANDO","texto":"CAMINHO A E"}\n':
        a.send(bytes([byte]))
    assert receptor.receber()["texto"] == "CAMINHO A E"
    a.close(); b.close()
    caso("mensagem partida byte a byte é reconstruída")


def teste_acento_partido():
    # 'ç' ocupa 2 bytes em UTF-8. Se a decodificação acontecesse a cada recv(),
    # um corte no meio do caractere quebraria. Decodificamos só a mensagem inteira.
    a, b = socket.socketpair()
    receptor = p.ReceptorTCP(b)
    dados = '{"tipo":"ERRO","motivo":"ação"}\n'.encode("utf-8")
    corte = dados.index("ç".encode("utf-8")) + 1   # corta entre os 2 bytes do ç
    a.send(dados[:corte])
    a.send(dados[corte:])
    assert receptor.receber()["motivo"] == "ação"
    a.close(); b.close()
    caso("caractere multibyte cortado ao meio")


def teste_quebra_de_linha_no_conteudo():
    # Um '\n' dentro de uma string não pode ser confundido com o delimitador.
    a, b = socket.socketpair()
    receptor = p.ReceptorTCP(b)
    p.enviar_tcp(a, p.criar_mensagem(p.RESPOSTA, texto="linha 1\nlinha 2"))
    assert receptor.receber()["texto"] == "linha 1\nlinha 2"
    a.close(); b.close()
    caso("'\\n' dentro do conteúdo não quebra o enquadramento")


def teste_fechamento():
    # Depois de consumir o que havia, o fechamento do outro lado vira None.
    a, b = socket.socketpair()
    receptor = p.ReceptorTCP(b)
    p.enviar_tcp(a, p.criar_mensagem(p.DESCONECTAR, id="A"))
    a.close()
    assert receptor.receber()["tipo"] == "DESCONECTAR"
    assert receptor.receber() is None
    b.close()
    caso("conexão fechada devolve None")


def teste_malformada():
    a, b = socket.socketpair()
    receptor = p.ReceptorTCP(b)
    a.send(b"isso nao e json\n")
    try:
        receptor.receber()
        raise AssertionError("deveria ter falhado")
    except p.ErroProtocolo:
        pass
    a.close(); b.close()
    caso("mensagem malformada gera ErroProtocolo")


def teste_nan_recusado():
    # NaN/Infinity não são JSON padrão; o protocolo recusa nos dois sentidos.
    a, b = socket.socketpair()
    receptor = p.ReceptorTCP(b)
    a.send(b'{"tipo":"REGISTRO","vizinhos":{"B":NaN}}\n')
    try:
        receptor.receber()
        raise AssertionError("deveria ter falhado")
    except p.ErroProtocolo:
        pass
    try:
        p.enviar_tcp(a, p.criar_mensagem(p.REGISTRO, custo=float("inf")))
        raise AssertionError("deveria ter falhado")
    except ValueError:
        pass
    a.close(); b.close()
    caso("NaN e Infinity recusados")


def teste_validacoes():
    assert p.id_valido("A") and p.id_valido("roteador-1")
    assert not p.id_valido("") and not p.id_valido("A B") and not p.id_valido(["A"])
    assert p.custo_valido(1) and p.custo_valido(0.5)
    for ruim in (0, -1, True, "3", float("nan"), float("inf")):
        assert not p.custo_valido(ruim), ruim
    caso("validação de id e custo")


def teste_udp():
    servidor = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    servidor.bind((p.HOST, 0))          # porta 0: o sistema escolhe uma livre
    cliente = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    p.enviar_udp(cliente, p.criar_mensagem(p.HEARTBEAT, id="B", seq=7), servidor.getsockname())
    msg, endereco = p.receber_udp(servidor)
    assert msg == {"tipo": "HEARTBEAT", "id": "B", "seq": 7}
    # O cliente nunca chamou bind(): no primeiro sendto() o sistema o liga a
    # 0.0.0.0 (todas as interfaces) numa porta efêmera. Por isso getsockname()
    # mostra '0.0.0.0', enquanto o servidor vê o IP real de origem, 127.0.0.1.
    # O que identifica o remetente aqui é a porta.
    assert endereco[1] == cliente.getsockname()[1]
    servidor.close(); cliente.close()
    caso("ida e volta por UDP")


if __name__ == "__main__":
    teste_mensagem_simples()
    teste_mensagens_coladas()
    teste_mensagem_partida()
    teste_acento_partido()
    teste_quebra_de_linha_no_conteudo()
    teste_fechamento()
    teste_malformada()
    teste_nan_recusado()
    teste_validacoes()
    teste_udp()
    print("\nTodos os testes passaram.")
