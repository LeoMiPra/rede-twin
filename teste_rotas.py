"""
teste_rotas.py — Verifica a montagem do grafo e o Dijkstra do servidor.py.

As funções de rota são puras: recebem uma cópia da tabela de nós e do
conjunto de enlaces derrubados. Por isso dá para testá-las sem sockets,
montando a tabela à mão.

Topologia usada na maioria dos casos:

        A ---4--- B
        |         |
        1         2
        |         |
        C ---5--- D
         \\       /
          2     1.5
           \\   /
             E

Rode com:  python teste_rotas.py
"""

import servidor as s


def caso(nome):
    print(f"[ok] {nome}")


def rede(inativos=(), **vizinhos):
    """Monta a tabela {id: {"status", "vizinhos"}} que montar_grafo espera."""
    return {i: {"status": s.INATIVO if i in inativos else s.ATIVO, "vizinhos": v}
            for i, v in vizinhos.items()}


def exemplo(inativos=()):
    return rede(
        inativos,
        A={"B": 4, "C": 1},
        B={"A": 4, "D": 2},
        C={"A": 1, "D": 5, "E": 2},
        D={"B": 2, "C": 5, "E": 1.5},
        E={"C": 2, "D": 1.5},
    )


def teste_menor_caminho():
    # Direto A-B custa 4; A-C-E-D-B custa 1+2+1.5+2 = 6.5. Ganha o direto.
    grafo = s.montar_grafo(exemplo(), set())
    assert s.dijkstra(grafo, "A", "B") == (4, ["A", "B"])
    # A-D: A-B-D = 6, A-C-D = 6, A-C-E-D = 4.5.
    assert s.dijkstra(grafo, "A", "D") == (4.5, ["A", "C", "E", "D"])
    caso("escolhe o caminho de menor custo, mesmo com mais saltos")


def teste_mesmo_no():
    grafo = s.montar_grafo(exemplo(), set())
    assert s.dijkstra(grafo, "C", "C") == (0, ["C"])
    caso("origem igual ao destino")


def teste_no_inativo_desvia():
    # Com E fora, A-D passa a ser 6 (empate A-B-D e A-C-D).
    grafo = s.montar_grafo(exemplo(inativos={"E"}), set())
    custo, caminho = s.dijkstra(grafo, "A", "D")
    assert custo == 6 and "E" not in caminho
    assert "E" not in grafo
    caso("nó INATIVO sai do grafo e a rota desvia")


def teste_ponta_inativa_sem_rota():
    grafo = s.montar_grafo(exemplo(inativos={"D"}), set())
    assert s.dijkstra(grafo, "A", "D") is None
    assert s.dijkstra(grafo, "D", "A") is None
    caso("origem ou destino inativo: sem rota")


def teste_enlace_derrubado():
    grafo = s.montar_grafo(exemplo(), {s.enlace("E", "C")})   # ordem não importa
    assert "E" not in grafo["C"] and "C" not in grafo["E"]
    # Sem C-E, empatam A-C-D-E (1+5+1.5) e A-B-D-E (4+2+1.5).
    custo, caminho = s.dijkstra(grafo, "A", "E")
    assert custo == 7.5 and caminho in (["A", "C", "D", "E"], ["A", "B", "D", "E"])
    caso("enlace derrubado some nas duas direções")


def teste_rede_partida():
    # Sem A-B e A-C, A fica isolado.
    grafo = s.montar_grafo(exemplo(), {s.enlace("A", "B"), s.enlace("A", "C")})
    assert s.dijkstra(grafo, "A", "E") is None
    caso("rede partida: sem rota")


def teste_declaracao_de_um_lado():
    # Só A declara o enlace; B não menciona A.
    grafo = s.montar_grafo(rede(A={"B": 3}, B={}), set())
    assert grafo == {"A": {"B": 3}, "B": {"A": 3}}
    caso("enlace declarado por uma ponta só vale nas duas direções")


def teste_custos_divergentes():
    grafo = s.montar_grafo(rede(A={"B": 3}, B={"A": 7}), set())
    assert grafo["A"]["B"] == grafo["B"]["A"] == 7
    caso("custos divergentes: vale o maior")


def teste_vizinho_nao_registrado():
    grafo = s.montar_grafo(rede(A={"B": 1, "Z": 1}, B={}), set())
    assert "Z" not in grafo and "Z" not in grafo["A"]
    caso("vizinho que nunca se registrou não entra no grafo")


def teste_listar_enlaces():
    grafo = s.montar_grafo(rede(A={"B": 3, "C": 1}, B={"A": 3}, C={}), set())
    assert s.listar_enlaces(grafo) == [("A", "B", 3), ("A", "C", 1)]
    caso("cada enlace listado uma vez")


if __name__ == "__main__":
    teste_menor_caminho()
    teste_mesmo_no()
    teste_no_inativo_desvia()
    teste_ponta_inativa_sem_rota()
    teste_enlace_derrubado()
    teste_rede_partida()
    teste_declaracao_de_um_lado()
    teste_custos_divergentes()
    teste_vizinho_nao_registrado()
    teste_listar_enlaces()
    print("\nTodos os testes passaram.")
