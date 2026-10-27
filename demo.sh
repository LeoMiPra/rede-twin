#!/usr/bin/env bash
# demo.sh — Sobe o servidor e 5 nós de uma vez e mostra o log do servidor.
#
# Topologia:
#
#        A ---4--- B
#        |         |
#        1         2
#        |         |
#        C ---5--- D
#         \       /
#          2     1.5
#           \   /
#             E
#
# Uso:  ./demo.sh                  (Ctrl+C encerra tudo)
#       PYTHON=python ./demo.sh    (outro interpretador)
#
# Os logs de cada processo ficam em logs/.

set -u
cd "$(dirname "$0")"

PYTHON=${PYTHON:-python3}
LOGS=logs
mkdir -p "$LOGS"
rm -f "$LOGS"/*.log

SERVIDOR=""
declare -A NOS   # id -> pid

encerrar() {
    trap - INT TERM EXIT
    echo
    echo "Encerrando os nós..."
    for id in "${!NOS[@]}"; do
        # Um nó suspenso (kill -STOP) não executa nada, nem o tratador de
        # SIGTERM: o sinal fica pendente até o SIGCONT. Por isso o CONT antes.
        kill -CONT "${NOS[$id]}" 2>/dev/null
        # SIGTERM: o nó manda DESCONECTAR e sai de forma limpa.
        kill -TERM "${NOS[$id]}" 2>/dev/null
    done
    wait "${NOS[@]}" 2>/dev/null
    # O servidor por último, para ele registrar a saída de cada nó.
    if [ -n "$SERVIDOR" ]; then
        echo "Encerrando o servidor..."
        kill -TERM "$SERVIDOR" 2>/dev/null
        wait "$SERVIDOR" 2>/dev/null
    fi
    exit 0
}
trap encerrar INT TERM EXIT

# --- Servidor --------------------------------------------------------------
"$PYTHON" servidor.py > "$LOGS/servidor.log" 2>&1 &
SERVIDOR=$!

# Espera as duas portas estarem abertas (a linha do UDP sai depois da do TCP).
# Consultar o log evita abrir uma conexão de teste, que o servidor registraria.
for _ in $(seq 50); do
    grep -q "ouvindo UDP" "$LOGS/servidor.log" && break
    if ! kill -0 "$SERVIDOR" 2>/dev/null; then
        echo "O servidor não subiu:"
        cat "$LOGS/servidor.log"
        SERVIDOR=""
        exit 1
    fi
    sleep 0.1
done

# --- Nós -------------------------------------------------------------------
subir_no() {
    local id=$1
    shift
    "$PYTHON" no.py "$id" "$@" > "$LOGS/$id.log" 2>&1 &
    NOS[$id]=$!
}

subir_no A B:4 C:1
subir_no B A:4 D:2
subir_no C A:1 D:5 E:2
subir_no D B:2 C:5 E:1.5
subir_no E C:2 D:1.5
sleep 0.5

cat <<EOF

Servidor (pid $SERVIDOR) e nós no ar:
$(for id in $(printf '%s\n' "${!NOS[@]}" | sort); do echo "  nó $id  pid ${NOS[$id]}  log $LOGS/$id.log"; done)

Em outro terminal:
  $PYTHON operador.py

Para simular falhas:
  kill -STOP <pid>   suspende o nó (como Ctrl+Z): INATIVO em 4 a 7 s
  kill -CONT <pid>   retoma (como fg): ATIVO no próximo heartbeat
  kill -9 <pid>      queda abrupta: INATIVO na hora (FIN do kernel)
  kill <pid>         saída limpa: DESCONECTADO

Ctrl+C aqui encerra tudo. Log do servidor:
------------------------------------------------------------------------
EOF

# Em primeiro plano: o Ctrl+C interrompe o tail e o bash roda o trap.
tail -n +1 -f "$LOGS/servidor.log"
