#!/bin/bash
# Teste de integracao em loopback: servidor de arranque + 4 nos.
cd "$(dirname "$0")/.." || exit 1
BP=58900; T=$(mktemp -d); PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null; wait 2>/dev/null; rm -rf "$T"; }
trap cleanup EXIT
./bootserver -p $BP > "$T/boot.log" 2>&1 & PIDS+=($!)
sleep 0.3
node() { # node <n> <port>
  mkfifo "$T/in$1"
  DDT_IP=127.0.0.1 ./ddt -t "$2" -i 127.0.0.1 -p $BP < "$T/in$1" > "$T/out$1" 2>"$T/err$1" & PIDS+=($!)
  exec {fd}<>"$T/in$1"; eval "FD$1=$fd"
}
cmd() { local v="FD$1"; echo "$2" >&"${!v}"; sleep "${3:-0.6}"; }
node 1 58911; node 2 58912; node 3 58913; node 4 58914
fail=0
check() { if grep -q "$2" "$T/out$1"; then echo "OK   $3"; else echo "FAIL $3"; fail=1; fi; }

cmd 1 "join 7 10"            ; check 1 "criou o anel" "no 10 cria anel"
cmd 2 "join 7 30"            ; check 2 "entrou no anel 7 (sucessor 10)" "no 30 entra"
cmd 3 "join 7 20"            ; check 3 "sucessor 30" "no 20 entra entre 10 e 30"
cmd 4 "join 7 20"            ; check 4 "ja esta em uso" "id repetido e rejeitado"
cmd 4 "join 7 50"            ; check 4 "entrou no anel 7 (sucessor 10)" "no 50 entra"
cmd 3 "show"                 ; check 3 "Predecessor: 10" "show no 20 (pred 10)"
cmd 1 "show"; grep -A3 "Anel" "$T/out1" | tail -4 | grep -q "Sucessor: 20" && echo "OK   no 10 tem sucessor 20" || { echo "FAIL no 10 sucessor"; fail=1; }
cmd 4 "search 25"            ; check 4 "Responsavel por 25: (30 " "search 25 -> 30"
cmd 4 "search 55"            ; check 4 "Responsavel por 55: (10 " "search 55 -> 10 (volta ao anel)"
cmd 1 "search 20"            ; check 1 "Responsavel por 20: (20 " "search 20 -> 20"
cmd 3 "leave"                ; check 3 "saiu do anel" "no 20 sai"
cmd 1 "show"; tail -4 "$T/out1" | grep -q "Sucessor: 30" && echo "OK   anel fechado sem 20" || { echo "FAIL anel apos saida"; fail=1; }
cmd 4 "search 15"            ; check 4 "Responsavel por 15: (30 " "search 15 -> 30 apos saida"
cmd 1 "leave" 1              ; check 1 "saiu do anel" "no 10 (arranque) sai"
cmd 2 "join 9 5"             ; check 2 "ja pertence" "join repetido rejeitado"
cmd 2 "show"; tail -4 "$T/out2" | grep -q "Predecessor: 50" && echo "OK   no 30 pred 50" || { echo "FAIL no 30 pred"; fail=1; }
cmd 3 "join 7 12"            ; check 3 "sucessor 30" "novo no entra via novo no de arranque"
cmd 3 "leave"; cmd 2 "leave"; cmd 4 "leave" 1
grep -q "UNR 7" "$T/boot.log" && echo "OK   anel anulado no servidor de arranque" || { echo "FAIL UNR"; fail=1; }
[ $fail = 0 ] && echo "TODOS OS TESTES PASSARAM" || { echo "HA FALHAS"; head -20 "$T"/err*; }
exit $fail
