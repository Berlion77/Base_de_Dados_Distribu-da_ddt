# ddt — Base de Dados Distribuída (RC-I 2026-2027, Projecto de Laboratório #1)

Anel de sessões TCP (h = 6, 64 identificadores) com pesquisa de identificadores
e servidor de arranque UDP.

## Compilar e executar
    make                      # gera ./ddt e ./bootserver
    ./bootserver [-p 58000]   # servidor de arranque UDP
    ./ddt [-t ringport] [-i bootIP] [-p bootport]   # defaults: 58001, 192.168.56.21, 58000

Comandos: `join x i`, `join x i succ succIP succTCP`, `leave`, `show`, `search k`, `exit`
(o `exit` faz `leave` automaticamente se o nó ainda estiver no anel).

## Teste em loopback (um só host)
    make test                 # servidor de arranque + 4 nós, join/search/leave
    ./bootserver &
    ./ddt -t 58011 -i 127.0.0.1     # noutros terminais: 58012, 58013, ...

`DDT_DEBUG=1` mostra as mensagens trocadas (stderr); `DDT_IP=a.b.c.d` força o IP anunciado.

## Vagrant
`vagrant up` cria `boot` (192.168.56.21, bootserver como serviço) e `node1..3` (.22–.24).

## Protocolo
Mensagens ASCII terminadas em `\n` (os `<>` do enunciado não fazem parte da mensagem).
UDP: `BQRY x`, `REG x i ip tcp`, `UNR x` → `EMPTY`, `BRSP x j ip tcp`, `OK`.
TCP: `QRY j k`, `RSP j k l ip tcp`, `ID i`, `SUCC l ip tcp`, `NEW i ip tcp`, `CON i ip tcp`, `BOOT`.
