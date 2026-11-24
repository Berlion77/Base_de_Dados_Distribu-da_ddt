/*
 * ddt.c - Base de Dados Distribuida (RC-I 2026-2027, Projecto de Laboratorio #1)
 *
 * Cada no do anel mantem duas sessoes TCP: uma com o sucessor (por onde
 * seguem as mensagens QRY) e outra com o predecessor (por onde seguem as
 * mensagens RSP). O servidor de arranque (bootserver.c) e consultado por UDP.
 *
 * Uso: ddt [-t ringport] [-i bootIP] [-p bootport]
 *
 * Convencoes:
 *   - As mensagens sao texto ASCII terminado em '\n' (os <> do enunciado
 *     delimitam a mensagem e nao fazem parte dela).
 *   - h = 6, ou seja 64 identificadores por anel.
 *   - Variaveis de ambiente: DDT_DEBUG=1 mostra as mensagens trocadas (stderr);
 *     DDT_IP=a.b.c.d forca o endereco IP anunciado pelo no.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define H 6
#define RING_SIZE (1 << H) /* 64 identificadores */
#define MAXLINE 512
#define MAX_INC 32
#define CONNECT_TIMEOUT 3 /* segundos */
#define BOOT_TIMEOUT 2    /* segundos por tentativa UDP */
#define BOOT_TRIES 3

#define DEFAULT_RING_PORT 58001
#define DEFAULT_BOOT_IP "192.168.56.21"
#define DEFAULT_BOOT_PORT 58000

/* ------------------------------------------------------------------ */
/* Tipos e estado global                                              */
/* ------------------------------------------------------------------ */

typedef struct {
    int fd;
    char buf[MAXLINE];
    int len;
} Conn;

/* Ligacao TCP recebida cujo papel ainda nao e conhecido (ID ou NEW). */
typedef struct {
    Conn c;
    int waiting; /* 1 = a aguardar o resultado da pesquisa de 'id' */
    int id;
} Inc;

typedef enum { ROLE_PRED, ROLE_SUCC, ROLE_INC } Role;

static char boot_ip[64] = DEFAULT_BOOT_IP;
static int boot_port = DEFAULT_BOOT_PORT;
static int ring_port = DEFAULT_RING_PORT;
static char my_ip[64] = "127.0.0.1";
static int listen_fd = -1;
static int debug = 0;
static volatile sig_atomic_t quit_flag = 0;

static int in_ring = 0;
static int ring_num = -1;
static int my_id = -1;
static int succ_id = -1;
static int pred_id = -1;
static char succ_ip[64] = "";
static int succ_port = 0;
static int is_boot = 0;
static int user_search = -1; /* identificador pesquisado pelo utilizador */

static Conn succ_c = {-1, "", 0};
static Conn pred_c = {-1, "", 0};
static Inc incs[MAX_INC];

/* Declaracoes antecipadas. */
static void process_lines(Conn *c, Role role, Inc *in);
static int step(int timeout_ms, int use_stdin);

/* ------------------------------------------------------------------ */
/* Utilitarios                                                        */
/* ------------------------------------------------------------------ */

static void warn(const char *fmt, ...)
{
    va_list ap;
    fputs("[aviso] ", stderr);
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fputc('\n', stderr);
}

static void dbg(const char *fmt, ...)
{
    if (!debug)
        return;
    va_list ap;
    fprintf(stderr, "[dbg %d] ", my_id);
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fputc('\n', stderr);
}

static long now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long)ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

static void set_nonblock(int fd)
{
    int fl = fcntl(fd, F_GETFL, 0);
    if (fl != -1)
        fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

static int valid_id(int i) { return i >= 0 && i < RING_SIZE; }

static int valid_loc(const char *ip, int port)
{
    struct in_addr a;
    return inet_pton(AF_INET, ip, &a) == 1 && port > 0 && port <= 65535;
}

/* Distancia (no sentido do anel) de 'from' ate 'to'. */
static int dist(int from, int to)
{
    return ((to - from) % RING_SIZE + RING_SIZE) % RING_SIZE;
}

/* O no e responsavel por k se k esta em (pred, me]. */
static int responsible(int k)
{
    if (!in_ring || pred_id < 0)
        return 0;
    if (pred_id == my_id) /* no unico: responsavel por tudo */
        return 1;
    int d = dist(pred_id, k);
    return d != 0 && d <= dist(pred_id, my_id);
}

static void detect_my_ip(void)
{
    const char *env = getenv("DDT_IP");
    struct in_addr a;
    if (env != NULL && inet_pton(AF_INET, env, &a) == 1) {
        snprintf(my_ip, sizeof my_ip, "%s", env);
        return;
    }
    snprintf(my_ip, sizeof my_ip, "127.0.0.1");
    /* "connect" UDP nao envia nada: serve so para descobrir a interface. */
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd == -1)
        return;
    struct sockaddr_in b, me;
    memset(&b, 0, sizeof b);
    b.sin_family = AF_INET;
    b.sin_port = htons((unsigned short)boot_port);
    if (inet_pton(AF_INET, boot_ip, &b.sin_addr) == 1 &&
        connect(fd, (struct sockaddr *)&b, sizeof b) == 0) {
        socklen_t l = sizeof me;
        if (getsockname(fd, (struct sockaddr *)&me, &l) == 0)
            inet_ntop(AF_INET, &me.sin_addr, my_ip, sizeof my_ip);
    }
    close(fd);
}

/* ------------------------------------------------------------------ */
/* Primitivas de rede                                                 */
/* ------------------------------------------------------------------ */

static int send_all(int fd, const char *buf, size_t len)
{
    size_t off = 0;
    while (off < len) {
        ssize_t n = write(fd, buf + off, len - off);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                fd_set wf;
                struct timeval tv = {2, 0};
                FD_ZERO(&wf);
                FD_SET(fd, &wf);
                if (select(fd + 1, NULL, &wf, NULL, &tv) <= 0)
                    return -1;
                continue;
            }
            return -1;
        }
        off += (size_t)n;
    }
    return 0;
}

/* Envia uma mensagem formatada por uma sessao TCP. */
static int conn_send(Conn *c, const char *fmt, ...)
{
    char msg[MAXLINE];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(msg, sizeof msg, fmt, ap);
    va_end(ap);
    if (n < 0 || n >= (int)sizeof msg) {
        warn("mensagem demasiado longa");
        return -1;
    }
    if (c->fd < 0) {
        warn("sessao TCP indisponivel; mensagem descartada: %.*s", n - 1, msg);
        return -1;
    }
    dbg("TX %.*s", n - 1, msg);
    if (send_all(c->fd, msg, (size_t)n) < 0) {
        warn("falha ao escrever na sessao TCP: %s", strerror(errno));
        return -1;
    }
    return 0;
}

/* Liga-se a (ip, port) com limite de tempo. Devolve fd nao bloqueante. */
static int tcp_connect(const char *ip, int port)
{
    struct sockaddr_in a;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        warn("socket: %s", strerror(errno));
        return -1;
    }
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_port = htons((unsigned short)port);
    if (inet_pton(AF_INET, ip, &a.sin_addr) != 1) {
        close(fd);
        return -1;
    }
    set_nonblock(fd);
    if (connect(fd, (struct sockaddr *)&a, sizeof a) == -1) {
        if (errno != EINPROGRESS) {
            warn("connect %s:%d: %s", ip, port, strerror(errno));
            close(fd);
            return -1;
        }
        fd_set wf;
        struct timeval tv = {CONNECT_TIMEOUT, 0};
        FD_ZERO(&wf);
        FD_SET(fd, &wf);
        if (select(fd + 1, NULL, &wf, NULL, &tv) <= 0) {
            warn("connect %s:%d: tempo esgotado", ip, port);
            close(fd);
            return -1;
        }
        int err = 0;
        socklen_t l = sizeof err;
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &l);
        if (err != 0) {
            warn("connect %s:%d: %s", ip, port, strerror(err));
            close(fd);
            return -1;
        }
    }
    return fd;
}

/* Le uma linha (ate '\n') com limite de tempo. 0 = ok, -1 = erro/EOF. */
static int read_line_timeout(int fd, char *buf, size_t size, int secs)
{
    size_t len = 0;
    long deadline = now_ms() + secs * 1000L;
    while (len + 1 < size) {
        long left = deadline - now_ms();
        if (left <= 0)
            return -1;
        fd_set rf;
        struct timeval tv = {(time_t)(left / 1000), (suseconds_t)((left % 1000) * 1000)};
        FD_ZERO(&rf);
        FD_SET(fd, &rf);
        int r = select(fd + 1, &rf, NULL, NULL, &tv);
        if (r < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (r == 0)
            return -1;
        char ch;
        ssize_t n = read(fd, &ch, 1);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0)
            return -1;
        if (ch == '\n') {
            if (len > 0 && buf[len - 1] == '\r')
                len--;
            buf[len] = '\0';
            return 0;
        }
        buf[len++] = ch;
    }
    return -1;
}

/*
 * Fecho "limpo": evita que um RST (dados por ler) apague a ultima mensagem
 * que acabamos de escrever. Espera ate 500 ms pelo fecho do par.
 */
static void graceful_close(int fd)
{
    if (fd < 0)
        return;
    shutdown(fd, SHUT_WR);
    long deadline = now_ms() + 500;
    for (;;) {
        long left = deadline - now_ms();
        if (left <= 0)
            break;
        fd_set rf;
        struct timeval tv = {0, (suseconds_t)(left * 1000)};
        FD_ZERO(&rf);
        FD_SET(fd, &rf);
        int r = select(fd + 1, &rf, NULL, NULL, &tv);
        if (r <= 0)
            break;
        char tmp[256];
        ssize_t n = read(fd, tmp, sizeof tmp);
        if (n == 0)
            break;
        if (n < 0 && errno != EAGAIN && errno != EINTR && errno != EWOULDBLOCK)
            break;
    }
    close(fd);
}

/* Pedido/resposta UDP ao servidor de arranque (com repeticoes). */
static int boot_request(const char *msg, char *reply, size_t rsz)
{
    struct sockaddr_in a;
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd == -1) {
        warn("socket UDP: %s", strerror(errno));
        return -1;
    }
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_port = htons((unsigned short)boot_port);
    if (inet_pton(AF_INET, boot_ip, &a.sin_addr) != 1) {
        warn("endereco do servidor de arranque invalido");
        close(fd);
        return -1;
    }
    for (int t = 0; t < BOOT_TRIES; t++) {
        dbg("UDP TX %.*s", (int)strlen(msg) - 1, msg);
        if (sendto(fd, msg, strlen(msg), 0, (struct sockaddr *)&a, sizeof a) == -1) {
            warn("sendto: %s", strerror(errno));
            break;
        }
        fd_set rf;
        struct timeval tv = {BOOT_TIMEOUT, 0};
        FD_ZERO(&rf);
        FD_SET(fd, &rf);
        int r = select(fd + 1, &rf, NULL, NULL, &tv);
        if (r > 0) {
            ssize_t n = recvfrom(fd, reply, rsz - 1, 0, NULL, NULL);
            if (n >= 0) {
                reply[n] = '\0';
                while (n > 0 && (reply[n - 1] == '\n' || reply[n - 1] == '\r'))
                    reply[--n] = '\0';
                dbg("UDP RX %s", reply);
                close(fd);
                return 0;
            }
        }
    }
    close(fd);
    return -1;
}

/* ------------------------------------------------------------------ */
/* Gestao de sessoes                                                  */
/* ------------------------------------------------------------------ */

static void conn_drop(Conn *c, Role role, Inc *in)
{
    if (c->fd >= 0)
        close(c->fd);
    c->fd = -1;
    c->len = 0;
    if (role == ROLE_INC && in != NULL) {
        in->waiting = 0;
        in->id = -1;
    }
}

/* Volta ao estado de no unico no anel (sucessor = predecessor = eu). */
static void reset_single(void)
{
    if (succ_c.fd >= 0)
        close(succ_c.fd);
    if (pred_c.fd >= 0)
        close(pred_c.fd);
    succ_c.fd = pred_c.fd = -1;
    succ_c.len = pred_c.len = 0;
    succ_id = pred_id = my_id;
    snprintf(succ_ip, sizeof succ_ip, "%s", my_ip);
    succ_port = ring_port;
}

/* Um resultado de pesquisa (iniciada neste no) chegou. */
static void complete_search(int k, int l, const char *lip, int lport)
{
    for (int n = 0; n < MAX_INC; n++) {
        if (incs[n].c.fd >= 0 && incs[n].waiting && incs[n].id == k) {
            conn_send(&incs[n].c, "SUCC %d %s %d\n", l, lip, lport);
            conn_drop(&incs[n].c, ROLE_INC, &incs[n]);
        }
    }
    if (user_search == k) {
        printf("Responsavel por %d: (%d %s %d)\n", k, l, lip, lport);
        user_search = -1;
    }
}

/* Mensagem NEW recebida de uma ligacao nova: 'i' passa a ser o predecessor. */
static void handle_new(Inc *in, int i, const char *ip, int port)
{
    Conn *c = &in->c;
    if (!in_ring) {
        warn("NEW recebido fora de um anel; ignorado");
        conn_drop(c, ROLE_INC, in);
        return;
    }
    int old_alone = (pred_id == my_id);
    int old_fd = pred_c.fd;

    if (old_fd >= 0) {
        if (!old_alone)
            conn_send(&pred_c, "CON %d %s %d\n", i, ip, port); /* avisa o antigo predecessor */
        graceful_close(old_fd);
    }
    /* A ligacao recebida passa a ser a sessao com o predecessor. */
    pred_c.fd = c->fd;
    pred_c.len = c->len;
    memcpy(pred_c.buf, c->buf, (size_t)c->len);
    pred_id = i;
    c->fd = -1;
    c->len = 0;
    in->waiting = 0;
    in->id = -1;

    if (old_alone) {
        /* Eramos o unico no: o novo no tambem passa a ser o nosso sucessor. */
        int fd = tcp_connect(ip, port);
        if (fd < 0) {
            warn("nao foi possivel ligar ao novo no %d", i);
            return;
        }
        succ_c.fd = fd;
        succ_c.len = 0;
        succ_id = i;
        snprintf(succ_ip, sizeof succ_ip, "%s", ip);
        succ_port = port;
        conn_send(&succ_c, "NEW %d %s %d\n", my_id, my_ip, ring_port);
    }
}

/* Mensagem CON recebida do sucessor: novo sucessor em (x, ip, port). */
static void handle_con(int x, const char *ip, int port)
{
    if (succ_c.fd >= 0)
        close(succ_c.fd);
    succ_c.fd = -1;
    succ_c.len = 0;
    if (x == my_id) { /* o anel ficou so comigo */
        reset_single();
        return;
    }
    int fd = tcp_connect(ip, port);
    if (fd < 0) {
        warn("nao foi possivel ligar ao novo sucessor %d", x);
        return;
    }
    succ_c.fd = fd;
    succ_id = x;
    snprintf(succ_ip, sizeof succ_ip, "%s", ip);
    succ_port = port;
    conn_send(&succ_c, "NEW %d %s %d\n", my_id, my_ip, ring_port);
}

/* ------------------------------------------------------------------ */
/* Processamento de mensagens TCP                                     */
/* ------------------------------------------------------------------ */

/* Devolve 0 se o processamento de linhas desta sessao deve parar. */
static int handle_line(Role role, Conn *c, Inc *in, const char *line)
{
    char extra[8], ip[64];
    int i, j, k, l, port;

    dbg("RX[%s] %s", role == ROLE_PRED ? "pred" : role == ROLE_SUCC ? "succ" : "novo", line);

    if (role == ROLE_INC) {
        if (sscanf(line, "ID %d %7s", &i, extra) == 1 && valid_id(i)) {
            if (!in_ring) {
                conn_drop(c, role, in);
                return 0;
            }
            in->waiting = 1;
            in->id = i;
            if (responsible(i)) {
                conn_send(c, "SUCC %d %s %d\n", my_id, my_ip, ring_port);
                conn_drop(c, role, in);
                return 0;
            }
            if (conn_send(&succ_c, "QRY %d %d\n", my_id, i) < 0) {
                conn_drop(c, role, in);
                return 0;
            }
            return 1;
        }
        if (sscanf(line, "NEW %d %63s %d %7s", &i, ip, &port, extra) == 3 &&
            valid_id(i) && valid_loc(ip, port)) {
            handle_new(in, i, ip, port);
            process_lines(&pred_c, ROLE_PRED, NULL); /* dados que vieram junto */
            return 0;
        }
        warn("mensagem invalida numa ligacao nova: '%s'", line);
        conn_drop(c, role, in);
        return 0;
    }

    if (role == ROLE_PRED) {
        if (sscanf(line, "QRY %d %d %7s", &j, &k, extra) == 2 && valid_id(j) && valid_id(k)) {
            if (responsible(k))
                conn_send(&pred_c, "RSP %d %d %d %s %d\n", j, k, my_id, my_ip, ring_port);
            else
                conn_send(&succ_c, "QRY %d %d\n", j, k);
            return 1;
        }
        if (strcmp(line, "BOOT") == 0) {
            is_boot = 1;
            return 1;
        }
        warn("mensagem invalida do predecessor: '%s'", line);
        return 1;
    }

    /* ROLE_SUCC */
    if (sscanf(line, "RSP %d %d %d %63s %d %7s", &j, &k, &l, ip, &port, extra) == 5 &&
        valid_id(j) && valid_id(k) && valid_id(l) && valid_loc(ip, port)) {
        if (j == my_id)
            complete_search(k, l, ip, port);
        else
            conn_send(&pred_c, "RSP %d %d %d %s %d\n", j, k, l, ip, port);
        return 1;
    }
    if (sscanf(line, "CON %d %63s %d %7s", &i, ip, &port, extra) == 3 &&
        valid_id(i) && valid_loc(ip, port)) {
        handle_con(i, ip, port);
        return 0; /* a sessao antiga foi substituida */
    }
    warn("mensagem invalida do sucessor: '%s'", line);
    return 1;
}

static void process_lines(Conn *c, Role role, Inc *in)
{
    for (;;) {
        if (c->fd < 0)
            return;
        char *nl = memchr(c->buf, '\n', (size_t)c->len);
        if (nl == NULL)
            break;
        size_t linelen = (size_t)(nl - c->buf);
        char line[MAXLINE];
        memcpy(line, c->buf, linelen);
        line[linelen] = '\0';
        if (linelen > 0 && line[linelen - 1] == '\r')
            line[linelen - 1] = '\0';
        size_t consumed = linelen + 1;
        memmove(c->buf, c->buf + consumed, (size_t)c->len - consumed);
        c->len -= (int)consumed;
        if (!handle_line(role, c, in, line))
            return;
    }
    if (c->fd >= 0 && c->len >= MAXLINE - 1) {
        warn("linha demasiado longa; sessao fechada");
        conn_drop(c, role, in);
    }
}

/* Le dados disponiveis numa sessao e processa as linhas completas. */
static void service(Conn *c, Role role, Inc *in)
{
    if (c->len >= MAXLINE - 1) {
        warn("linha demasiado longa; sessao fechada");
        conn_drop(c, role, in);
        return;
    }
    ssize_t r = read(c->fd, c->buf + c->len, (size_t)(MAXLINE - 1 - c->len));
    if (r < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
            return;
        warn("erro de leitura: %s", strerror(errno));
        conn_drop(c, role, in);
        return;
    }
    if (r == 0) {
        if (role == ROLE_SUCC)
            warn("sessao com o sucessor fechada de forma inesperada");
        else
            dbg("sessao fechada pelo par (%s)", role == ROLE_PRED ? "pred" : "novo");
        conn_drop(c, role, in);
        return;
    }
    c->len += (int)r;
    process_lines(c, role, in);
}

static void accept_conn(void)
{
    int fd = accept(listen_fd, NULL, NULL);
    if (fd == -1) {
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
            warn("accept: %s", strerror(errno));
        return;
    }
    set_nonblock(fd);
    for (int n = 0; n < MAX_INC; n++) {
        if (incs[n].c.fd < 0) {
            incs[n].c.fd = fd;
            incs[n].c.len = 0;
            incs[n].waiting = 0;
            incs[n].id = -1;
            return;
        }
    }
    warn("demasiadas ligacoes pendentes; ligacao recusada");
    close(fd);
}

/* ------------------------------------------------------------------ */
/* Comandos do utilizador                                             */
/* ------------------------------------------------------------------ */

/* Liga-se ao sucessor (s) e anuncia-se com NEW; espera pelo predecessor. */
static int attach_to_successor(int x, int i, int s, const char *sip, int sport)
{
    int fd = tcp_connect(sip, sport);
    if (fd < 0) {
        warn("nao foi possivel ligar ao sucessor %d (%s:%d)", s, sip, sport);
        return -1;
    }
    in_ring = 1;
    ring_num = x;
    my_id = i;
    is_boot = 0;
    succ_c.fd = fd;
    succ_c.len = 0;
    pred_c.fd = -1;
    pred_c.len = 0;
    succ_id = s;
    pred_id = -1;
    snprintf(succ_ip, sizeof succ_ip, "%s", sip);
    succ_port = sport;
    if (conn_send(&succ_c, "NEW %d %s %d\n", my_id, my_ip, ring_port) < 0) {
        close(succ_c.fd);
        succ_c.fd = -1;
        in_ring = 0;
        my_id = ring_num = succ_id = -1;
        return -1;
    }
    /* O predecessor liga-se a nos (mensagem NEW) para fechar o anel. */
    long deadline = now_ms() + 3000;
    while (pred_c.fd < 0 && now_ms() < deadline && !quit_flag)
        step(100, 0);
    if (pred_c.fd < 0)
        warn("o predecessor ainda nao estabeleceu a ligacao");
    return 0;
}

static void cmd_join_auto(int x, int i)
{
    char msg[128], reply[256], extra[8];

    snprintf(msg, sizeof msg, "BQRY %d\n", x);
    if (boot_request(msg, reply, sizeof reply) < 0) {
        printf("Servidor de arranque sem resposta.\n");
        return;
    }
    if (strcmp(reply, "EMPTY") == 0) {
        snprintf(msg, sizeof msg, "REG %d %d %s %d\n", x, i, my_ip, ring_port);
        if (boot_request(msg, reply, sizeof reply) < 0 || strcmp(reply, "OK") != 0) {
            printf("Falha ao registar o no no servidor de arranque.\n");
            return;
        }
        in_ring = 1;
        ring_num = x;
        my_id = i;
        is_boot = 1;
        reset_single();
        printf("No %d criou o anel %d.\n", i, x);
        return;
    }

    int rx, j, jport;
    char jip[64];
    if (sscanf(reply, "BRSP %d %d %63s %d %7s", &rx, &j, jip, &jport, extra) != 4 ||
        rx != x || !valid_id(j) || !valid_loc(jip, jport)) {
        printf("Resposta invalida do servidor de arranque: '%s'\n", reply);
        return;
    }
    if (j == i) {
        printf("O identificador %d ja esta em uso (no de arranque). Escolha outro.\n", i);
        return;
    }

    /* Pede ao no de arranque que pesquise o identificador i. */
    int fd = tcp_connect(jip, jport);
    if (fd < 0) {
        printf("Nao foi possivel contactar o no de arranque %d.\n", j);
        return;
    }
    char line[MAXLINE];
    snprintf(msg, sizeof msg, "ID %d\n", i);
    if (send_all(fd, msg, strlen(msg)) < 0 || read_line_timeout(fd, line, sizeof line, 5) < 0) {
        printf("O no de arranque nao respondeu a pesquisa.\n");
        close(fd);
        return;
    }
    close(fd);

    int l, lport;
    char lip[64];
    if (sscanf(line, "SUCC %d %63s %d %7s", &l, lip, &lport, extra) != 3 ||
        !valid_id(l) || !valid_loc(lip, lport)) {
        printf("Resposta invalida do no de arranque: '%s'\n", line);
        return;
    }
    if (l == i) {
        printf("O identificador %d ja esta em uso no anel %d. Escolha outro.\n", i, x);
        return;
    }
    if (attach_to_successor(x, i, l, lip, lport) == 0)
        printf("No %d entrou no anel %d (sucessor %d).\n", i, x, l);
}

static void cmd_join_explicit(int x, int i, int s, const char *sip, int sport)
{
    if (s == i) {
        printf("O sucessor nao pode ser o proprio no.\n");
        return;
    }
    if (attach_to_successor(x, i, s, sip, sport) == 0)
        printf("No %d entrou no anel %d (sucessor %d).\n", i, x, s);
}

static void do_leave(void)
{
    char msg[160], reply[64];

    if (succ_id == my_id) { /* unico no: anula o anel */
        snprintf(msg, sizeof msg, "UNR %d\n", ring_num);
        if (boot_request(msg, reply, sizeof reply) < 0 || strcmp(reply, "OK") != 0)
            warn("o servidor de arranque nao confirmou a anulacao do anel");
        if (succ_c.fd >= 0)
            close(succ_c.fd);
        if (pred_c.fd >= 0)
            close(pred_c.fd);
    } else {
        if (is_boot) {
            snprintf(msg, sizeof msg, "REG %d %d %s %d\n", ring_num, succ_id, succ_ip, succ_port);
            if (boot_request(msg, reply, sizeof reply) < 0 || strcmp(reply, "OK") != 0)
                warn("o servidor de arranque nao confirmou o novo no de arranque");
            conn_send(&succ_c, "BOOT\n");
        }
        graceful_close(succ_c.fd);
        if (pred_c.fd >= 0) {
            conn_send(&pred_c, "CON %d %s %d\n", succ_id, succ_ip, succ_port);
            graceful_close(pred_c.fd);
        } else {
            warn("sem sessao com o predecessor; o anel pode ficar inconsistente");
        }
    }
    succ_c.fd = pred_c.fd = -1;
    succ_c.len = pred_c.len = 0;
    for (int n = 0; n < MAX_INC; n++)
        conn_drop(&incs[n].c, ROLE_INC, &incs[n]);
    in_ring = 0;
    is_boot = 0;
    user_search = -1;
    my_id = ring_num = succ_id = pred_id = -1;
}

static void cmd_search(int k)
{
    if (!in_ring) {
        printf("O no nao pertence a nenhum anel.\n");
        return;
    }
    if (!valid_id(k)) {
        printf("Identificador invalido (0 a %d).\n", RING_SIZE - 1);
        return;
    }
    if (responsible(k)) {
        printf("Responsavel por %d: (%d %s %d)\n", k, my_id, my_ip, ring_port);
        return;
    }
    if (conn_send(&succ_c, "QRY %d %d\n", my_id, k) == 0)
        user_search = k;
    else
        printf("Sessao com o sucessor indisponivel.\n");
}

static void cmd_show(void)
{
    if (!in_ring) {
        printf("O no nao pertence a nenhum anel.\n");
        return;
    }
    printf("Anel: %d\nNo: %d\nSucessor: %d\n", ring_num, my_id, succ_id);
    if (pred_id >= 0)
        printf("Predecessor: %d\n", pred_id);
    else
        printf("Predecessor: (a aguardar ligacao)\n");
}

/* Devolve 1 se a aplicacao deve terminar. */
static int handle_command(const char *line)
{
    char cmd[32], extra[8], sip[64];
    int x, i, s, sport, k;

    if (sscanf(line, "%31s", cmd) != 1)
        return 0;

    if (strcmp(cmd, "join") == 0) {
        int explicit_ok = sscanf(line, "join %d %d %d %63s %d %7s", &x, &i, &s, sip, &sport, extra) == 5;
        int auto_ok = !explicit_ok && sscanf(line, "join %d %d %7s", &x, &i, extra) == 2;
        if (!explicit_ok && !auto_ok) {
            printf("Uso: join x i  |  join x i succ succIP succTCP\n");
            return 0;
        }
        if (in_ring) {
            printf("O no ja pertence ao anel %d. Use 'leave' primeiro.\n", ring_num);
            return 0;
        }
        if (x < 0 || !valid_id(i)) {
            printf("Argumentos invalidos (anel >= 0, identificador 0 a %d).\n", RING_SIZE - 1);
            return 0;
        }
        if (explicit_ok) {
            if (!valid_id(s) || !valid_loc(sip, sport)) {
                printf("Sucessor invalido.\n");
                return 0;
            }
            cmd_join_explicit(x, i, s, sip, sport);
        } else {
            cmd_join_auto(x, i);
        }
    } else if (strcmp(cmd, "leave") == 0) {
        if (!in_ring) {
            printf("O no nao pertence a nenhum anel.\n");
        } else {
            do_leave();
            printf("O no saiu do anel.\n");
        }
    } else if (strcmp(cmd, "show") == 0) {
        cmd_show();
    } else if (strcmp(cmd, "search") == 0) {
        if (sscanf(line, "search %d %7s", &k, extra) != 1)
            printf("Uso: search k\n");
        else
            cmd_search(k);
    } else if (strcmp(cmd, "exit") == 0) {
        return 1;
    } else {
        printf("Comando desconhecido. Comandos: join, leave, show, search, exit\n");
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* Ciclo de eventos                                                   */
/* ------------------------------------------------------------------ */

static void addfd(fd_set *s, int fd, int *maxfd)
{
    if (fd >= 0) {
        FD_SET(fd, s);
        if (fd > *maxfd)
            *maxfd = fd;
    }
}

/* Uma volta de select(). Devolve 1 se a aplicacao deve terminar. */
static int step(int timeout_ms, int use_stdin)
{
    fd_set rf;
    int maxfd = -1;
    int pfd = pred_c.fd, sfd = succ_c.fd;
    int ifd[MAX_INC];

    FD_ZERO(&rf);
    addfd(&rf, listen_fd, &maxfd);
    if (use_stdin)
        addfd(&rf, STDIN_FILENO, &maxfd);
    addfd(&rf, pfd, &maxfd);
    addfd(&rf, sfd, &maxfd);
    for (int n = 0; n < MAX_INC; n++) {
        ifd[n] = incs[n].c.fd;
        addfd(&rf, ifd[n], &maxfd);
    }

    struct timeval tv, *ptv = NULL;
    if (timeout_ms >= 0) {
        tv.tv_sec = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        ptv = &tv;
    }
    int r = select(maxfd + 1, &rf, NULL, NULL, ptv);
    if (r < 0) {
        if (errno == EINTR)
            return quit_flag ? 1 : 0;
        warn("select: %s", strerror(errno));
        return 1;
    }
    if (r == 0)
        return 0;

    /* As sessoes podem ser substituidas durante o tratamento: so se actua
     * sobre descritores que continuam a ser os mesmos (as leituras sao nao
     * bloqueantes, por isso um evento obsoleto e inofensivo). */
    if (pfd >= 0 && pred_c.fd == pfd && FD_ISSET(pfd, &rf))
        service(&pred_c, ROLE_PRED, NULL);
    if (sfd >= 0 && succ_c.fd == sfd && FD_ISSET(sfd, &rf))
        service(&succ_c, ROLE_SUCC, NULL);
    for (int n = 0; n < MAX_INC; n++)
        if (ifd[n] >= 0 && incs[n].c.fd == ifd[n] && FD_ISSET(ifd[n], &rf))
            service(&incs[n].c, ROLE_INC, &incs[n]);
    if (FD_ISSET(listen_fd, &rf))
        accept_conn();

    if (use_stdin && FD_ISSET(STDIN_FILENO, &rf)) {
        char line[256];
        if (fgets(line, sizeof line, stdin) == NULL)
            return 1; /* EOF no stdin */
        if (handle_command(line))
            return 1;
    }
    return 0;
}

static void on_signal(int sig)
{
    (void)sig;
    quit_flag = 1;
}

static int start_server(void)
{
    struct sockaddr_in a;
    int one = 1;

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd == -1) {
        perror("socket");
        return -1;
    }
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port = htons((unsigned short)ring_port);
    if (bind(listen_fd, (struct sockaddr *)&a, sizeof a) == -1) {
        perror("bind");
        return -1;
    }
    if (listen(listen_fd, 16) == -1) {
        perror("listen");
        return -1;
    }
    set_nonblock(listen_fd);
    return 0;
}

int main(int argc, char *argv[])
{
    struct in_addr tmp;

    for (int a = 1; a < argc; a++) {
        if (strcmp(argv[a], "-t") == 0 && a + 1 < argc) {
            ring_port = atoi(argv[++a]);
        } else if (strcmp(argv[a], "-i") == 0 && a + 1 < argc) {
            snprintf(boot_ip, sizeof boot_ip, "%s", argv[++a]);
        } else if (strcmp(argv[a], "-p") == 0 && a + 1 < argc) {
            boot_port = atoi(argv[++a]);
        } else {
            fprintf(stderr, "Uso: %s [-t ringport] [-i bootIP] [-p bootport]\n", argv[0]);
            return 1;
        }
    }
    if (ring_port <= 0 || ring_port > 65535 || boot_port <= 0 || boot_port > 65535 ||
        inet_pton(AF_INET, boot_ip, &tmp) != 1) {
        fprintf(stderr, "Argumentos invalidos.\n");
        return 1;
    }

    debug = (getenv("DDT_DEBUG") != NULL);
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal; /* sem SA_RESTART: select() devolve EINTR */
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    setvbuf(stdin, NULL, _IONBF, 0); /* fgets + select sem dados "presos" no buffer */
    setvbuf(stdout, NULL, _IOLBF, 0);

    for (int n = 0; n < MAX_INC; n++) {
        incs[n].c.fd = -1;
        incs[n].c.len = 0;
        incs[n].waiting = 0;
        incs[n].id = -1;
    }

    detect_my_ip();
    if (start_server() < 0)
        return 1;
    printf("ddt: servidor TCP em %s:%d | servidor de arranque %s:%d\n",
           my_ip, ring_port, boot_ip, boot_port);

    while (!quit_flag) {
        if (step(-1, 1))
            break;
    }

    if (in_ring)
        do_leave(); /* saida graciosa: o anel nao fica partido */
    close(listen_fd);
    return 0;
}
