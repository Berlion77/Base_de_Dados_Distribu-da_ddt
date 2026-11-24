/*
 * bootserver.c - Servidor de arranque UDP do projecto ddt (RC-I 2026-2027)
 *
 * Mantem a associacao: numero do anel -> (id, IP, porta TCP) do no de arranque.
 *
 * Mensagens (texto, terminadas em '\n'):
 *   BQRY x                 -> EMPTY | BRSP x j j.IP j.TCP
 *   REG x i i.IP i.TCP     -> OK
 *   UNR x                  -> OK
 *
 * Uso: bootserver [-p porta]     (por omissao 58000)
 */
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define MAX_RINGS 1024
#define BUF_SIZE 256
#define DEFAULT_PORT 58000

typedef struct {
    int used;
    int ring;
    int id;
    char ip[64];
    int tcp;
} Entry;

static Entry table[MAX_RINGS];

static Entry *find_ring(int ring)
{
    for (int k = 0; k < MAX_RINGS; k++)
        if (table[k].used && table[k].ring == ring)
            return &table[k];
    return NULL;
}

static Entry *free_slot(void)
{
    for (int k = 0; k < MAX_RINGS; k++)
        if (!table[k].used)
            return &table[k];
    return NULL;
}

/* Valida um endereco IPv4 textual. */
static int valid_ip(const char *ip)
{
    struct in_addr a;
    return inet_pton(AF_INET, ip, &a) == 1;
}

int main(int argc, char *argv[])
{
    int port = DEFAULT_PORT;

    for (int k = 1; k < argc; k++) {
        if (strcmp(argv[k], "-p") == 0 && k + 1 < argc) {
            port = atoi(argv[++k]);
        } else {
            fprintf(stderr, "Uso: %s [-p porta]\n", argv[0]);
            return 1;
        }
    }
    if (port <= 0 || port > 65535) {
        fprintf(stderr, "Porta invalida\n");
        return 1;
    }

    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd == -1) {
        perror("socket");
        return 1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((unsigned short)port);
    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) == -1) {
        perror("bind");
        close(fd);
        return 1;
    }
    setvbuf(stdout, NULL, _IOLBF, 0);
    printf("[bootserver] a escutar em UDP:%d\n", port);

    for (;;) {
        char buf[BUF_SIZE];
        char reply[BUF_SIZE];
        struct sockaddr_in from;
        socklen_t flen = sizeof from;

        ssize_t n = recvfrom(fd, buf, sizeof buf - 1, 0,
                             (struct sockaddr *)&from, &flen);
        if (n == -1) {
            if (errno == EINTR)
                continue;
            perror("recvfrom");
            break;
        }
        buf[n] = '\0';
        /* Remove terminadores de linha. */
        while (n > 0 && (buf[n - 1] == '\n' || buf[n - 1] == '\r'))
            buf[--n] = '\0';

        char cmd[16] = "";
        char extra[8];
        int x, i, tcp;
        char ip[64];
        int have_reply = 1;

        if (sscanf(buf, "%15s", cmd) != 1) {
            continue; /* mensagem vazia: ignora */
        }

        if (strcmp(cmd, "BQRY") == 0 &&
            sscanf(buf, "BQRY %d %7s", &x, extra) == 1) {
            Entry *e = find_ring(x);
            if (e == NULL)
                snprintf(reply, sizeof reply, "EMPTY\n");
            else
                snprintf(reply, sizeof reply, "BRSP %d %d %s %d\n",
                         e->ring, e->id, e->ip, e->tcp);
            printf("[bootserver] BQRY %d -> %s", x, reply);
        } else if (strcmp(cmd, "REG") == 0 &&
                   sscanf(buf, "REG %d %d %63s %d %7s", &x, &i, ip, &tcp, extra) == 4 &&
                   valid_ip(ip) && i >= 0 && tcp > 0 && tcp <= 65535) {
            Entry *e = find_ring(x);
            if (e == NULL)
                e = free_slot();
            if (e == NULL) {
                snprintf(reply, sizeof reply, "ERR tabela cheia\n");
            } else {
                e->used = 1;
                e->ring = x;
                e->id = i;
                snprintf(e->ip, sizeof e->ip, "%s", ip);
                e->tcp = tcp;
                snprintf(reply, sizeof reply, "OK\n");
            }
            printf("[bootserver] REG %d %d %s %d -> %s", x, i, ip, tcp, reply);
        } else if (strcmp(cmd, "UNR") == 0 &&
                   sscanf(buf, "UNR %d %7s", &x, extra) == 1) {
            Entry *e = find_ring(x);
            if (e != NULL)
                e->used = 0;
            snprintf(reply, sizeof reply, "OK\n");
            printf("[bootserver] UNR %d -> OK\n", x);
        } else {
            printf("[bootserver] mensagem invalida: '%s'\n", buf);
            snprintf(reply, sizeof reply, "ERR\n");
        }

        if (have_reply &&
            sendto(fd, reply, strlen(reply), 0,
                   (struct sockaddr *)&from, flen) == -1)
            perror("sendto");
    }

    close(fd);
    return 1;
}
