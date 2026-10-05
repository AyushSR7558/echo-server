#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>

#define PORT 3000
#define SERVER_IP "127.0.0.1"
#define MAX_MSG 4096

int main() {
    // Create socket
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd < 0) {
        perror("socket");
        return 1;
    }

    // Server address
    struct sockaddr_in server_addr = {};

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);

    if (inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr) <= 0) {
        perror("inet_pton");
        close(fd);
        return 1;
    }

    // Connect to server
    if (connect(fd,
                (struct sockaddr *)&server_addr,
                sizeof(server_addr)) < 0) {
        perror("connect");
        close(fd);
        return 1;
    }

    printf("Connected to server\n");

    while (true) {
        char msg[MAX_MSG];

        printf("Enter message: ");

        if (!fgets(msg, sizeof(msg), stdin)) {
            break;
        }

        // Remove newline
        size_t len = strlen(msg);

        if (len > 0 && msg[len - 1] == '\n') {
            msg[len - 1] = '\0';
            len--;
        }

        if (len > MAX_MSG) {
            printf("Message too long\n");
            continue;
        }

        uint32_t msg_len = (uint32_t)len;

        // First 4 bytes = length
        if (write(fd, &msg_len, 4) != 4) {
            perror("write length");
            break;
        }

        // Following bytes = message
        if (write(fd, msg, len) != (ssize_t)len) {
            perror("write message");
            break;
        }
        uint32_t response_len;

        if (read(fd, &response_len, 4) != 4) {
            perror("read length");
            break;
        }

        if (response_len > MAX_MSG) {
            printf("Response too large\n");
            break;
        }

        char response[MAX_MSG + 1];

        if (read(fd, response, response_len) != (ssize_t)response_len) {
            perror("read response");
            break;
        }

        response[response_len] = '\0';

        printf("Server replied: %s\n", response);
    }

    close(fd);

    return 0;
}
