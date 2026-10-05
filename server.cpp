#include <cstdint>
#include <vector>
#include <poll.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <cstddef>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <cassert>


using namespace std;


#define PORT "3000"
#define k_max_msg 4096

enum {
    STATE_REQ = 0,
    STATE_RES = 1,
    STATE_END = 2, // mark the connection for deletion
};

struct Conn {
    int fd = -1;
    uint32_t state = 0; // either STATE_REQ or STATE_RES
    size_t rbuf_size = 0;
    uint8_t rbuf[4 + k_max_msg];
    // buffer for writing 
    size_t wbuf_size = 0;
    size_t wbuf_sent = 0;
    uint8_t wbuf[4 + k_max_msg];
};

int get_listening_socket(void) {
    int listner; // Listening socket descritor
    int yes = 1; // For setsockopt() SO_REUSEADDR, below
    int rv;

    struct addrinfo hint, *res, *p;

    memset(&hint, 0, sizeof hint);

    hint.ai_flags = AI_PASSIVE;
    hint.ai_family = AF_INET;
    hint.ai_socktype = SOCK_STREAM;
    hint.ai_protocol = 0;

    if((rv = getaddrinfo(NULL, PORT, &hint, &res)) != 0) {
        fprintf(stderr, "pollserver: %s\n", gai_strerror(rv));
        exit(1);
    }

    for(p = res; p != NULL; p = p -> ai_next) {
        if((listner = socket(p -> ai_family, p -> ai_socktype, p -> ai_protocol)) == -1) {
            perror("server: socket");
            continue;
        }

        if(setsockopt(listner, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
            perror("setsockopt");
            exit(1);
        }

        if(bind(listner, p -> ai_addr, p -> ai_addrlen) == -1) {
            close(listner);
            perror("server: bind");
            continue;
        }

        break;
    }

    freeaddrinfo(res);

    if(listen(listner, 10) == -1) {
        return -1;
    }

    return listner;
}

void fd_set_nb(int fd) {
    if(fcntl(fd, F_SETFL, O_NONBLOCK) == -1) {
        perror("fcntl");
        exit(1);
    }
}

static void conn_put(vector<Conn *> &fd2conn, struct Conn *conn) {
    if(fd2conn.size() <= (size_t) conn -> fd) {
        fd2conn.resize(conn -> fd + 1);
    }
    fd2conn[conn->fd] = conn;
}

static int32_t accept_new_conn(vector<Conn *> &fd2conn, int fd) {
    struct sockaddr_in client_addr = {};
    socklen_t socklen = sizeof(client_addr);
    int connfd = accept(fd, (struct sockaddr *)& client_addr, &socklen);
    if(connfd < 0) {
        return -1;
    }

    // set new connection fd to nonblocking mode
    fd_set_nb(connfd);

    // creating the struct Conn
    struct Conn *conn = (struct Conn*)malloc(sizeof (Conn));
    if(!conn) {
        close(connfd);
        return -1;
    }

    conn -> fd = connfd;
    conn -> state = STATE_REQ;
    conn -> rbuf_size = 0;
    conn -> wbuf_size = 0;
    conn -> wbuf_sent = 0;
    conn_put(fd2conn, conn);
    return 0;
}

static bool try_flush_buffer(Conn *conn) {
    ssize_t rv = 0; 
    do{
        size_t remain = conn -> wbuf_size - conn -> wbuf_sent;
        rv = write(conn -> fd, &conn -> wbuf[conn -> wbuf_sent], remain);
    }while(rv < 0 && errno == EINTR);

    if(rv < 0 && errno == EAGAIN) {
        // go EAGAIN, stop.
        return false;
    }
    
    if(rv < 0) {
        printf("write() error");
        conn -> state = STATE_END;
        return false;
    }

    conn -> wbuf_sent += (size_t)rv;
    assert(conn -> wbuf_sent <= conn -> wbuf_size);

    if(conn -> wbuf_sent == conn -> wbuf_size) {
        // response was fully sent, change state back
        conn -> state = STATE_REQ;
        conn -> wbuf_sent = 0;
        conn -> wbuf_size = 0;
        return false;
    }

    // still got some data in wbuf, could try to write again
    return true;
}


static void state_res(Conn *conn) {
    while(try_flush_buffer(conn)) {}
}

static bool try_one_request (Conn *conn) {
    // try to parse a request from the buffer
    if (conn -> rbuf_size < 4) {
        // not enough data in the buffer. Will retry in the next iteration
        return false;
    }

    uint32_t len = 0;
    memcpy(&len, &conn -> rbuf[0], 4);
    if(len > k_max_msg) {
        printf("too long");
        return false;
    }

    if(4 + len > conn -> rbuf_size) {
        // not enough data in the buffer, Will retry in the next iteration
        return false;
    }

    // got one request, do something with it
    printf("client says: %.*s\n", len, &conn -> rbuf[4]);

    // generating echoing response
    memcpy(&conn -> wbuf[0], &len, 4);
    memcpy(&conn -> wbuf[4], &conn -> rbuf[4], len);
    conn -> wbuf_size = 4 + len;

    // remove the request from the buffer
    // note: frequnt memmove is inefficient
    // note: need buffer handling for production code.
    size_t remain = conn -> rbuf_size - 4 - len;
    if(remain) {
        memmove(conn->rbuf,
            &conn->rbuf[4 + len],
            (remain));
    }
    conn->rbuf_size = remain;

    // change state
    conn -> state = STATE_RES; 
    state_res(conn);

    // continue the outer loop if the requesst fully processed
    return (conn -> state == STATE_REQ);
}

static bool try_fill_buffer(Conn *conn) {
    assert(conn -> rbuf_size < sizeof(conn -> rbuf));
    ssize_t rv = 0;
    do{
        size_t cap = sizeof(conn -> rbuf) - conn -> rbuf_size;
        rv = read(conn -> fd, &conn -> rbuf[conn -> rbuf_size], cap);
    } while(rv < 0 && errno == EINTR);

    if(rv < 0 && errno == EAGAIN) {
        return false;
    }
    
    if(rv < 0) {
        conn -> state = STATE_END;
        return false;
    }

    if(rv == 0) {
        if(conn -> rbuf_size > 0) {
            printf("unexpected EOF");
        } else {
            printf("EOF");
        }
        conn ->state = STATE_END;
        return false;
    }


    conn -> rbuf_size += (size_t) rv;
    assert(conn -> rbuf_size <= sizeof(conn -> rbuf) - conn -> rbuf_size);

    // Try to process requests one by one
    while(try_one_request(conn)) {}
    return (conn -> state == STATE_REQ);
}

static void state_req(Conn *conn) {
    while(try_fill_buffer(conn)) {}
}

static void connection_io (Conn *conn) {
    if(conn -> state == STATE_REQ) {
        state_req(conn);
    } else if(conn -> state == STATE_RES) {
        state_res(conn);
    } else {
        assert(0); // not expected
    }
}

int main() {
    int fd = get_listening_socket();

    // a map of all client connection, keyed by fd
    vector<Conn *> fd2conn;

    // set the listen fd to nonblocking mode
    fd_set_nb(fd); 

    // the event loop
    vector<struct pollfd> poll_args;

    while(true) {
        // prepare the argument of poll
        poll_args.clear();
        // prepare the arguments of the poll
        struct pollfd pfd = {fd, POLLIN, 0};


        poll_args.push_back(pfd);

        // connection fds
        for(Conn *conn : fd2conn)  {
            if(!conn) {
                continue;
            }

            struct pollfd pfd = {};
            pfd.fd = conn -> fd;
            pfd.events = (conn -> state == STATE_REQ)? POLLIN: POLLOUT;
            pfd.events = pfd.events | POLLERR;
            poll_args.push_back(pfd);
        }

        int rv = poll(poll_args.data(),(nfds_t) poll_args.size(), 1000);
        if(rv < 0) {
            return -1;
        }
        
        // process active connections
        for(size_t i = 1; i < poll_args.size(); i++) {
            if(poll_args[i].revents) {
                Conn *conn = fd2conn[poll_args[i].fd];
                connection_io(conn);
                if(conn -> state == STATE_END) {
                    // client closed normally, or something bad happened
                    // destroy the connection
                    fd2conn[conn -> fd] = NULL;
                    (void) close(conn -> fd);
                    free(conn);
                }
            }
        }

        // try to accept a new connection if the listening fd is active
        if(poll_args[0].revents) {
            (void) accept_new_conn(fd2conn, fd);
        }
    }

    return 0;
}
