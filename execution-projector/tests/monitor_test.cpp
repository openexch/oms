// SPDX-License-Identifier: Apache-2.0
#include "monitor.hpp"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>
#include <stdexcept>
#include <string>

static int probe(int port, const char* path) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    timeval timeout{1,0}; setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&timeout,sizeof(timeout));
    sockaddr_in address{}; address.sin_family=AF_INET; address.sin_port=htons(port);
    address.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
    if (connect(fd,reinterpret_cast<sockaddr*>(&address),sizeof(address)) < 0) {
        close(fd); throw std::runtime_error("Probe connection failed");
    }
    auto request=std::string("GET ")+path+" HTTP/1.1\r\nHost: localhost\r\n\r\n";
    send(fd,request.data(),request.size(),MSG_NOSIGNAL);
    char reply[1024]; auto n=recv(fd,reply,sizeof(reply),0); close(fd);
    if(n<12) throw std::runtime_error("Missing HTTP response");
    return std::stoi(std::string(reply+9,3));
}
int main() {
    try {
        oe::Status status;
        // Bind a process-specific high port; a collision fails visibly.
        int port=20000+getpid()%20000;
        oe::Monitor monitor(status,port);
        auto expect=[&](const char* path,int expected) {
            if(probe(port,path)!=expected) throw std::runtime_error(std::string("Incorrect probe: ")+path);
        };
        expect("/ready",503);
        status.pollMs=oe::monotonicMs(); status.databaseMs=oe::monotonicMs();
        status.target=100;
        expect("/health",200); expect("/ready",503);
        status.committed=100; status.streamConnected=true; expect("/ready",200);
        status.queued=1; expect("/ready",503); status.queued=0;
        status.databaseMs=oe::monotonicMs()-5000; expect("/ready",503); expect("/health",200);
        status.databaseMs=oe::monotonicMs(); status.streamConnected=false; expect("/ready",503);
        status.streamConnected=true; status.pollMs=oe::monotonicMs()-5000; expect("/health",503);
        status.pollMs=oe::monotonicMs(); status.failed=true; expect("/ready",503); expect("/health",503);
        expect("/metrics",200); expect("/unknown",404);
        std::cout << "PASS public HTTP readiness: lag, pending commit, idle, DB freshness, replay connection, poll heartbeat and failure\n";
    } catch(const std::exception& e) { std::cerr<<e.what()<<'\n'; return 1; }
}
