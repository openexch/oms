// SPDX-License-Identifier: Apache-2.0
#include "monitor.hpp"
#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <chrono>
#include <stdexcept>
#include <string>

namespace oe {
std::int64_t monotonicMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}
bool Status::ready() const {
    const auto now = monotonicMs();
    return !failed.load() && streamConnected.load() && now - pollMs.load() < 3000 && now - databaseMs.load() < 3000
        && committed.load() >= target.load() && queued.load() == 0;
}
Monitor::Monitor(Status& state, int port) {
    if (port < 1024 || port > 65535) throw std::runtime_error("Invalid monitor port");
    socket_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (socket_ < 0) throw std::runtime_error("Cannot create monitor socket");
    int reuse = 1; setsockopt(socket_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in address{}; address.sin_family = AF_INET; address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(socket_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0 || listen(socket_, 8) < 0) {
        close(socket_); socket_ = -1; throw std::runtime_error("Cannot bind loopback monitor");
    }
    thread_ = std::jthread([this, &state](std::stop_token stop) {
        while (!stop.stop_requested()) {
            pollfd server{socket_, POLLIN, 0};
            if (poll(&server, 1, 100) <= 0) continue;
            int client = accept4(socket_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (client < 0) continue;
            pollfd input{client, POLLIN, 0};
            if (poll(&input, 1, 100) <= 0) { close(client); continue; }
            char bytes[1024]; auto length = recv(client, bytes, sizeof(bytes), 0);
            if (length <= 0) { close(client); continue; }
            std::string request(bytes, length), body;
            int code = 200;
            if (request.starts_with("GET /ready ")) {
                bool ready = state.ready(); code = ready ? 200 : 503;
                body = std::string("{\"ready\":") + (ready ? "true" : "false") +
                    ",\"checkpoint\":" + std::to_string(state.committed.load()) +
                    ",\"target\":" + std::to_string(state.target.load()) + "}\n";
            } else if (request.starts_with("GET /health ")) {
                bool alive = !state.failed.load() && monotonicMs() - state.pollMs.load() < 3000;
                code = alive ? 200 : 503; body = alive ? "{\"alive\":true}\n" : "{\"alive\":false}\n";
            } else if (request.starts_with("GET /metrics ")) {
                auto committed = state.committed.load(), target = state.target.load();
                body = "execution_projector_ready " + std::to_string(state.ready()) +
                    "\nexecution_projector_checkpoint " + std::to_string(committed) +
                    "\nexecution_projector_target " + std::to_string(target) +
                    "\nexecution_projector_lag_bytes " + std::to_string(std::max<std::int64_t>(0, target-committed)) +
                    "\nexecution_projector_queued " + std::to_string(state.queued.load()) +
                    "\nexecution_projector_failed " + std::to_string(state.failed.load()) + "\n";
            } else { code = 404; body = "Not found\n"; }
            auto response = "HTTP/1.1 " + std::to_string(code) + (code == 200 ? " OK\r\n" : " Unavailable\r\n") +
                "Connection: close\r\nContent-Length: " + std::to_string(body.size()) + "\r\n\r\n" + body;
            std::size_t sent = 0;
            auto deadline = monotonicMs() + 100;
            while (sent < response.size() && monotonicMs() < deadline) {
                auto count = send(client, response.data()+sent, response.size()-sent, MSG_NOSIGNAL);
                if (count > 0) sent += count;
                else { pollfd output{client, POLLOUT, 0}; if (poll(&output, 1, 10) < 0) break; }
            }
            close(client);
        }
    });
}
Monitor::~Monitor() { thread_.request_stop(); if (thread_.joinable()) thread_.join(); if (socket_ >= 0) close(socket_); }
}
