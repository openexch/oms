// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <atomic>
#include <cstdint>
#include <thread>
namespace oe {
std::int64_t monotonicMs();
struct Status {
    std::atomic<std::int64_t> committed{0}, target{0}, pollMs{0}, databaseMs{0}, queued{0};
    std::atomic<bool> failed{false}, streamConnected{false};
    bool ready() const;
};
// Loopback-only HTTP probes, on a separate thread with bounded I/O waits.
class Monitor {
public:
    Monitor(Status& status, int port);
    ~Monitor();
    Monitor(const Monitor&) = delete;
    Monitor& operator=(const Monitor&) = delete;
private:
    int socket_ = -1;
    std::jthread thread_;
};
}
