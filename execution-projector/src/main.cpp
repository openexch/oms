// SPDX-License-Identifier: Apache-2.0
#include "store.hpp"
#include "monitor.hpp"
#include "client/archive/AeronArchive.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <deque>
#include <iostream>
#include <map>
#include <mutex>
#include <thread>

using namespace std::chrono_literals;
namespace {
volatile std::sig_atomic_t interrupted = 0;
void signalHandler(int) { interrupted = 1; }
std::string required(const std::map<std::string,std::string>& args, const std::string& key) {
    auto i = args.find(key);
    if (i == args.end() || i->second.empty()) throw std::runtime_error("Missing option " + key);
    return i->second;
}
struct Queue : oe::Status {
    std::mutex mutex;
    std::condition_variable wake;
    std::deque<oe::Event> events;
    bool done = false;
    std::exception_ptr failure;
};
void writeLoop(Queue& q, oe::Store& store) {
    try {
        for (;;) {
            std::vector<oe::Event> batch; batch.reserve(256);
            {
                std::unique_lock lock(q.mutex);
                q.wake.wait_for(lock, 1s, [&] { return q.done || !q.events.empty(); });
                if (q.events.empty() && q.done) break;
                while (!q.events.empty() && batch.size() < 256) {
                    batch.push_back(std::move(q.events.front())); q.events.pop_front();
                }
            }
            if (batch.empty()) store.probe(); else store.apply(batch);
            q.committed.store(store.position());
            q.queued.fetch_sub(static_cast<std::int64_t>(batch.size()));
            q.databaseMs.store(oe::monotonicMs());
        }
    } catch (...) {
        std::lock_guard lock(q.mutex); q.failure = std::current_exception(); q.failed.store(true);
    }
}
}

int main(int argc, char** argv) {
    try {
        std::map<std::string,std::string> args;
        for (int i = 1; i < argc; i += 2) {
            if (i + 1 >= argc || !args.emplace(argv[i], argv[i + 1]).second)
                throw std::runtime_error("Options require unique --name value pairs");
        }
        const std::vector<std::string> names{"--aeron-dir","--control-channel","--control-stream",
            "--response-channel","--replay-channel","--replay-stream","--recording","--source","--mode","--monitor-port"};
        for (const auto& [key, value] : args)
            if (std::find(names.begin(), names.end(), key) == names.end()) throw std::runtime_error("Unknown option " + key);
        const auto mode = required(args, "--mode");
        if (mode != "once" && mode != "follow") throw std::runtime_error("--mode must be once or follow");
        const auto recording = std::stoll(required(args, "--recording"));
        if (recording < 0) throw std::runtime_error("Recording id must be nonnegative");
        const char* conninfo = std::getenv("OE_PROJECTOR_PG");
        if (!conninfo || !*conninfo) throw std::runtime_error("OE_PROJECTOR_PG is required (prefer a libpq service)");
        aeron::archive::client::Context context;
        context.aeronDirectoryName(required(args, "--aeron-dir"))
            .controlRequestChannel(required(args, "--control-channel"))
            .controlRequestStreamId(std::stoi(required(args, "--control-stream")))
            .controlResponseChannel(required(args, "--response-channel"))
            .messageTimeoutNs(5'000'000'000ULL);
        auto archive = aeron::archive::client::AeronArchive::connect(context);
        std::string descriptor;
        std::int64_t start = -1;
        if (archive->listRecording(recording, [&](aeron::archive::client::RecordingDescriptor& d) {
            start = d.m_startPosition;
            descriptor = std::to_string(archive->archiveId()) + "/" + std::to_string(d.m_recordingId)
                + "/" + std::to_string(d.m_startTimestamp) + "/" + std::to_string(d.m_initialTermId)
                + "/" + std::to_string(d.m_sessionId) + "/" + std::to_string(d.m_streamId)
                + "/" + std::to_string(d.m_termBufferLength) + "/" + d.m_originalChannel;
        }) != 1) throw std::runtime_error("Pinned journal recording not found");
        oe::Store store(conninfo, required(args, "--source"), descriptor);
        if (store.position() < start) throw std::runtime_error("Required replay prefix has been removed");
        auto target = archive->getMaxRecordedPosition(recording);
        if (target < store.position()) throw std::runtime_error("Recording is behind the durable checkpoint");
        if (mode == "once" && target == store.position()) {
            std::cout << "caught_up position=" << target << '\n'; return 0;
        }
        aeron::archive::client::ReplayParams params;
        params.position(store.position()).length(mode == "once" ? target - store.position() : INT64_MAX);
        auto subscription = archive->replay(recording, required(args, "--replay-channel"),
                std::stoi(required(args, "--replay-stream")), params);
        Queue queue; queue.committed.store(store.position()); queue.target.store(target);
        queue.pollMs.store(oe::monotonicMs()); queue.databaseMs.store(oe::monotonicMs());
        oe::Monitor monitor(queue, std::stoi(required(args, "--monitor-port")));
        std::jthread writer([&] { writeLoop(queue, store); });
        std::signal(SIGINT, signalHandler); std::signal(SIGTERM, signalHandler);
        std::exception_ptr pollFailure;
        auto lastProgress = std::chrono::steady_clock::now();
        auto lastControl = lastProgress;
        auto consumed = store.position();
        try {
            while (!interrupted && !queue.failed.load()) {
                if (mode == "once" && queue.committed.load() == target) break;
                int work = subscription->controlledPoll([&](aeron::AtomicBuffer& buffer,
                        aeron::util::index_t offset, aeron::util::index_t length, aeron::Header& header) {
                    try {
                        if (header.flags() != 0xC0 || length < 8 || length > 101)
                            throw std::runtime_error("Unexpected journal fragmentation/size");
                        std::lock_guard lock(queue.mutex);
                        if (queue.events.size() >= 1024) return aeron::ControlledPollAction::ABORT;
                        oe::Event event{header.position(), {buffer.buffer() + offset, buffer.buffer() + offset + length}};
                        oe::decode(event.bytes); // Validate before accepting into the bounded queue.
                        if (event.position <= consumed) throw std::runtime_error("Non-monotonic archive replay");
                        consumed = event.position;
                        queue.target.store(std::max(queue.target.load(), consumed));
                        queue.queued.fetch_add(1);
                        queue.events.push_back(std::move(event)); queue.wake.notify_one();
                        return aeron::ControlledPollAction::CONTINUE;
                    } catch (...) {
                        pollFailure = std::current_exception(); return aeron::ControlledPollAction::ABORT;
                    }
                }, 32);
                if (pollFailure) std::rethrow_exception(pollFailure);
                queue.pollMs.store(oe::monotonicMs());
                queue.streamConnected.store(subscription->isConnected());
                auto now = std::chrono::steady_clock::now();
                if (work) lastProgress = now;
                if (now - lastControl >= 1s) {
                    archive->pollForRecordingSignals();
                    auto error = archive->pollForErrorResponse();
                    if (!error.empty()) throw std::runtime_error("Archive reported a replay error");
                    const auto maxPosition = archive->getMaxRecordedPosition(recording);
                    if (maxPosition < consumed) throw std::runtime_error("Archive position regressed");
                    target = mode == "once" ? target : maxPosition;
                    queue.target.store(target);
                    if (now - lastProgress > 15s && (consumed < target || !subscription->isConnected()))
                        throw std::runtime_error("Replay stalled or recording stopped; recovery required");
                    lastControl = now;
                }
                if (!work) std::this_thread::sleep_for(1ms);
            }
        } catch (...) { queue.failed.store(true); pollFailure = std::current_exception(); }
        {
            std::lock_guard lock(queue.mutex); queue.done = true; queue.wake.notify_one();
        }
        writer.join();
        if (queue.failure) std::rethrow_exception(queue.failure);
        if (pollFailure) std::rethrow_exception(pollFailure);
        std::cout << (interrupted ? "stopped" : "caught_up") << " position=" << queue.committed.load()
                  << " target=" << target << '\n';
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "execution-projector halted: " << e.what() << '\n'; return 1;
    }
}
