// Exercise the owning store, record codec and WAL in a browser virtual filesystem.
#include "store.h"
#include <emscripten/emscripten.h>
#include <filesystem>
#include <sstream>
#include <string>

namespace fs = std::filesystem;
static std::string output;
static std::uintmax_t bytes() {
    std::uintmax_t n = 0;
    for (const auto& entry : fs::recursive_directory_iterator("/public-store"))
        if (entry.is_regular_file()) n += entry.file_size();
    return n;
}
extern "C" EMSCRIPTEN_KEEPALIVE const char* run_store(int count, int done, int cycles, int compact) {
    if (count < 1 || count > 12 || done < 0 || done > count || cycles < 1 || cycles > 8 || (compact != 0 && compact != 1)) {
        output = "{\"error\":\"Invalid bounded store inputs\"}";
        return output.c_str();
    }
    try {
        fs::remove_all("/public-store");
        std::uintmax_t before = 0, after = 0;
        {
            tautq::JobStore store;
            if (!store.open("/public-store")) throw std::runtime_error("open");
            for (int i = 0; i < count; ++i) {
                tautq::Job job;
                job.id = {1, static_cast<std::uint64_t>(i + 1)};
                job.idem_key = "synthetic-" + std::to_string(i);
                job.url = "https://unused.synthetic.invalid";
                job.body.resize(32, std::byte{42});
                job.max_attempts = 12;
                if (!store.commit(tautq::SubmitRec{job})) throw std::runtime_error("submit");
                for (int grant = 1; grant <= cycles; ++grant)
                    if (!store.commit(tautq::LeaseRec{job.id, 1, static_cast<std::uint32_t>(grant), 7})) throw std::runtime_error("lease");
                if (i < done) {
                    if (!store.commit(tautq::DoneRec{job.id, 1, static_cast<std::uint32_t>(cycles)})) throw std::runtime_error("done");
                    // Delayed lease records must not resurrect completed work.
                    if (!store.commit(tautq::LeaseRec{job.id, 1, static_cast<std::uint32_t>(cycles + 1), 8})) throw std::runtime_error("late lease");
                }
            }
            before = bytes();
            if (compact && !store.compact()) throw std::runtime_error("compact");
            after = bytes();
        }
        int recovered_done = 0, recovered_leased = 0, recovered_keys = 0;
        {
            tautq::JobStore recovered;
            if (!recovered.open("/public-store")) throw std::runtime_error("reopen");
            for (const auto& [id, job] : recovered.jobs()) {
                (void)id;
                recovered_done += job.state == tautq::JobState::Done;
                recovered_leased += job.state == tautq::JobState::Leased;
            }
            for (int i = 0; i < count; ++i) recovered_keys += recovered.find_by_idem("synthetic-" + std::to_string(i)) != nullptr;
        }
        std::ostringstream json;
        json << "{\"jobs\":" << count << ",\"completed_after_reopen\":" << recovered_done
             << ",\"leased_after_reopen\":" << recovered_leased << ",\"idempotency_keys_after_reopen\":" << recovered_keys
             << ",\"virtual_wal_bytes_before\":" << before << ",\"virtual_wal_bytes_after\":" << after
             << ",\"compacted\":" << (compact ? "true" : "false") << "}";
        output = json.str();
        fs::remove_all("/public-store");
    } catch (...) { output = "{\"error\":\"Store run failed; no result is available\"}"; }
    return output.c_str();
}
