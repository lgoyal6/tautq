// tautq-verify: the chaos suite's judge. Reads every node's WAL, the submitter's record of
// accepted submits, and the sink's receipt log, then asserts the §5 contract:
//
//   1. NO LOSS        — every accepted job's merged cluster state is Done (or DeadLetter
//                       only if --allow-dlq, for scenarios that inject delivery failures).
//   2. EXACTLY-ONCE   — per job, every DONE record across every log carries the same
//      COMPLETION       lease_seq: one completion, however many times it replicated.
//   3. ATTRIBUTABLE   — a key delivered more than once at the sink must belong to a job
//      DUPLICATES       whose logs show >1 lease grant or an ownership change; duplicates
//                       with no such cause are protocol bugs.
//
//   tautq-verify --submits submits.csv --sink sink.log --logs dir0,dir1,...  [--allow-dlq]
//
// Exit 0 = PASS. Everything it checks is on disk — rerunnable after the fact.

#include <cstdio>
#include <cstring>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "records.h"
#include "wal.h"

using namespace tautq;

namespace {

struct JobFacts {
    Job merged; // most-advanced full copy seen
    bool have_copy = false;
    std::set<std::uint32_t> done_seqs; // distinct lease_seq across all DONE records
    std::uint32_t max_lease_seq = 0;   // grants observed anywhere
    std::uint32_t max_epoch = 1;
};

} // namespace

int main(int argc, char** argv) {
    std::string submits_path;
    std::string sink_path;
    std::string logs_csv;
    bool allow_dlq = false;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        const auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
        if (a == "--submits") {
            submits_path = next();
        } else if (a == "--sink") {
            sink_path = next();
        } else if (a == "--logs") {
            logs_csv = next();
        } else if (a == "--allow-dlq") {
            allow_dlq = true;
        } else {
            std::fprintf(stderr, "unknown arg %s\n", a.c_str());
            return 2;
        }
    }

    // ---- fold every node's WAL ---------------------------------------------------------
    std::map<std::string, JobFacts> jobs;                 // by hex id
    std::map<std::string, std::set<std::string>> key_ids; // idem key -> distinct job ids
    std::size_t log_dirs = 0;
    std::size_t pos = 0;
    while (pos < logs_csv.size()) {
        std::size_t comma = logs_csv.find(',', pos);
        if (comma == std::string::npos) {
            comma = logs_csv.size();
        }
        const std::string dir = logs_csv.substr(pos, comma - pos);
        pos = comma + 1;
        if (dir.empty()) {
            continue;
        }
        ++log_dirs;
        Wal wal;
        const bool ok = wal.open(dir, [&](std::span<const std::byte> body) {
            const auto rec = decode_record(body);
            if (!rec) {
                return;
            }
            const auto fold_job = [&](const Job& j) {
                JobFacts& f = jobs[to_hex(j.id)];
                if (!f.have_copy || job_advances(f.merged, j)) {
                    f.merged = j;
                }
                f.have_copy = true;
                f.max_lease_seq = std::max(f.max_lease_seq, j.lease_seq);
                f.max_epoch = std::max(f.max_epoch, j.epoch);
                key_ids[j.idem_key].insert(to_hex(j.id));
            };
            if (const auto* s = std::get_if<SubmitRec>(&*rec)) {
                fold_job(s->job);
            } else if (const auto* r = std::get_if<ReplicateRec>(&*rec)) {
                fold_job(r->job);
            } else if (const auto* l = std::get_if<LeaseRec>(&*rec)) {
                JobFacts& f = jobs[to_hex(l->id)];
                f.max_lease_seq = std::max(f.max_lease_seq, l->lease_seq);
                f.max_epoch = std::max(f.max_epoch, l->epoch);
            } else if (const auto* d = std::get_if<DoneRec>(&*rec)) {
                JobFacts& f = jobs[to_hex(d->id)];
                f.done_seqs.insert(d->lease_seq);
                Job done_marker = f.merged;
                done_marker.id = d->id;
                done_marker.state = JobState::Done;
                done_marker.epoch = std::max(f.max_epoch, d->epoch);
                if (!f.have_copy || job_advances(f.merged, done_marker)) {
                    f.merged = done_marker;
                    f.have_copy = true;
                }
            } else if (const auto* t = std::get_if<TakeoverRec>(&*rec)) {
                JobFacts& f = jobs[to_hex(t->id)];
                f.max_epoch = std::max(f.max_epoch, t->new_epoch);
            } else if (const auto* dl = std::get_if<DeadLetterRec>(&*rec)) {
                JobFacts& f = jobs[to_hex(dl->id)];
                if (f.have_copy) {
                    Job m = f.merged;
                    m.state = JobState::DeadLetter;
                    m.epoch = std::max(f.max_epoch, dl->epoch);
                    if (job_advances(f.merged, m)) {
                        f.merged = m;
                    }
                }
            }
        });
        if (!ok) {
            std::fprintf(stderr, "FAIL: cannot replay %s\n", dir.c_str());
            return 1;
        }
    }

    // ---- accepted submits (key,id,code per line) -----------------------------------------
    struct Accepted {
        std::string id;
    };
    std::map<std::string, Accepted> accepted; // key -> id
    {
        std::ifstream in(submits_path);
        if (!in) {
            std::fprintf(stderr, "FAIL: cannot read %s\n", submits_path.c_str());
            return 1;
        }
        std::string line;
        while (std::getline(in, line)) {
            std::stringstream ss(line);
            std::string key;
            std::string id;
            std::string code;
            if (std::getline(ss, key, ',') && std::getline(ss, id, ',') &&
                std::getline(ss, code, ',')) {
                if (code == "200" || code == "201") {
                    accepted[key] = Accepted{id};
                }
            }
        }
    }

    // ---- sink receipts -------------------------------------------------------------------
    std::map<std::string, int> deliveries; // key -> count
    {
        std::ifstream in(sink_path);
        if (!in) {
            std::fprintf(stderr, "FAIL: cannot read %s\n", sink_path.c_str());
            return 1;
        }
        std::string line;
        while (std::getline(in, line)) {
            std::stringstream ss(line);
            std::string ms;
            std::string key;
            if (ss >> ms >> key) {
                deliveries[key]++;
            }
        }
    }

    // ---- the three assertions --------------------------------------------------------------
    int failures = 0;
    std::size_t lost = 0;
    std::size_t double_done = 0;
    std::size_t unexplained_dupes = 0;
    std::size_t dlq = 0;
    std::size_t twins = 0;

    for (const auto& [key, acc] : accepted) {
        auto it = jobs.find(acc.id);
        const bool done = it != jobs.end() && it->second.merged.state == JobState::Done;
        const bool dead = it != jobs.end() && it->second.merged.state == JobState::DeadLetter;
        if (dead) {
            ++dlq;
        }
        if (!done && !(allow_dlq && dead)) {
            ++lost;
            if (lost <= 10) {
                std::fprintf(stderr, "LOST: key=%s id=%s state=%d\n", key.c_str(), acc.id.c_str(),
                             it == jobs.end() ? -1 : static_cast<int>(it->second.merged.state));
            }
        }
    }
    for (const auto& [id, f] : jobs) {
        if (f.done_seqs.size() > 1) {
            ++double_done;
            std::fprintf(stderr, "DOUBLE-DONE: id=%s distinct completion seqs=%zu\n", id.c_str(),
                         f.done_seqs.size());
        }
    }
    for (const auto& [key, count] : deliveries) {
        if (count <= 1) {
            continue;
        }
        auto ait = accepted.find(key);
        const JobFacts* f = ait != accepted.end() && jobs.count(ait->second.id) != 0
                                ? &jobs[ait->second.id]
                                : nullptr;
        // Attributable iff the logs show >1 grant (expiry/nack retry), an epoch change
        // (takeover re-run), or TWO DISTINCT jobs sharing the key — the documented dedup
        // degradation when a gateway falls back to local ownership after a lost forward
        // (§2; the receiver's idempotency key still dedups). Anything else is a protocol
        // bug.
        const auto kit = key_ids.find(key);
        const bool fallback_twin = kit != key_ids.end() && kit->second.size() > 1;
        const bool attributable =
            fallback_twin || (f != nullptr && (f->max_lease_seq > 1 || f->max_epoch > 1));
        if (fallback_twin) {
            ++twins;
        }
        if (!attributable) {
            ++unexplained_dupes;
            std::fprintf(stderr, "UNEXPLAINED-DUPE: key=%s deliveries=%d\n", key.c_str(), count);
        }
    }

    if (lost != 0) {
        std::fprintf(stderr, "FAIL: %zu accepted jobs not completed\n", lost);
        failures++;
    }
    if (double_done != 0) {
        std::fprintf(stderr, "FAIL: %zu jobs with more than one completion\n", double_done);
        failures++;
    }
    if (unexplained_dupes != 0) {
        std::fprintf(stderr, "FAIL: %zu unexplained duplicate deliveries\n", unexplained_dupes);
        failures++;
    }

    std::size_t dup_total = 0;
    for (const auto& [k, c] : deliveries) {
        (void)k;
        dup_total += static_cast<std::size_t>(c > 1 ? c - 1 : 0);
    }
    std::printf("verify: logs=%zu accepted=%zu delivered_keys=%zu duplicate_deliveries=%zu "
                "(all attributable; fallback_twins=%zu) dead_letter=%zu jobs_seen=%zu -> %s\n",
                log_dirs, accepted.size(), deliveries.size(), dup_total, twins, dlq, jobs.size(),
                failures == 0 ? "PASS" : "FAIL");
    return failures == 0 ? 0 : 1;
}
