#include "wal.h"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "taut/crc32c.h"

#include "bytes.h"

namespace tautq {

namespace {

constexpr std::size_t kFrameHeader = 8;
constexpr std::size_t kMaxBody = 64u << 10;

bool fsync_dir(const std::string& dir) {
    const int dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dfd < 0) {
        return false;
    }
    const bool ok = ::fsync(dfd) == 0;
    ::close(dfd);
    return ok;
}

bool sync_data_fd(int fd) {
#if defined(__APPLE__)
    // Darwin does not expose fdatasync; fsync also flushes file metadata.
    return ::fsync(fd) == 0;
#else
    return ::fdatasync(fd) == 0;
#endif
}

bool read_all(const std::string& path, std::vector<std::byte>& out) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return false;
    }
    out.clear();
    std::byte buf[64 << 10];
    for (;;) {
        const ssize_t n = ::read(fd, buf, sizeof buf);
        if (n < 0) {
            ::close(fd);
            return false;
        }
        if (n == 0) {
            break;
        }
        out.insert(out.end(), buf, buf + n);
    }
    ::close(fd);
    return true;
}

std::string gen_name(std::uint64_t generation) {
    char buf[40];
    std::snprintf(buf, sizeof buf, "wal-gen-%020llu", static_cast<unsigned long long>(generation));
    return buf;
}

bool write_frame(int fd, std::span<const std::byte> body) {
    std::vector<std::byte> frame;
    frame.reserve(kFrameHeader + body.size());
    put_u32(frame, static_cast<std::uint32_t>(body.size()));
    put_u32(frame, taut::crc32c(body));
    put_bytes(frame, body);
    std::size_t written = 0;
    while (written < frame.size()) {
        const ssize_t n = ::write(fd, frame.data() + written, frame.size() - written);
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0)
            return false;
        written += static_cast<std::size_t>(n);
    }
    return true;
}

// Parses frames from `data`, invoking fn per body. Returns the offset of the first
// invalid/incomplete frame (== data.size() when the whole file is clean).
std::size_t scan_frames(std::span<const std::byte> data, const Wal::ReplayFn& fn,
                        std::uint64_t& count) {
    std::size_t off = 0;
    while (off + kFrameHeader <= data.size()) {
        const std::uint32_t len = get_u32(data, off);
        const std::uint32_t crc = get_u32(data, off + 4);
        if (len == 0 || len > kMaxBody || off + kFrameHeader + len > data.size()) {
            return off;
        }
        const auto body = data.subspan(off + kFrameHeader, len);
        if (taut::crc32c(body) != crc) {
            return off;
        }
        fn(body);
        ++count;
        off += kFrameHeader + len;
    }
    return off;
}

} // namespace

Wal::~Wal() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

std::string Wal::segment_name(std::uint64_t start_lsn) {
    char buf[40];
    std::snprintf(buf, sizeof buf, "wal-%020llu.log", static_cast<unsigned long long>(start_lsn));
    return buf;
}

bool Wal::open(const std::string& dir, const ReplayFn& fn) {
    failed_ = false;
    dir_ = dir;
    if (::mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
        return false;
    }

    active_dir_ = dir_;
    generation_ = 0;
    std::ifstream current(dir_ + "/CURRENT");
    if (current.good()) {
        if (!(current >> generation_))
            return false;
        active_dir_ = dir_ + "/" + gen_name(generation_);
    }

    std::vector<std::string> segs;
    DIR* d = ::opendir(active_dir_.c_str());
    if (d == nullptr) {
        return false;
    }
    while (const dirent* e = ::readdir(d)) {
        const std::string name = e->d_name;
        if (name.rfind("wal-", 0) == 0 && name.size() > 8 &&
            name.compare(name.size() - 4, 4, ".log") == 0) {
            segs.push_back(name);
        }
    }
    ::closedir(d);
    std::sort(segs.begin(), segs.end()); // zero-padded start-lsn names sort correctly

    next_lsn_ = 0;
    for (std::size_t i = 0; i < segs.size(); ++i) {
        const std::string path = active_dir_ + "/" + segs[i];
        std::vector<std::byte> data;
        if (!read_all(path, data)) {
            return false;
        }
        const std::size_t clean = scan_frames(data, fn, next_lsn_);
        if (clean != data.size()) {
            if (i + 1 != segs.size()) {
                return false; // corruption before the tail — replication's job, not ours
            }
            if (::truncate(path.c_str(), static_cast<off_t>(clean)) != 0) {
                return false;
            }
        }
        if (i + 1 == segs.size()) {
            if (!open_active(path, /*create=*/false)) {
                return false;
            }
            active_size_ = clean;
        }
    }

    if (fd_ < 0) {
        const std::string path = active_dir_ + "/" + segment_name(0);
        if (active_dir_ != dir_ && ::mkdir(active_dir_.c_str(), 0755) != 0 && errno != EEXIST)
            return false;
        if (!open_active(path, /*create=*/true)) {
            return false;
        }
        active_size_ = 0;
        if (!sync_directory(active_dir_)) {
            failed_ = true;
            return false;
        }
    }
    return true;
}

bool Wal::open_active(const std::string& path, bool create) {
    if (fd_ >= 0) {
        ::close(fd_);
    }
    const int flags = O_WRONLY | O_APPEND | (create ? O_CREAT : 0);
    fd_ = ::open(path.c_str(), flags, 0644);
    return fd_ >= 0;
}

bool Wal::sync_directory(const std::string& path) {
    return directory_sync_hook_ ? directory_sync_hook_(path) : fsync_dir(path);
}

bool Wal::rotate() {
    if (failed_)
        return false;
    if (!sync_data_fd(fd_)) {
        failed_ = true;
        return false;
    }
    const std::string path = active_dir_ + "/" + segment_name(next_lsn_);
    if (!open_active(path, /*create=*/true)) {
        failed_ = true;
        return false;
    }
    active_size_ = 0;
    if (!sync_directory(active_dir_)) {
        failed_ = true;
        return false;
    }
    return true;
}

bool Wal::compact(const std::vector<std::vector<std::byte>>& snapshot, const CompactHook& hook) {
    if (fd_ < 0 || failed_)
        return false;
    for (const auto& body : snapshot) {
        if (body.empty() || body.size() > kMaxBody)
            return false;
    }
    if (!sync())
        return false;

    // A new generation is fully written and synced before CURRENT is atomically replaced.
    // A crash before that rename replays the old generation; after it, replay uses the new one.
    std::uint64_t next_generation = generation_ + 1;
    std::string new_dir;
    for (;;) {
        new_dir = dir_ + "/" + gen_name(next_generation);
        if (::mkdir(new_dir.c_str(), 0755) == 0)
            break;
        if (errno != EEXIST || next_generation == UINT64_MAX)
            return false;
        ++next_generation; // a prior interrupted attempt may have left an uninstalled generation
    }
    std::size_t size = 0;
    std::uint64_t lsn = 0;
    int out = -1;
    auto open_segment = [&](std::uint64_t start) {
        if (out >= 0) {
            if (!sync_data_fd(out))
                return false;
            ::close(out);
        }
        out = ::open((new_dir + "/" + segment_name(start)).c_str(),
                     O_WRONLY | O_CREAT | O_EXCL | O_APPEND, 0644);
        size = 0;
        return out >= 0;
    };
    bool ok = open_segment(0);
    for (const auto& body : snapshot) {
        if (!ok)
            break;
        const std::size_t frame_size = kFrameHeader + body.size();
        if (size > 0 && size + frame_size > segment_bytes_)
            ok = open_segment(lsn);
        if (ok)
            ok = write_frame(out, body);
        if (ok) {
            size += frame_size;
            ++lsn;
        }
    }
    if (out >= 0) {
        if (!sync_data_fd(out))
            ok = false;
        ::close(out);
    }
    if (!ok || !sync_directory(new_dir) || !sync_directory(dir_))
        return false;
    if (hook && !hook(CompactStage::SnapshotDurable))
        return false;

    const std::string pointer_tmp = dir_ + "/CURRENT.tmp";
    const int pfd = ::open(pointer_tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (pfd < 0)
        return false;
    const std::string value = std::to_string(next_generation) + "\n";
    std::size_t written = 0;
    while (written < value.size()) {
        const ssize_t n = ::write(pfd, value.data() + written, value.size() - written);
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0) {
            ::close(pfd);
            return false;
        }
        written += static_cast<std::size_t>(n);
    }
    if (!sync_data_fd(pfd)) {
        ::close(pfd);
        return false;
    }
    ::close(pfd);
    if (::rename(pointer_tmp.c_str(), (dir_ + "/CURRENT").c_str()) != 0)
        return false;

    // CURRENT may now name the new generation. Until its parent directory is durable,
    // writes to that generation cannot safely be acknowledged across a crash.
    failed_ = true;
    if (!sync_directory(dir_))
        return false;

    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    active_dir_ = new_dir;
    generation_ = next_generation;
    // Locate the last segment because it may begin after one or more snapshot rotations.
    std::vector<std::string> names;
    DIR* nd = ::opendir(active_dir_.c_str());
    if (nd == nullptr)
        return false;
    while (const dirent* e = ::readdir(nd)) {
        const std::string n = e->d_name;
        if (n.rfind("wal-", 0) == 0 && n.size() > 8 && n.compare(n.size() - 4, 4, ".log") == 0)
            names.push_back(n);
    }
    ::closedir(nd);
    std::sort(names.begin(), names.end());
    const std::string selected =
        names.empty() ? active_dir_ + "/" + segment_name(0) : active_dir_ + "/" + names.back();
    if (!open_active(selected, /*create=*/names.empty()))
        return false;
    struct stat st{};
    if (::fstat(fd_, &st) != 0)
        return false;
    active_size_ = static_cast<std::size_t>(st.st_size);
    next_lsn_ = lsn;
    failed_ = false;
    if (hook && !hook(CompactStage::Installed)) {
        failed_ = true;
        return false;
    }
    if (hook && !hook(CompactStage::BeforeOldLogDeletion)) {
        failed_ = true;
        return false;
    }

    // The pointer is durable before cleanup. Old data is now unreachable from CURRENT.
    DIR* root = ::opendir(dir_.c_str());
    if (root == nullptr)
        return false;
    while (const dirent* e = ::readdir(root)) {
        const std::string name = e->d_name;
        if (name.rfind("wal-gen-", 0) == 0 && dir_ + "/" + name != active_dir_) {
            std::error_code ec;
            std::filesystem::remove_all(dir_ + "/" + name, ec);
            if (ec) {
                ::closedir(root);
                return false;
            }
        } else if (name.rfind("wal-", 0) == 0 && name.size() > 8 &&
                   name.compare(name.size() - 4, 4, ".log") == 0) {
            if (::unlink((dir_ + "/" + name).c_str()) != 0 && errno != ENOENT) {
                ::closedir(root);
                return false;
            }
        }
    }
    ::closedir(root);
    return fsync_dir(dir_);
}

bool Wal::append(std::span<const std::byte> body, bool do_sync) {
    if (fd_ < 0 || failed_ || body.empty() || body.size() > kMaxBody) {
        return false;
    }
    const std::size_t frame_size = kFrameHeader + body.size();
    if (active_size_ > 0 && active_size_ + frame_size > segment_bytes_) {
        if (!rotate()) {
            return false;
        }
    }

    std::vector<std::byte> frame;
    frame.reserve(frame_size);
    put_u32(frame, static_cast<std::uint32_t>(body.size()));
    put_u32(frame, taut::crc32c(body));
    put_bytes(frame, body);

    std::size_t written = 0;
    while (written < frame.size()) {
        const ssize_t n = ::write(fd_, frame.data() + written, frame.size() - written);
        if (n <= 0) {
            if (errno == EINTR) {
                continue;
            }
            failed_ = true;
            return false; // disk full / IO error; a partial frame is next open()'s torn tail
        }
        written += static_cast<std::size_t>(n);
    }
    active_size_ += frame.size();
    ++next_lsn_;
    return do_sync ? sync() : true;
}

bool Wal::sync() {
    if (fd_ < 0 || failed_)
        return false;
    if (!sync_data_fd(fd_)) {
        failed_ = true;
        return false;
    }
    return true;
}

} // namespace tautq
