#include "wal.h"

#include <algorithm>
#include <cerrno>
#include <cstdio>
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
    dir_ = dir;
    if (::mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
        return false;
    }

    std::vector<std::string> segs;
    DIR* d = ::opendir(dir.c_str());
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
        const std::string path = dir_ + "/" + segs[i];
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
        const std::string path = dir_ + "/" + segment_name(0);
        if (!open_active(path, /*create=*/true)) {
            return false;
        }
        active_size_ = 0;
        if (!fsync_dir(dir_)) {
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

bool Wal::rotate() {
    if (::fdatasync(fd_) != 0) {
        return false;
    }
    const std::string path = dir_ + "/" + segment_name(next_lsn_);
    if (!open_active(path, /*create=*/true)) {
        return false;
    }
    active_size_ = 0;
    return fsync_dir(dir_);
}

bool Wal::append(std::span<const std::byte> body, bool do_sync) {
    if (fd_ < 0 || body.empty() || body.size() > kMaxBody) {
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
            return false; // disk full / IO error; a partial frame is next open()'s torn tail
        }
        written += static_cast<std::size_t>(n);
    }
    active_size_ += frame.size();
    ++next_lsn_;
    return do_sync ? sync() : true;
}

bool Wal::sync() {
    return fd_ >= 0 && ::fdatasync(fd_) == 0;
}

} // namespace tautq
