# Browser store lab

`public.cc` calls the original JobStore, record codec, WAL and compactor on bounded synthetic jobs. Reopening replays the generated WAL; delayed lease records must not resurrect completed jobs. The browser uses Emscripten's virtual filesystem in a disposable worker. This is not a physical disk, fsync durability, power-loss or five-node network experiment.

Build with Emscripten 6.0.12 and a clean taut checkout at `a7720b961e0240ecf28e6fd699270dc926c85e57`:

```sh
TAUT_SOURCE=/absolute/path/to/taut bash wasm/build.sh
python3 -m http.server 4322 --directory docs
```

The build pins the dependency and uses a 1 MiB stack because the native WAL reader has a 64 KiB local buffer. The original native source is unchanged. The lab validates 1-12 jobs and 1-8 lease records before calling the engine, and the engine independently validates these bounds. It terminates a failed or timed-out worker and clears results after input changes. The recorded benchmark below the lab remains a separate study.
