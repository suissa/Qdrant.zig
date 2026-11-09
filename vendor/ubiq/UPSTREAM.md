# UbiQ vendoring information

These files are a source snapshot of
[suissa/UbiQUIC-eXtreme-zero-trust-message-queue](https://github.com/suissa/UbiQUIC-eXtreme-zero-trust-message-queue)
at commit `f94d40d0d5f436cbf6bcb2a30c2bc6ab66f10e0c`.

Only the canonical event, memory delivery, wire codec, NATS protocol and NATS
client modules needed by Qdrant.zig are included. The upstream project does not
currently publish a `build.zig.zon` package, so the pinned source is vendored to
keep builds reproducible. See `LICENSE` for the upstream MIT license.
