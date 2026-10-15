//! Vendored subset of UbiQUIC eXtreme used by Qdrant.zig.
pub const event = @import("event.zig");
pub const delivery = @import("delivery.zig");
pub const wire = @import("wire.zig");
pub const nats_protocol = @import("nats_protocol.zig");
pub const nats_client = @import("nats_client.zig");
