//! Event-driven Qdrant interface built on the UbiQ canonical envelope.
//!
//! Subjects start with `<action>.<entity/property>` as requested. UbiQ requires
//! a final lifecycle state, therefore wire subjects are, for example,
//! `create.collection.request`, `create.collection.ok`, and
//! `create.collection.error`.

const std = @import("std");
const qdrant = @import("qdrant.zig");
const ubiq = @import("ubiq");

pub const event = ubiq.event;
pub const wire = ubiq.wire;

pub const Action = enum {
    create_collection,
    update_collection,
    delete_collection,
    get_collection,
    list_collection,
    upsert_collection,
    delete_point,
    get_point,
    update_vector,
    delete_vector,
    set_payload,
    overwrite_payload,
    delete_payload,
    clear_payload,
    create_index,
    delete_index,
    search_point,
    search_batch,
    search_group,
    scroll_point,
    recommend_point,
    recommend_batch,
    recommend_group,
    discover_point,
    discover_batch,
    count_point,
    update_batch,
    query_point,
    query_batch,
    query_group,
    facet_point,
    search_matrix_pairs,
    search_matrix_offsets,
    create_snapshot,
    list_snapshot,
    delete_snapshot,
    create_full_snapshot,
    list_full_snapshot,
    delete_full_snapshot,
};

pub fn subject(action: Action) []const u8 {
    return switch (action) {
        .create_collection => "create.collection",
        .update_collection => "update.collection",
        .delete_collection => "delete.collection",
        .get_collection => "get.collection",
        .list_collection => "list.collection",
        .upsert_collection => "upsert.collection",
        .delete_point => "delete.point",
        .get_point => "get.point",
        .update_vector => "update.vector",
        .delete_vector => "delete.vector",
        .set_payload => "set.payload",
        .overwrite_payload => "overwrite.payload",
        .delete_payload => "delete.payload",
        .clear_payload => "clear.payload",
        .create_index => "create.index",
        .delete_index => "delete.index",
        .search_point => "search.point",
        .search_batch => "search.batch",
        .search_group => "search.group",
        .scroll_point => "scroll.point",
        .recommend_point => "recommend.point",
        .recommend_batch => "recommend.batch",
        .recommend_group => "recommend.group",
        .discover_point => "discover.point",
        .discover_batch => "discover.batch",
        .count_point => "count.point",
        .update_batch => "update.batch",
        .query_point => "query.point",
        .query_batch => "query.batch",
        .query_group => "query.group",
        .facet_point => "facet.point",
        .search_matrix_pairs => "search.matrixPairs",
        .search_matrix_offsets => "search.matrixOffsets",
        .create_snapshot => "create.snapshot",
        .list_snapshot => "list.snapshot",
        .delete_snapshot => "delete.snapshot",
        .create_full_snapshot => "create.fullSnapshot",
        .list_full_snapshot => "list.fullSnapshot",
        .delete_full_snapshot => "delete.fullSnapshot",
    };
}

fn grpcMethod(action: Action) []const u8 {
    return switch (action) {
        .create_collection => "/qdrant.Collections/Create",
        .update_collection => "/qdrant.Collections/Update",
        .delete_collection => "/qdrant.Collections/Delete",
        .get_collection => "/qdrant.Collections/Get",
        .list_collection => "/qdrant.Collections/List",
        .upsert_collection => "/qdrant.Points/Upsert",
        .delete_point => "/qdrant.Points/Delete",
        .get_point => "/qdrant.Points/Get",
        .update_vector => "/qdrant.Points/UpdateVectors",
        .delete_vector => "/qdrant.Points/DeleteVectors",
        .set_payload => "/qdrant.Points/SetPayload",
        .overwrite_payload => "/qdrant.Points/OverwritePayload",
        .delete_payload => "/qdrant.Points/DeletePayload",
        .clear_payload => "/qdrant.Points/ClearPayload",
        .create_index => "/qdrant.Points/CreateFieldIndex",
        .delete_index => "/qdrant.Points/DeleteFieldIndex",
        .search_point => "/qdrant.Points/Search",
        .search_batch => "/qdrant.Points/SearchBatch",
        .search_group => "/qdrant.Points/SearchGroups",
        .scroll_point => "/qdrant.Points/Scroll",
        .recommend_point => "/qdrant.Points/Recommend",
        .recommend_batch => "/qdrant.Points/RecommendBatch",
        .recommend_group => "/qdrant.Points/RecommendGroups",
        .discover_point => "/qdrant.Points/Discover",
        .discover_batch => "/qdrant.Points/DiscoverBatch",
        .count_point => "/qdrant.Points/Count",
        .update_batch => "/qdrant.Points/UpdateBatch",
        .query_point => "/qdrant.Points/Query",
        .query_batch => "/qdrant.Points/QueryBatch",
        .query_group => "/qdrant.Points/QueryGroups",
        .facet_point => "/qdrant.Points/Facet",
        .search_matrix_pairs => "/qdrant.Points/SearchMatrixPairs",
        .search_matrix_offsets => "/qdrant.Points/SearchMatrixOffsets",
        .create_snapshot => "/qdrant.Snapshots/Create",
        .list_snapshot => "/qdrant.Snapshots/List",
        .delete_snapshot => "/qdrant.Snapshots/Delete",
        .create_full_snapshot => "/qdrant.Snapshots/CreateFull",
        .list_full_snapshot => "/qdrant.Snapshots/ListFull",
        .delete_full_snapshot => "/qdrant.Snapshots/DeleteFull",
    };
}

pub const ParseError = error{UnknownEvent} || event.EventNameError;

pub fn parseAction(name: []const u8) ParseError!Action {
    const canonical = try event.CanonicalEvent.parse(name);
    if (canonical.state != .request) return error.UnknownEvent;
    const suffix = ".request";
    const base = name[0 .. name.len - suffix.len];
    inline for (@typeInfo(Action).@"enum".fields) |field| {
        const candidate: Action = @enumFromInt(field.value);
        if (std.mem.eql(u8, base, subject(candidate))) return candidate;
    }
    return error.UnknownEvent;
}

/// Transport output used by NATS and QUIC. The payload is the exact UbiQ wire
/// frame, preserving one event identity across both transports.
pub const Sink = struct {
    context: *anyopaque,
    publishFn: *const fn (context: *anyopaque, subject_name: []const u8, frame: []const u8) anyerror!void,

    pub fn publish(self: Sink, subject_name: []const u8, frame: []const u8) !void {
        try self.publishFn(self.context, subject_name, frame);
    }
};

/// Wraps the UbiQ NATS client as an event sink.
pub const NatsSink = struct {
    client: *ubiq.nats_client.Client,
    protocol_buffer: []u8,

    pub fn sink(self: *NatsSink) Sink {
        return .{ .context = self, .publishFn = publish };
    }
    fn publish(context: *anyopaque, subject_name: []const u8, frame: []const u8) !void {
        const self: *NatsSink = @ptrCast(@alignCast(context));
        try self.client.publishRaw(subject_name, null, frame, self.protocol_buffer);
    }
};

/// Blocking UbiQ/NATS listener. It validates that the NATS subject matches the
/// canonical event embedded in the frame before invoking Qdrant.
pub const NatsListener = struct {
    client: *ubiq.nats_client.Client,
    payload_buffer: []u8,
    subject_buffer: []u8,
    reply_buffer: []u8,
    protocol_buffer: []u8,

    pub fn subscribeRequests(self: *NatsListener) !void {
        try self.client.subscribe("*.*.request", "qdrant-zig", self.protocol_buffer);
    }

    pub fn next(self: *NatsListener, allocator: std.mem.Allocator, client: *qdrant.Client) !Handled {
        const envelope = try self.client.nextEnvelope(
            self.payload_buffer,
            self.subject_buffer,
            self.reply_buffer,
            self.protocol_buffer,
        );
        return handle(allocator, client, envelope);
    }
};

/// QUIC integrations pass each complete UbiQ stream/datagram frame here. This
/// keeps QUIC socket and TLS choices in the provider while sharing decoding,
/// validation and Qdrant dispatch with NATS and memory.
pub fn handleQuicFrame(allocator: std.mem.Allocator, client: *qdrant.Client, frame: []const u8) !Handled {
    return handle(allocator, client, try wire.decode(frame));
}

pub fn Hub(comptime capacity: usize, comptime max_frame_size: usize) type {
    return struct {
        const Self = @This();
        memory: ubiq.delivery.MemoryBroker(capacity) = .{},
        nats: ?Sink = null,
        quic: ?Sink = null,
        frame_buffer: [max_frame_size]u8 = undefined,

        /// Emits one canonical event to memory, NATS and QUIC. QUIC providers
        /// receive the UbiQ binary frame and are responsible only for transport.
        pub fn emit(self: *Self, envelope: event.Envelope) !usize {
            const index = try self.memory.publish(envelope);
            const frame = try wire.encode(envelope, &self.frame_buffer);
            if (self.nats) |sink| try sink.publish(envelope.event.name, frame);
            if (self.quic) |sink| try sink.publish(envelope.event.name, frame);
            return index;
        }

        /// Claims the next in-memory request and marks transport receipt. The
        /// caller executes it with `handle`, then calls `settleMemory`.
        pub fn nextMemory(self: *Self, worker: []const u8, now_ms: u64, lease_ms: u64) !struct { index: usize, envelope: event.Envelope } {
            while (true) {
                const index = try self.memory.claim(worker, now_ms, lease_ms);
                try self.memory.markReceived(index, worker);
                const record = try self.memory.get(index);
                if (record.envelope.event.state == .request)
                    return .{ .index = index, .envelope = record.envelope };
                // This listener only executes requests; result events remain
                // observable in the broker record but need no Qdrant action.
                try self.memory.settle(index, worker, record.envelope.event.state);
            }
        }

        pub fn settleMemory(self: *Self, index: usize, worker: []const u8, succeeded: bool) !void {
            try self.memory.settle(index, worker, if (succeeded) .ok else .@"error");
        }
    };
}

pub const Handled = struct {
    allocator: std.mem.Allocator,
    response: qdrant.Response,
    event_name: []u8,
    schema_id: []u8,
    idempotency_key: []u8,
    envelope: event.Envelope,

    /// Call only after every in-memory consumer has finished with `envelope`.
    pub fn deinit(self: *Handled) void {
        self.response.deinit(self.allocator);
        self.allocator.free(self.event_name);
        self.allocator.free(self.schema_id);
        self.allocator.free(self.idempotency_key);
        self.* = undefined;
    }
};

/// Executes a request event against Qdrant. Its payload must be the official
/// protobuf request for that RPC. The returned event contains protobuf output.
pub fn handle(allocator: std.mem.Allocator, client: *qdrant.Client, request: event.Envelope) !Handled {
    const action = try parseAction(request.event.name);
    var response = try client.call(grpcMethod(action), request.payload);
    errdefer response.deinit(allocator);
    const state: []const u8 = if (response.isSuccess()) "ok" else "error";
    const event_name = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ subject(action), state });
    errdefer allocator.free(event_name);
    const schema_id = try std.fmt.allocPrint(allocator, "{s}@1", .{event_name});
    errdefer allocator.free(schema_id);
    const idempotency_key = try std.fmt.allocPrint(allocator, "{s}:{s}", .{ request.idempotency_key, state });
    errdefer allocator.free(idempotency_key);
    const canonical = try event.CanonicalEvent.parse(event_name);
    return .{
        .allocator = allocator,
        .response = response,
        .event_name = event_name,
        .schema_id = schema_id,
        .idempotency_key = idempotency_key,
        .envelope = .{
            .id = request.id,
            .event = canonical,
            .correlation_id = request.correlation_id,
            .causation_id = request.id,
            .idempotency_key = idempotency_key,
            .schema_id = schema_id,
            .payload = if (response.isSuccess()) response.protobuf else response.message,
            .guarantee = request.guarantee,
            .created_at_ms = request.created_at_ms,
        },
    };
}

test "action subjects follow action.entity/property and map to Qdrant RPCs" {
    try std.testing.expectEqualStrings("create.collection", subject(.create_collection));
    try std.testing.expectEqualStrings("upsert.collection", subject(.upsert_collection));
    try std.testing.expectEqual(Action.create_collection, try parseAction("create.collection.request"));
    try std.testing.expectEqualStrings("/qdrant.Collections/Create", grpcMethod(.create_collection));
}

test "hub emits the identical UbiQ frame to NATS, QUIC, and memory" {
    const Capture = struct {
        frame: []const u8 = "",
        subject_name: []const u8 = "",
        fn publish(context: *anyopaque, name: []const u8, frame: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.subject_name = name;
            self.frame = frame;
        }
        fn sink(self: *@This()) Sink {
            return .{ .context = self, .publishFn = publish };
        }
    };
    var nats = Capture{};
    var quic = Capture{};
    var hub = Hub(4, 1024){ .nats = nats.sink(), .quic = quic.sink() };
    const envelope: event.Envelope = .{
        .id = "evt-1",
        .event = try event.CanonicalEvent.parse("upsert.collection.ok"),
        .correlation_id = "corr-1",
        .causation_id = "evt-0",
        .idempotency_key = "idem-1",
        .schema_id = "upsert.collection.ok@1",
        .payload = "protobuf",
        .guarantee = .received,
        .created_at_ms = 1,
    };
    _ = try hub.emit(envelope);
    try std.testing.expectEqual(@as(usize, 1), hub.memory.len);
    try std.testing.expectEqualStrings("upsert.collection.ok", nats.subject_name);
    try std.testing.expectEqualSlices(u8, nats.frame, quic.frame);
    const decoded = try wire.decode(nats.frame);
    try std.testing.expectEqualStrings(envelope.event.name, decoded.event.name);
}
