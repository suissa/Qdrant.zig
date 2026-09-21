//! Qdrant gRPC client for Zig 0.16.
//!
//! The generic `call` entry point accepts and returns protobuf wire bytes, so
//! every unary method exposed by Qdrant is immediately usable. Small protobuf
//! builders and common collection/point helpers are included for convenience.

const std = @import("std");

const NativeResult = extern struct {
    response: ?[*]u8,
    response_len: usize,
    status_code: c_int,
    status_message: ?[*:0]u8,
};

extern fn qdrant_grpc_client_create(target: [*:0]const u8, api_key: ?[*:0]const u8, use_tls: bool) ?*anyopaque;
extern fn qdrant_grpc_client_destroy(client: *anyopaque) void;
extern fn qdrant_grpc_unary_call(client: *anyopaque, method: [*:0]const u8, request: [*]const u8, request_len: usize, timeout_ms: u32) NativeResult;
extern fn qdrant_grpc_result_destroy(result: *NativeResult) void;

pub const Status = enum(c_int) {
    ok = 0,
    cancelled = 1,
    unknown = 2,
    invalid_argument = 3,
    deadline_exceeded = 4,
    not_found = 5,
    already_exists = 6,
    permission_denied = 7,
    resource_exhausted = 8,
    failed_precondition = 9,
    aborted = 10,
    out_of_range = 11,
    unimplemented = 12,
    internal = 13,
    unavailable = 14,
    data_loss = 15,
    unauthenticated = 16,
    _,
};

pub const Response = struct {
    status: Status,
    message: []u8,
    protobuf: []u8,

    pub fn deinit(self: *Response, allocator: std.mem.Allocator) void {
        allocator.free(self.message);
        allocator.free(self.protobuf);
        self.* = undefined;
    }

    pub fn isSuccess(self: Response) bool {
        return self.status == .ok;
    }
};

pub const Client = struct {
    allocator: std.mem.Allocator,
    native: *anyopaque,
    timeout_ms: u32,

    pub const Options = struct {
        target: []const u8 = "localhost:6334",
        api_key: ?[]const u8 = null,
        tls: bool = false,
        timeout_ms: u32 = 10_000,
    };

    pub fn init(allocator: std.mem.Allocator, options: Options) !Client {
        if (options.target.len == 0 or std.mem.indexOfScalar(u8, options.target, 0) != null) return error.InvalidTarget;
        const target = try allocator.dupeZ(u8, options.target);
        defer allocator.free(target);
        const key = if (options.api_key) |value| try allocator.dupeZ(u8, value) else null;
        defer if (key) |value| allocator.free(value);
        const native = qdrant_grpc_client_create(target, if (key) |value| value else null, options.tls) orelse return error.ClientInitializationFailed;
        return .{ .allocator = allocator, .native = native, .timeout_ms = options.timeout_ms };
    }

    pub fn deinit(self: *Client) void {
        qdrant_grpc_client_destroy(self.native);
        self.* = undefined;
    }

    /// Invokes any unary Qdrant gRPC method using protobuf wire bytes.
    /// `method` has the canonical form `/qdrant.Service/Method`.
    pub fn call(self: *Client, method: []const u8, protobuf_request: []const u8) !Response {
        if (!std.mem.startsWith(u8, method, "/qdrant.") or std.mem.indexOfScalar(u8, method, 0) != null) return error.InvalidMethod;
        const method_z = try self.allocator.dupeZ(u8, method);
        defer self.allocator.free(method_z);
        const request_ptr: [*]const u8 = if (protobuf_request.len == 0) @ptrCast("".ptr) else protobuf_request.ptr;
        var native_result = qdrant_grpc_unary_call(self.native, method_z, request_ptr, protobuf_request.len, self.timeout_ms);
        defer qdrant_grpc_result_destroy(&native_result);

        const body = if (native_result.response) |ptr|
            try self.allocator.dupe(u8, ptr[0..native_result.response_len])
        else
            try self.allocator.alloc(u8, 0);
        errdefer self.allocator.free(body);
        const native_message = if (native_result.status_message) |ptr| std.mem.span(ptr) else "";
        return .{
            .status = @enumFromInt(native_result.status_code),
            .message = try self.allocator.dupe(u8, native_message),
            .protobuf = body,
        };
    }

    pub fn health(self: *Client) !Response {
        return self.call("/qdrant.Qdrant/HealthCheck", "");
    }
    pub fn collections(self: *Client) !Response {
        return self.call("/qdrant.Collections/List", "");
    }
    pub fn collection(self: *Client, name: []const u8) !Response {
        const request = try encodeCollectionName(self.allocator, name);
        defer self.allocator.free(request);
        return self.call("/qdrant.Collections/Get", request);
    }
    pub fn createCollection(self: *Client, options: CreateCollectionOptions) !Response {
        const request = try encodeCreateCollection(self.allocator, options);
        defer self.allocator.free(request);
        return self.call("/qdrant.Collections/Create", request);
    }
    pub fn deleteCollection(self: *Client, name: []const u8) !Response {
        const request = try encodeCollectionName(self.allocator, name);
        defer self.allocator.free(request);
        return self.call("/qdrant.Collections/Delete", request);
    }
    pub fn upsert(self: *Client, collection_name: []const u8, points: []const Point, wait: bool) !Response {
        const request = try encodeUpsert(self.allocator, collection_name, points, wait);
        defer self.allocator.free(request);
        return self.call("/qdrant.Points/Upsert", request);
    }

    // Raw helpers retain type-safe method names while accepting the complete
    // official protobuf request, including fields added by future Qdrant versions.
    pub fn collectionsRaw(self: *Client, comptime method: []const u8, request: []const u8) !Response {
        return self.call("/qdrant.Collections/" ++ method, request);
    }
    pub fn pointsRaw(self: *Client, comptime method: []const u8, request: []const u8) !Response {
        return self.call("/qdrant.Points/" ++ method, request);
    }
    pub fn snapshotsRaw(self: *Client, comptime method: []const u8, request: []const u8) !Response {
        return self.call("/qdrant.Snapshots/" ++ method, request);
    }
};

pub const Distance = enum(u32) { cosine = 1, euclid = 2, dot = 3, manhattan = 4 };
pub const CreateCollectionOptions = struct {
    name: []const u8,
    vector_size: u64,
    distance: Distance = .cosine,
};
pub const Point = struct { id: u64, vector: []const f32 };

/// Minimal protobuf writer, also useful when calling new Qdrant methods.
pub const ProtoWriter = struct {
    output: std.Io.Writer.Allocating,

    pub fn init(allocator: std.mem.Allocator) ProtoWriter {
        return .{ .output = .init(allocator) };
    }
    pub fn deinit(self: *ProtoWriter) void {
        self.output.deinit();
    }
    pub fn finish(self: *ProtoWriter, allocator: std.mem.Allocator) ![]u8 {
        return allocator.dupe(u8, self.output.writer.buffered());
    }
    pub fn varint(self: *ProtoWriter, field: u32, value: u64) !void {
        try writeVarint(&self.output.writer, (@as(u64, field) << 3));
        try writeVarint(&self.output.writer, value);
    }
    pub fn bytes(self: *ProtoWriter, field: u32, value: []const u8) !void {
        try writeVarint(&self.output.writer, (@as(u64, field) << 3) | 2);
        try writeVarint(&self.output.writer, value.len);
        try self.output.writer.writeAll(value);
    }
};

fn writeVarint(writer: *std.Io.Writer, input: u64) !void {
    var value = input;
    while (value >= 0x80) : (value >>= 7) try writer.writeByte(@intCast((value & 0x7f) | 0x80));
    try writer.writeByte(@intCast(value));
}

fn encodeCollectionName(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    if (name.len == 0) return error.InvalidCollectionName;
    var message = ProtoWriter.init(allocator);
    defer message.deinit();
    try message.bytes(1, name);
    return message.finish(allocator);
}

fn encodeCreateCollection(allocator: std.mem.Allocator, options: CreateCollectionOptions) ![]u8 {
    if (options.name.len == 0 or options.vector_size == 0) return error.InvalidCollectionConfiguration;
    var params = ProtoWriter.init(allocator);
    defer params.deinit();
    try params.varint(1, options.vector_size);
    try params.varint(2, @intFromEnum(options.distance));
    const params_bytes = try params.finish(allocator);
    defer allocator.free(params_bytes);

    var config = ProtoWriter.init(allocator);
    defer config.deinit();
    try config.bytes(1, params_bytes);
    const config_bytes = try config.finish(allocator);
    defer allocator.free(config_bytes);

    var request = ProtoWriter.init(allocator);
    defer request.deinit();
    try request.bytes(1, options.name);
    try request.bytes(10, config_bytes);
    return request.finish(allocator);
}

fn encodeUpsert(allocator: std.mem.Allocator, collection_name: []const u8, points: []const Point, wait: bool) ![]u8 {
    if (collection_name.len == 0 or points.len == 0) return error.InvalidPoints;
    var request = ProtoWriter.init(allocator);
    defer request.deinit();
    try request.bytes(1, collection_name);
    try request.varint(2, @intFromBool(wait));
    for (points) |point| {
        var id = ProtoWriter.init(allocator);
        defer id.deinit();
        try id.varint(1, point.id);
        const id_bytes = try id.finish(allocator);
        defer allocator.free(id_bytes);

        var packed_data = std.Io.Writer.Allocating.init(allocator);
        defer packed_data.deinit();
        for (point.vector) |component| try packed_data.writer.writeInt(u32, @bitCast(component), .little);
        var vector = ProtoWriter.init(allocator);
        defer vector.deinit();
        try vector.bytes(1, packed_data.writer.buffered());
        const vector_bytes = try vector.finish(allocator);
        defer allocator.free(vector_bytes);
        var vectors = ProtoWriter.init(allocator);
        defer vectors.deinit();
        try vectors.bytes(1, vector_bytes);
        const vectors_bytes = try vectors.finish(allocator);
        defer allocator.free(vectors_bytes);

        var point_message = ProtoWriter.init(allocator);
        defer point_message.deinit();
        try point_message.bytes(1, id_bytes);
        try point_message.bytes(4, vectors_bytes);
        const point_bytes = try point_message.finish(allocator);
        defer allocator.free(point_bytes);
        try request.bytes(3, point_bytes);
    }
    return request.finish(allocator);
}

test "protobuf requests match the Qdrant wire schema" {
    const create = try encodeCreateCollection(std.testing.allocator, .{ .name = "zig", .vector_size = 4 });
    defer std.testing.allocator.free(create);
    try std.testing.expectEqualSlices(u8, &.{ 0x0a, 0x03, 'z', 'i', 'g', 0x52, 0x06, 0x0a, 0x04, 0x08, 0x04, 0x10, 0x01 }, create);
    const named = try encodeCollectionName(std.testing.allocator, "zig");
    defer std.testing.allocator.free(named);
    try std.testing.expectEqualSlices(u8, &.{ 0x0a, 0x03, 'z', 'i', 'g' }, named);
}
