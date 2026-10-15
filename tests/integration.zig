const std = @import("std");
const qdrant = @import("qdrant");

fn expectSuccess(response: qdrant.Response) !void {
    if (!response.isSuccess()) {
        std.debug.print("gRPC {s}: {s}\n", .{ @tagName(response.status), response.message });
        return error.QdrantRequestFailed;
    }
}

fn cleanup(client: *qdrant.Client, allocator: std.mem.Allocator) void {
    var response = client.deleteCollection("zig_grpc_integration") catch return;
    response.deinit(allocator);
}

test "Qdrant gRPC collection and vector lifecycle" {
    const allocator = std.testing.allocator;
    var client = try qdrant.Client.init(allocator, .{});
    defer client.deinit();

    var health = try client.health();
    defer health.deinit(allocator);
    try expectSuccess(health);
    try std.testing.expect(health.protobuf.len != 0);

    cleanup(&client, allocator);
    // CreateCollection protobuf: name (field 1), vectors_config (field 10),
    // VectorParams size (field 1) and Cosine distance (field 2).
    const create_request = [_]u8{ 0x0a, 0x14, 'z', 'i', 'g', '_', 'g', 'r', 'p', 'c', '_', 'i', 'n', 't', 'e', 'g', 'r', 'a', 't', 'i', 'o', 'n', 0x52, 0x06, 0x0a, 0x04, 0x08, 0x04, 0x10, 0x01 };
    const request_event: qdrant.events.event.Envelope = .{
        .id = "evt-create",
        .event = try qdrant.events.event.CanonicalEvent.parse("create.collection.request"),
        .correlation_id = "integration",
        .causation_id = "",
        .idempotency_key = "create-integration",
        .schema_id = "create.collection.request@1",
        .payload = &create_request,
        .guarantee = .processed,
        .created_at_ms = 1,
    };
    var created = try qdrant.events.handle(allocator, &client, request_event);
    defer created.deinit();
    try expectSuccess(created.response);
    try std.testing.expectEqualStrings("create.collection.ok", created.envelope.event.name);
    var event_hub = qdrant.events.Hub(4, 1024){};
    _ = try event_hub.emit(created.envelope);
    try std.testing.expectEqual(@as(usize, 1), event_hub.memory.len);
    defer cleanup(&client, allocator);

    const points = [_]qdrant.Point{
        .{ .id = 1, .vector = &.{ 0.05, 0.61, 0.76, 0.74 } },
        .{ .id = 2, .vector = &.{ 0.19, 0.81, 0.75, 0.11 } },
    };
    var upserted = try client.upsert("zig_grpc_integration", &points, true);
    defer upserted.deinit(allocator);
    try expectSuccess(upserted);

    var info = try client.collection("zig_grpc_integration");
    defer info.deinit(allocator);
    try expectSuccess(info);
    try std.testing.expect(info.protobuf.len != 0);

    var listed = try client.collections();
    defer listed.deinit(allocator);
    try expectSuccess(listed);
    try std.testing.expect(std.mem.indexOf(u8, listed.protobuf, "zig_grpc_integration") != null);

    // Exercise the generic transport with an additional Points RPC. CountPoints
    // field 1 is collection_name and field 3 is the optional exact boolean.
    var count_request = qdrant.ProtoWriter.init(allocator);
    defer count_request.deinit();
    try count_request.bytes(1, "zig_grpc_integration");
    try count_request.varint(3, 1);
    const count_bytes = try count_request.finish(allocator);
    defer allocator.free(count_bytes);
    var counted = try client.pointsRaw("Count", count_bytes);
    defer counted.deinit(allocator);
    try expectSuccess(counted);
    try std.testing.expect(counted.protobuf.len != 0);
}
