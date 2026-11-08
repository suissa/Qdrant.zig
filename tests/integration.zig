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
    var created = try client.createCollection(.{ .name = "zig_grpc_integration", .vector_size = 4 });
    defer created.deinit(allocator);
    try expectSuccess(created);
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
