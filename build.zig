const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const qdrant = b.addModule("qdrant", .{
        .root_source_file = b.path("src/qdrant.zig"),
        .target = target,
        .optimize = optimize,
    });
    qdrant.addCSourceFile(.{ .file = b.path("src/grpc_bridge.c"), .flags = &.{"-std=c11"} });
    qdrant.linkSystemLibrary("grpc", .{});

    const unit_tests = b.addTest(.{ .root_module = qdrant });
    const run_unit_tests = b.addRunArtifact(unit_tests);
    b.step("test", "Run unit tests").dependOn(&run_unit_tests.step);

    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/integration.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "qdrant", .module = qdrant }},
        }),
    });
    const run_integration_tests = b.addRunArtifact(integration_tests);
    b.step("integration-test", "Run tests against Qdrant on localhost:6333").dependOn(&run_integration_tests.step);
}
