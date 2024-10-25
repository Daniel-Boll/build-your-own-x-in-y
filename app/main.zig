const std = @import("std");
const bencode_parser = @import("bencode.zig");
const stdout = std.io.getStdOut().writer();
const allocator = std.heap.page_allocator;

pub fn main() !void {
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 3) {
        try stdout.print("Usage: your_bittorrent.zig <command> <args>\n", .{});
        std.process.exit(1);
    }

    const command = args[1];

    if (std.mem.eql(u8, command, "decode")) {
        const encodedStr = args[2];
        const bencoded_value = try bencode_parser.parse(allocator, encodedStr);
        defer bencoded_value.free(allocator) catch {
          std.process.exit(1);
        };

        var string = std.ArrayList(u8).init(allocator);
        try std.json.stringify(try bencoded_value.toJSON(allocator), .{}, string.writer());
        try stdout.print("{s}\n", .{try string.toOwnedSlice()});
    }
}
