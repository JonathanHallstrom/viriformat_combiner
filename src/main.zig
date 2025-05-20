const std = @import("std");

const MARLIN_BOARD_SIZE = 32;
const MOVE_SCORE_PAIR_SIZE = 4;
const NULL_TERMINATOR_ARRAY: [4]u8 = .{ 0, 0, 0, 0 };

pub fn main() !u8 {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var args = try std.process.argsWithAllocator(gpa.allocator());
    defer args.deinit();
    _ = args.next(); // discard the name

    const output_file_name = args.next() orelse return error.NoOutputFile;
    if (std.fs.cwd().statFile(output_file_name)) |_| {
        std.log.err("The specified output file ('{s}') already exists, exiting to avoid overwriting it", .{output_file_name});
        return error.OutputFileAlreadyExists;
    } else |_| {}
    const output_file = try std.fs.cwd().createFile(output_file_name, .{});
    defer output_file.close();

    var bw = std.io.BufferedWriter(1 << 20, @TypeOf(output_file.writer())){ .unbuffered_writer = output_file.writer() };
    defer bw.flush() catch |e| std.debug.panic("flushing outputfile failed with error: {}\n", .{e});
    const writer = bw.writer();

    var file_names = std.ArrayList([]const u8).init(gpa.allocator());
    defer file_names.deinit();
    while (args.next()) |file_name| {
        if (std.fs.cwd().statFile(file_name)) |_| {} else |_| {
            std.log.err("The input file '{s}' does not exist or is not visible", .{file_name});
            return error.FileNotFOund;
        }
        try file_names.append(file_name);
    }
    if (file_names.items.len == 0) {
        return error.NoInputFile;
    }
    var total_output_size: usize = 0;
    var total_count: usize = 0;
    var timer = try std.time.Timer.start();
    for (file_names.items) |file_name| {
        std.debug.print("parsing: {s}", .{file_name});
        const file = try std.fs.cwd().openFile(file_name, .{});
        defer file.close();

        const stat = try std.posix.fstat(file.handle);
        const file_size: usize = @intCast(stat.size);

        const mapped = try std.posix.mmap(null, file_size, std.posix.PROT.READ, .{ .TYPE = .PRIVATE }, file.handle, 0);
        defer std.posix.munmap(mapped);

        var i: usize = 0;
        var count: usize = 0;
        var amount_written: usize = 0;
        while (i < file_size) {
            if (i > 0) {
                std.debug.assert(std.mem.eql(u8, mapped[i - 4 ..][0..4], &NULL_TERMINATOR_ARRAY));
            }
            const start_current = i;
            i += MARLIN_BOARD_SIZE;
            const remaining_bytes = file_size - i;
            const u32_ptr: []const u32 = @ptrCast(@alignCast(mapped[i..]));
            const u32_slice = u32_ptr[0 .. remaining_bytes / 4];
            const moves_in_game = std.mem.indexOfScalar(u32, u32_slice, 0) orelse break;
            count += moves_in_game;
            const end_current = i + MOVE_SCORE_PAIR_SIZE * moves_in_game;
            i += MOVE_SCORE_PAIR_SIZE * (moves_in_game + 1);
            try writer.writeAll(mapped[start_current..end_current]);
            amount_written += end_current - start_current;
            try writer.writeAll(&NULL_TERMINATOR_ARRAY);
            amount_written += MOVE_SCORE_PAIR_SIZE;
        }
        std.debug.print(" file contains {} positions\n", .{count});
        total_count += count;
        total_output_size += amount_written;
    }
    const elapsed = timer.read();

    std.debug.print("wrote a total of {} positions\n", .{total_count});
    std.debug.print("wrote {} at a speed of {d:.4}GB/s\n", .{
        std.fmt.fmtIntSizeDec(total_output_size),
        @as(f64, @floatFromInt(total_output_size)) * std.time.ns_per_s / 1e9 / @as(f64, @floatFromInt(elapsed)),
    });

    return 0;
}
