const std = @import("std");

const MARLIN_BOARD_SIZE = 32;
const MOVE_SCORE_PAIR_SIZE = 4;
const NULL_TERMINATOR_ARRAY: [4]u8 = .{ 0, 0, 0, 0 };

const ViriformatFileIterator = struct {
    file: std.fs.File,
    mapped: []align(std.heap.pageSize()) const u8,
    read_idx: usize,
    file_size: usize,

    pub fn init(file_name: []const u8) !ViriformatFileIterator {
        const file = try std.fs.cwd().openFile(file_name, .{});
        errdefer file.close();

        const stat = try std.posix.fstat(file.handle);
        const file_size: usize = @intCast(stat.size);

        const mapped = try std.posix.mmap(null, file_size, std.posix.PROT.READ, .{ .TYPE = .PRIVATE }, file.handle, 0);
        errdefer std.posix.munmap(mapped);

        return .{
            .file = file,
            .mapped = mapped,
            .read_idx = 0,
            .file_size = file_size,
        };
    }

    pub fn bytesRemaining(self: *const ViriformatFileIterator) usize {
        return std.math.sub(usize, self.file_size, self.read_idx) catch 0;
    }

    // returns a full viriformat game or null if there are no valid ones left
    // returned slice does not include the null terminator
    pub fn next(self: *ViriformatFileIterator) ?[]const u8 {
        if (self.bytesRemaining() == 0) {
            return null;
        }

        const game_start_idx = self.read_idx;
        const move_start_idx = self.read_idx + MARLIN_BOARD_SIZE;
        if (move_start_idx >= self.file_size) {
            return null;
        }
        const u32_ptr: []const u32 = @ptrCast(@alignCast(self.mapped[move_start_idx..]));
        const u32_slice = u32_ptr[0 .. (self.file_size - move_start_idx) / 4];
        const moves_in_game = std.mem.indexOfScalar(u32, u32_slice, 0) orelse return null;
        const move_end_idx = move_start_idx + MOVE_SCORE_PAIR_SIZE * moves_in_game;
        self.read_idx += MARLIN_BOARD_SIZE + MOVE_SCORE_PAIR_SIZE * (moves_in_game + 1); // + 1 because we need to discard the null terminator
        return self.mapped[game_start_idx..move_end_idx];
    }

    pub fn deinit(self: ViriformatFileIterator) void {
        std.posix.munmap(self.mapped);
        self.file.close();
    }
};

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

    var iterators = std.ArrayList(ViriformatFileIterator).init(gpa.allocator());
    defer iterators.deinit();
    while (args.next()) |file_name| {
        try iterators.append(try ViriformatFileIterator.init(file_name));
    }
    defer for (iterators.items) |iter| {
        iter.deinit();
    };
    if (iterators.items.len == 0) {
        return error.NoInputFile;
    }
    var total_output_size: usize = 0;
    var total_count: usize = 0;
    var timer = try std.time.Timer.start();
    for (iterators.items) |*iter| {
        var count: usize = 0;
        var amount_written: usize = 0;
        while (iter.next()) |game| {
            count += (game.len - MARLIN_BOARD_SIZE) / MOVE_SCORE_PAIR_SIZE;
            amount_written += game.len;
            try writer.writeAll(game);
            try writer.writeAll(&NULL_TERMINATOR_ARRAY);
        }

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
