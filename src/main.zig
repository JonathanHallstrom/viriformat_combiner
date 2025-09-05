const std = @import("std");

const VALIDATE_MOVES = true;
const MARLIN_BOARD_SIZE = 32;
const MOVE_SCORE_PAIR_SIZE = 4;
const NULL_TERMINATOR_ARRAY: [4]u8 = .{ 0, 0, 0, 0 };

const PROMO_FLAG: u4 = 0b1100;
const EP_FLAG: u4 = 0b0100;
const CASTLE_FLAG: u4 = 0b1000;

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
        comptime std.debug.assert(MARLIN_BOARD_SIZE % MOVE_SCORE_PAIR_SIZE == 0);
        const trimmed_size: usize = file_size - file_size % MOVE_SCORE_PAIR_SIZE;

        const mapped = try std.posix.mmap(null, trimmed_size, std.posix.PROT.READ, .{ .TYPE = .PRIVATE }, file.handle, 0);
        errdefer std.posix.munmap(mapped);

        return .{
            .file = file,
            .mapped = mapped,
            .read_idx = 0,
            .file_size = trimmed_size,
        };
    }

    pub fn bytesRemaining(self: *const ViriformatFileIterator) usize {
        return std.math.sub(usize, self.file_size, self.read_idx) catch 0;
    }

    // returns a full viriformat game or null if there are no valid ones left
    // returned slice does not include the null terminator
    pub inline fn next(self: *ViriformatFileIterator) ?[]const u8 {
        if (self.bytesRemaining() == 0) {
            return null;
        }

        const game_start_idx = self.read_idx;
        const initial_occupancy: u64 = std.mem.readInt(u64, self.mapped[game_start_idx..][0..8], .little);
        if (@popCount(initial_occupancy) < 2) {
            self.read_idx = self.file_size;
            return null; // need to have at least two pieces to have two kings
        }
        const move_start_idx = self.read_idx + MARLIN_BOARD_SIZE;
        if (move_start_idx >= self.file_size) {
            self.read_idx = self.file_size;
            return null;
        }
        const u32_ptr: []const u32 = @ptrCast(@alignCast(self.mapped[move_start_idx..]));
        const u32_slice = u32_ptr[0 .. (self.file_size - move_start_idx) / 4];
        const moves_in_game = std.mem.indexOfScalar(u32, u32_slice, 0) orelse {
            self.read_idx = self.file_size;
            return null;
        };
        const move_end_idx = move_start_idx + MOVE_SCORE_PAIR_SIZE * moves_in_game;

        if (VALIDATE_MOVES) {
            var occ = initial_occupancy;
            for (0..moves_in_game) |i| {
                const move = std.mem.readInt(u16, self.mapped[move_start_idx..][i * MOVE_SCORE_PAIR_SIZE ..][0..2], .little);
                const from = move & 0b111111;
                const from_bb = @as(u64, 1) << @intCast(from);
                const to = move >> 6 & 0b111111;
                const to_bb = @as(u64, 1) << @intCast(to);
                const flag = move >> 12;
                const from_rank = from / 8;
                // const to_rank = to / 8;
                const to_file = to % 8;
                if (occ & from_bb == 0) {
                    @branchHint(.unlikely);
                    self.read_idx = self.file_size;
                    return null;
                }
                if (from == to) {
                    @branchHint(.unlikely);
                    self.read_idx = self.file_size;
                    return null;
                }
                if (flag == CASTLE_FLAG) {
                    @branchHint(.unpredictable);
                    const mask: u64 = if (from < to) 0b01100000 else 0b00001100;
                    occ &= ~(to_bb | from_bb);
                    occ |= mask << @intCast(from_rank * 8);
                } else {
                    occ ^= from_bb;
                    occ |= to_bb;
                }
                if (flag == EP_FLAG) {
                    @branchHint(.unlikely);
                    occ ^= @as(u64, 1) << @intCast(8 * from_rank + to_file);
                }
            }
        }

        self.read_idx += MARLIN_BOARD_SIZE + MOVE_SCORE_PAIR_SIZE * (moves_in_game + 1); // + 1 because we need to discard the null terminator
        return self.mapped[game_start_idx..move_end_idx];
    }

    pub fn deinit(self: ViriformatFileIterator) void {
        std.posix.munmap(self.mapped);
        self.file.close();
    }
};

fn usage(err: anyerror) anyerror {
    std.debug.print("Usage: viriformat_combiner [--no-shuffle | --concat | --count] [<output_file>] <input_file>...\n\nCombines and shuffles games from multiple viriformat (.vf) files into a single new file.\n\nArguments:\n  --no-shuffle, --concat   Disables shuffling of games, combining them in the order they appear in input files.\n  --count                  Counts the number of positions in the input files and prints to stdout.\n  <output_file>            The path to the new, combined file to be created. Not used with --count.\n  <input_file>...          One or more paths to the input viriformat files.\n", .{});
    return err;
}

const Mode = enum {
    concat,
    shuffle,
    count,
};

pub fn main() !u8 {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.next();

    var seed: u64 = 0;
    try std.posix.getrandom(std.mem.asBytes(&seed));
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var input_file_names = std.ArrayList([]const u8).init(allocator);
    defer input_file_names.deinit();
    var output_file_name: ?[]const u8 = null;
    var mode: Mode = .concat;

    while (args.next()) |argument| {
        if (std.ascii.eqlIgnoreCase(argument, "--shuffle")) {
            mode = .shuffle;
        } else if (std.ascii.eqlIgnoreCase(argument, "--concat") or std.ascii.eqlIgnoreCase(argument, "--no-shuffle")) {
            mode = .concat;
        } else if (std.ascii.eqlIgnoreCase(argument, "--count")) {
            mode = .count;
        } else if (argument[0] == '-') {
            std.log.err("unknown flag: {s}", .{argument});
            return usage(error.UnknownFlag);
        } else {
            if (output_file_name == null and mode != .count) {
                output_file_name = argument;
            } else {
                try input_file_names.append(argument);
            }
        }
    }

    if (input_file_names.items.len == 0) {
        std.log.err("no input files specified.", .{});
        return usage(error.NoInputFile);
    }

    if (mode != .count and output_file_name == null) {
        std.log.err("output file must be specified for shuffle or concat mode.", .{});
        return usage(error.NoOutputFile);
    }

    var iterators = std.ArrayList(ViriformatFileIterator).init(allocator);
    defer {
        for (iterators.items) |*iter| {
            iter.deinit();
        }
        iterators.deinit();
    }

    for (input_file_names.items) |input_file| {
        const iterator = ViriformatFileIterator.init(input_file) catch |e| {
            std.log.err("Failed to open input file '{s}': {s}", .{ input_file, @errorName(e) });
            return e;
        };
        try iterators.append(iterator);
    }

    var timer = try std.time.Timer.start();
    var total_count: usize = 0;
    var game_count: usize = 0;

    switch (mode) {
        .count => {
            for (iterators.items) |*iter| {
                while (iter.next()) |game| {
                    total_count += (game.len - MARLIN_BOARD_SIZE) / MOVE_SCORE_PAIR_SIZE;
                    game_count += 1;
                }
            }
            std.debug.print("{} games\n", .{game_count});
            std.debug.print("{} positions\n", .{total_count});
        },
        .concat, .shuffle => {
            const out_filename = output_file_name orelse unreachable;

            if (std.fs.cwd().statFile(out_filename)) |_| {
                std.log.err("the specified output file ('{s}') already exists, exiting to avoid overwriting it", .{out_filename});
                return error.OutputFileAlreadyExists;
            } else |_| {}

            const output_file = try std.fs.cwd().createFile(out_filename, .{});
            defer output_file.close();

            var bw = std.io.bufferedWriter(output_file.writer());
            const writer = bw.writer();
            defer bw.flush() catch |e| std.debug.panic("flushing outputfile failed with error: {}\n", .{e});

            var total_output_size: usize = 0;

            if (mode == .concat) {
                for (iterators.items) |*iter| {
                    while (iter.next()) |game| {
                        try writer.writeAll(game);
                        try writer.writeAll(&NULL_TERMINATOR_ARRAY);
                        total_count += (game.len - MARLIN_BOARD_SIZE) / MOVE_SCORE_PAIR_SIZE;
                        total_output_size += game.len + NULL_TERMINATOR_ARRAY.len;
                        game_count += 1;
                    }
                }
            } else {
                var remaining_input_size: usize = 0;
                for (iterators.items) |*iter| {
                    remaining_input_size += iter.bytesRemaining();
                }

                while (remaining_input_size > 0) {
                    const rand_val = random.uintLessThan(usize, remaining_input_size);
                    var bytes_seen: usize = 0;
                    var processed_in_iteration = false;

                    for (iterators.items) |*iter| {
                        const iter_bytes_before = iter.bytesRemaining();
                        if (iter_bytes_before == 0) continue;

                        if (rand_val < bytes_seen + iter_bytes_before) {
                            const game = iter.next() orelse {
                                // Invalid data at end of file.
                                remaining_input_size -= iter_bytes_before;
                                processed_in_iteration = true;
                                break;
                            };

                            try writer.writeAll(game);
                            try writer.writeAll(&NULL_TERMINATOR_ARRAY);
                            total_count += (game.len - MARLIN_BOARD_SIZE) / MOVE_SCORE_PAIR_SIZE;
                            total_output_size += game.len + NULL_TERMINATOR_ARRAY.len;
                            game_count += 1;

                            const bytes_consumed = iter_bytes_before - iter.bytesRemaining();
                            remaining_input_size -= bytes_consumed;
                            processed_in_iteration = true;
                            break; // Exit inner for loop, continue with while
                        }
                        bytes_seen += iter_bytes_before;
                    }
                    if (!processed_in_iteration) {
                        // This can happen if all remaining data is invalid.
                        // Recalculate to be safe and exit if needed.
                        remaining_input_size = 0;
                        for (iterators.items) |*iter| {
                            remaining_input_size += iter.bytesRemaining();
                        }
                    }
                }
            }

            const elapsed = timer.read();
            std.debug.print("wrote a total of {} positions\n", .{total_count});
            std.debug.print("wrote a total of {} games\n", .{game_count});
            std.debug.print("wrote {} at a speed of {d:.4}GB/s\n", .{
                std.fmt.fmtIntSizeDec(total_output_size),
                @as(f64, @floatFromInt(total_output_size)) * std.time.ns_per_s / 1e9 / @as(f64, @floatFromInt(elapsed)),
            });
        },
    }

    return 0;
}
