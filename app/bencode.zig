const std = @import("std");
const json = std.json;

pub const Error = error{
    UnexpectedEndOfInput,
    InvalidFormat,
    InvalidInteger,
    InvalidStringLength,
    InvalidDictKey,
    OutOfMemory,
};

pub const BencodeValue = union(enum) {
    Int: i64,
    String: []u8,
    List: []BencodeValue,
    Dict: []Entry,

    pub const Entry = struct {
        key: []u8,
        value: BencodeValue,
    };

    pub fn toJSON(self: BencodeValue, allocator: std.mem.Allocator) !json.Value {
        switch (self) {
            .Int => return json.Value{
                .integer = self.Int,
            },
            .String => return json.Value{ .string = self.String },
            .List => {
                var items = std.ArrayList(json.Value).init(allocator);
                for (self.List) |item| {
                    try items.append(try item.toJSON(allocator));
                }
                return json.Value{ .array = items };
            },
            .Dict => {
                var entries = std.json.ObjectMap.init(allocator);
                for (self.Dict) |entry| {
                    const key = entry.key;
                    const value = try entry.value.toJSON(allocator);
                    try entries.put(key, value);
                }
                return json.Value{ .object = entries };
            },
        }
    }

    pub fn free(self: BencodeValue, allocator: std.mem.Allocator) !void {
        switch (self) {
            .Int => {},
            .String => allocator.free(self.String),
            .List => {
                for (self.List) |*item| try item.free(allocator);
                allocator.free(self.List);
            },
            .Dict => {
                for (self.Dict) |*entry| {
                    allocator.free(entry.key);
                    try entry.value.free(allocator);
                }
                allocator.free(self.Dict);
            },
        }
    }
};

pub fn parse(allocator: std.mem.Allocator, input: []const u8) Error!BencodeValue {
    var index: usize = 0;
    return parseValue(allocator, input, &index);
}

fn parseValue(allocator: std.mem.Allocator, input: []const u8, index: *usize) Error!BencodeValue {
    if ((index.*) >= input.len) return Error.UnexpectedEndOfInput;

    const c = input[index.*];

    if (c == 'i') {
        return parseInt(input, index);
    } else if (std.ascii.isDigit(c)) {
        return parseString(allocator, input, index);
    } else if (c == 'l') {
        return parseList(allocator, input, index);
    } else if (c == 'd') {
        return parseDict(allocator, input, index);
    } else return Error.InvalidFormat;
}

fn parseInt(input: []const u8, index: *usize) Error!BencodeValue {
    // Consume 'i'
    index.* += 1;

    const start = index.*;

    while (index.* < input.len and input[index.*] != 'e') index.* += 1;
    if (index.* >= input.len) return Error.UnexpectedEndOfInput;

    const number_str = input[start..index.*];
    const number = std.fmt.parseInt(i64, number_str, 10) catch return Error.InvalidInteger;

    // Consume 'e'
    index.* += 1;

    return BencodeValue{ .Int = number };
}

fn parseString(allocator: std.mem.Allocator, input: []const u8, index: *usize) Error!BencodeValue {
    // Parse length
    const start = index.*;

    while (index.* < input.len and input[index.*] != ':') index.* += 1;
    if (index.* >= input.len) return Error.UnexpectedEndOfInput;

    const length_str = input[start..index.*];
    const length = std.fmt.parseInt(usize, length_str, 10) catch return Error.InvalidStringLength;

    // Consume ':'
    index.* += 1;

    if ((index.*) + length > input.len) return Error.UnexpectedEndOfInput;

    const str = input[index.* .. index.* + length];

    index.* += length;

    // Allocate and copy the string
    const copy = try allocator.alloc(u8, str.len);
    @memcpy(copy, str);

    return BencodeValue{ .String = copy };
}

fn parseList(allocator: std.mem.Allocator, input: []const u8, index: *usize) Error!BencodeValue {
    // Consume 'l'
    index.* += 1;

    var items = std.ArrayList(BencodeValue).init(allocator);

    while ((index.*) < input.len and input[index.*] != 'e') {
        const item = try parseValue(allocator, input, index);
        try items.append(item);
    }

    if ((index.*) >= input.len) return Error.UnexpectedEndOfInput;

    // Consume 'e'
    index.* += 1;

    return BencodeValue{ .List = try items.toOwnedSlice() };
}

fn parseDict(allocator: std.mem.Allocator, input: []const u8, index: *usize) Error!BencodeValue {
    // Consume 'd'
    index.* += 1;

    var entries = std.ArrayList(BencodeValue.Entry).init(allocator);

    while ((index.*) < input.len and input[index.*] != 'e') {
        const key_value = try parseString(allocator, input, index);

        const key = key_value.String;
        const value = try parseValue(allocator, input, index);

        try entries.append(.{ .key = key, .value = value });
    }

    if ((index.*) >= input.len) return Error.UnexpectedEndOfInput;

    // Consume 'e'
    index.* += 1;

    return BencodeValue{ .Dict = try entries.toOwnedSlice() };
}

