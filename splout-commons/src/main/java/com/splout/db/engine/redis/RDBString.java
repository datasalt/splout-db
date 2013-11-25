package com.splout.db.engine.redis;

import java.io.IOException;

public abstract class RDBString {
	abstract void write(RDBOutputStream out) throws IOException;

	public static RDBString uncompressed(byte[] bytes) {
		return new RDBByteString(bytes);
	}

	public static RDBString create(byte[] bytes) throws IOException {
		return uncompressed(bytes);
	}

	public static RDBString create(String string) throws IOException {
		return create(string.getBytes());
	}

	public static RDBString create(int i) {
		return new RDBIntString(i);
	}
}

class RDBByteString extends RDBString {
	byte[] bytes;

	RDBByteString(byte[] bytes) {
		this.bytes = bytes;
	}

	void write(RDBOutputStream out) throws IOException {
		out.writeLength(bytes.length);
		out.write(bytes);
	}
}

class RDBIntString extends RDBString {
	int value;

	RDBIntString(int value) {
		this.value = value;
	}

	void write(RDBOutputStream out) throws IOException {
		if(value >= -(1 << 7) && value <= (1 << 7) - 1) {
			out.write(0xC0);
			out.write(value & 0xFF);
		} else if(value >= -(1 << 15) && value <= (1 << 15) - 1) {
			out.write(0xC1);
			out.write(value & 0xFF);
			out.write((value >> 8) & 0xFF);
		} else {
			out.write(0xC2);
			out.write(value & 0xFF);
			out.write((value >> 8) & 0xFF);
			out.write((value >> 16) & 0xFF);
			out.write((value >> 24) & 0xFF);
		}
	}
}