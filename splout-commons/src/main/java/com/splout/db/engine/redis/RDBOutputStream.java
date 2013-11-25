package com.splout.db.engine.redis;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class RDBOutputStream {
	OutputStream out;

	public RDBOutputStream(OutputStream out) {
		this.out = out;
	}

	public void close() throws IOException {
		out.flush();
		out.close();
	}

	public void writeHeader() throws IOException {
		out.write("REDIS".getBytes());
		out.write("0001".getBytes());
	}

	public void writeFooter() throws IOException {
		out.write(0xFF);
	}

	public void writeDatabaseSelector(long db) throws IOException {
		out.write(0xFE);
		writeLength(db);
	}

	public void writeString(RDBString key, RDBString value) throws IOException {
		out.write(0x00);
		write(key);
		write(value);
	}

	public void writeList(RDBString key, RDBString[] values) throws IOException {
		out.write(0x01);
		write(key);
		writeLength(values.length);
		for(int i = 0; i < values.length; i++)
			write(values[i]);
	}

	public void writeSet(RDBString key, RDBString[] values) throws IOException {
		out.write(0x02);
		write(key);
		writeLength(values.length);
		for(int i = 0; i < values.length; i++)
			write(values[i]);
	}

	public void writeHash(RDBString key, RDBString[] hashKeys, RDBString[] values) throws IOException {
		if(hashKeys.length != values.length)
			throw new RuntimeException("Must have same number of keys and values");

		out.write(0x04);
		write(key);
		writeLength(hashKeys.length);
		for(int i = 0; i < hashKeys.length; i++) {
			write(hashKeys[i]);
			write(values[i]);
		}
	}

	void write(int b) throws IOException {
		out.write(b);
	}

	void write(byte[] bytes) throws IOException {
		out.write(bytes);
	}

	void write(RDBString string) throws IOException {
		string.write(this);
	}

	void writeLength(long len) throws IOException {
		if(len < (1 << 6)) {
			out.write((int) (len & 0xFF));
		} else if(len < (1 << 14)) {
			out.write((int) (((len >> 8) & 0xFF) | 0x40));
			out.write((int) (len & 0xFF));
		} else if(len < (1L << 32)) {
			out.write(0x80);
			out.write((int) ((len >> 24) & 0xFF));
			out.write((int) ((len >> 16) & 0xFF));
			out.write((int) ((len >> 8) & 0xFF));
			out.write((int) (len & 0xFF));
		} else {
			throw new RuntimeException("length is too long");
		}
	}

	public static void main(String[] args) throws FileNotFoundException, IOException {
		RDBOutputStream rdb = new RDBOutputStream(new FileOutputStream(args[0]));
		rdb.writeHeader();
		rdb.writeDatabaseSelector(0);
		rdb.writeString(
		    RDBString.create("foo"),
		    RDBString
		        .create("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
		rdb.writeFooter();
		rdb.close();
	}
}
