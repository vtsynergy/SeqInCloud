// Copyright (c) 2010 Aalto University
// Copyright (c) 2012-2013 by Virginia Polytechnic Institute and State
// University
// All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

// File created: 2010-08-03 12:20:20

package org.broadinstitute.sting.gatk.hadoop;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import net.sf.samtools.util.BlockCompressedInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.broadinstitute.sting.gatk.hadoop.hbamsrc.SplittingBAMIndex;
import org.broadinstitute.sting.gatk.hadoop.hbamsrc.WrapSeekable;

/**
 * An indexing tool for BAM files, making them palatable to
 * {@link BAMInputFormat}. Writes splitting BAM indices as understood by
 * {@link SplittingBAMIndex}.
 */
public final class SplittingBAMIndexer {
	private final ByteBuffer byteBuffer;
	private final int granularity;

	private static final int PRINT_EVERY = 500 * 1024 * 1024;

	public SplittingBAMIndexer(int g) {
		granularity = g;
		byteBuffer = ByteBuffer.allocate(8); // Enough to fit a long
	}

	public void index(final Path file, Configuration cfg) throws IOException {

		FileSystem fs = file.getFileSystem(cfg);

		final FSDataInputStream fin = fs.open(file);

		fin.seek(0);

		final BlockCompressedInputStream in = new BlockCompressedInputStream(
				new WrapSeekable<FSDataInputStream>(fin, fs.getFileStatus(file)
						.getLen(), file));

		String pstr = file.toString();
		String nstr = pstr.replace(file.getName(), "_" + file.getName()
				+ ".splitting-bai");

		FSDataOutputStream fout = fs.create(new Path(nstr), true);

		final OutputStream out = new BufferedOutputStream(fout);

		final LongBuffer lb = byteBuffer.order(ByteOrder.BIG_ENDIAN)
				.asLongBuffer();

		skipToAlignmentList(in);

		// Always write the first one to make sure it's not skipped
		lb.put(0, in.getFilePointer());
		out.write(byteBuffer.array());

		long prevPrint = in.getFilePointer() >> 16;

		for (int i = 0;;) {
			final PtrSkipPair pair = readAlignment(in);
			if (pair == null)
				break;

			if (++i == granularity) {
				i = 0;
				lb.put(0, pair.ptr);
				out.write(byteBuffer.array());

				final long filePos = pair.ptr >> 16;
				if (filePos - prevPrint >= PRINT_EVERY) {
					System.out.print("-");
					prevPrint = filePos;
				}
			}
			fullySkip(in, pair.skip);
		}
		lb.put(0, fs.getFileStatus(file).getLen() << 16);
		out.write(byteBuffer.array());
		out.close();
		in.close();
	}

	public void index(final File file) throws IOException {
		final BlockCompressedInputStream in = new BlockCompressedInputStream(
				file);

		final OutputStream out = new BufferedOutputStream(new FileOutputStream(
				file.getPath() + ".splitting-bai"));

		final LongBuffer lb = byteBuffer.order(ByteOrder.BIG_ENDIAN)
				.asLongBuffer();

		skipToAlignmentList(in);

		lb.put(0, in.getFilePointer());
		out.write(byteBuffer.array());

		long prevPrint = in.getFilePointer() >> 16;

		for (int i = 0;;) {
			final PtrSkipPair pair = readAlignment(in);
			if (pair == null)
				break;

			if (++i == granularity) {
				i = 0;
				lb.put(0, pair.ptr);
				out.write(byteBuffer.array());

				final long filePos = pair.ptr >> 16;
				if (filePos - prevPrint >= PRINT_EVERY) {
					System.out.print("-");
					prevPrint = filePos;
				}
			}
			fullySkip(in, pair.skip);
		}
		lb.put(0, file.length() << 16);
		out.write(byteBuffer.array());
		out.close();
		in.close();
	}

	private void skipToAlignmentList(final InputStream in) throws IOException {
		// Check magic number
		if (!readExactlyBytes(in, 4))
			ioError("Invalid BAM header: too short, no magic");

		final int magic = byteBuffer.order(ByteOrder.BIG_ENDIAN).getInt(0);
		if (magic != 0x42414d01)
			ioError("Invalid BAM header: bad magic %#x != 0x42414d01", magic);

		// Skip the SAM header
		if (!readExactlyBytes(in, 4))
			ioError("Invalid BAM header: too short, no SAM header length");

		byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

		final int samLen = byteBuffer.getInt(0);
		if (samLen < 0)
			ioError("Invalid BAM header: negative SAM header length %d", samLen);

		fullySkip(in, samLen);

		// Get the number of reference sequences
		if (!readExactlyBytes(in, 4))
			ioError("Invalid BAM header: too short, no reference sequence count");

		final int referenceSeqs = byteBuffer.getInt(0);

		// Skip over each reference sequence datum individually
		for (int s = 0; s < referenceSeqs; ++s) {
			if (!readExactlyBytes(in, 4))
				ioError("Invalid reference list: EOF before reference %d",
						s + 1);

			// Skip over the name + the int giving the sequence length
			fullySkip(in, byteBuffer.getInt(0) + 4);
		}
	}

	private static final class PtrSkipPair {
		public long ptr;
		public int skip;

		public PtrSkipPair(long p, int s) {
			ptr = p;
			skip = s;
		}
	}

	private PtrSkipPair readAlignment(final BlockCompressedInputStream in)
			throws IOException {
		final long ptr = in.getFilePointer();
		final int read = readBytes(in, 4);
		if (read != 4) {
			if (read == 0)
				return null;
			ioError("Invalid alignment at virtual offset %#x: "
					+ "less than 4 bytes long", in.getFilePointer());
		}
		return new PtrSkipPair(ptr, byteBuffer.getInt(0));
	}

	private void fullySkip(final InputStream in, final int skip)
			throws IOException {
		// Skip repeatedly until we're either done skipping or can't skip any
		// more, in case some kind of IO error is temporarily preventing it.
		// That kind of situation might not necessarily be possible; the docs are
		// rather vague about the whole thing.
		for (int s = skip; s > 0;) {
			final long skipped = in.skip(s);
			if (skipped == 0)
				throw new IOException("Skip failed");
			s -= skipped;
		}
	}

	private int readBytes(final InputStream in, final int n) throws IOException {
		assert n <= byteBuffer.capacity();

		int read = 0;
		while (read < n) {
			final int readNow = in.read(byteBuffer.array(), read, n - read);
			if (readNow <= 0)
				break;
			read += readNow;
		}
		return read;
	}

	private boolean readExactlyBytes(final InputStream in, final int n)
			throws IOException {
		return readBytes(in, n) == n;
	}

	private void ioError(String s, Object... va) throws IOException {
		throw new IOException(String.format(s, va));
	}
}
