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

// File created: 2010-08-10 13:03:10

package org.broadinstitute.sting.gatk.hadoop;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.broadinstitute.sting.gatk.hadoop.hbamsrc.SAMRecordWritable;

import net.sf.samtools.util.BinaryCodec;

import net.sf.samtools.BAMRecordCodec;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMSequenceDictionary;
import net.sf.samtools.SAMSequenceRecord;
import java.io.File;

/**
 * A base {@link RecordWriter} for BAM records.
 * 
 * <p>
 * Handles the output stream, writing the header if requested, and provides the
 * {@link #writeAlignment} function for subclasses.
 * </p>
 */
public abstract class BAMRecordWriter<K> extends
		RecordWriter<K, SAMRecordWritable> {
	private BinaryCodec binaryCodec;
	private BAMRecordCodec recordCodec;

	/** A SAMFileHeader is read from the input Path. */
	public BAMRecordWriter(Path output, Path input, boolean writeHeader,
			TaskAttemptContext ctx) throws IOException {
		final SAMFileReader r = new SAMFileReader(input.getFileSystem(
				ctx.getConfiguration()).open(input));

		final SAMFileHeader hdr = r.getFileHeader();
		r.close();
		init(output, hdr, writeHeader, ctx);
	}

	public BAMRecordWriter(Path output, SAMFileHeader header,
			boolean writeHeader, TaskAttemptContext ctx) throws IOException {
		FileSystem fs = output.getFileSystem(ctx.getConfiguration());
		init(fs.create(output, true), header, writeHeader, ctx);
	}

	// Working around not being able to call a constructor other than as the
	// first statement...
	private void init(Path output, SAMFileHeader header, boolean writeHeader,
			TaskAttemptContext ctx) throws IOException {
                FileSystem fs = output.getFileSystem(ctx.getConfiguration());
		init(fs.create(output, true), header, writeHeader, ctx);
	}

	private void init(OutputStream output, SAMFileHeader header,
			boolean writeHeader, TaskAttemptContext ctx) throws IOException {
		final OutputStream compressedOut;
		if ((ctx.getConfiguration().getBoolean("gatk.hadoop.ismerge", false)) == true) {
			// BlockCompressedOutputStream.setDefaultCompressionLevel(0);
			compressedOut = new BlockCompressedOutputStream(output,
					new File(""));
		} else {
			// net.sf.samtools.util.BlockCompressedOutputStream.setDefaultCompressionLevel(0);
			compressedOut = new net.sf.samtools.util.BlockCompressedOutputStream(
					output, new File(""));
		}

		binaryCodec = new BinaryCodec(compressedOut);
		recordCodec = new BAMRecordCodec(header);
		recordCodec.setOutputStream(compressedOut);

		if (writeHeader)
			this.writeHeader(header);
	}

	@Override
	public void close(TaskAttemptContext ctx) {
		binaryCodec.close();
	}

	protected void writeAlignment(final SAMRecord rec) {
		recordCodec.encode(rec);
	}

	private void writeHeader(final SAMFileHeader header) {
		binaryCodec.writeBytes("BAM\001".getBytes());
		binaryCodec.writeString(header.getTextHeader(), true, false);

		final SAMSequenceDictionary dict = header.getSequenceDictionary();

		binaryCodec.writeInt(dict.size());
		for (final SAMSequenceRecord rec : dict.getSequences()) {
			binaryCodec.writeString(rec.getSequenceName(), true, true);
			binaryCodec.writeInt(rec.getSequenceLength());
		}
	}
}
