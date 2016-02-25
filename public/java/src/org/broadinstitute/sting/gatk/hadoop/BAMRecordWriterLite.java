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
import net.sf.samtools.util.BinaryCodec;

import net.sf.samtools.BAMRecordCodec;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMSequenceDictionary;
import net.sf.samtools.SAMSequenceRecord;
import org.apache.hadoop.conf.Configuration;
import java.io.File;

import net.sf.samtools.util.BlockCompressedOutputStream;

/**
 * A base {@link RecordWriter} for BAM records.
 * 
 * <p>
 * Handles the output stream, writing the header if requested, and provides the
 * {@link #writeAlignment} function for subclasses.
 * </p>
 */
public class BAMRecordWriterLite {
	private BinaryCodec binaryCodec;
	private BAMRecordCodec recordCodec;
	private SAMFileHeader header;

	/** A SAMFileHeader is read from the input Path. */
	public BAMRecordWriterLite(Path output, Path input, boolean writeHeader,
			Configuration cfg) throws IOException {
		final SAMFileReader r = new SAMFileReader(input.getFileSystem(cfg)
				.open(input));

		final SAMFileHeader hdr = r.getFileHeader();
		r.close();
		init(output, hdr, writeHeader, cfg);
	}

	public BAMRecordWriterLite(Path output, SAMFileHeader header,
			boolean writeHeader, Configuration cfg) throws IOException {
		FileSystem fs = output.getFileSystem(cfg);
		init(fs.create(output, true), header, writeHeader);
	}

	public BAMRecordWriterLite(OutputStream output, SAMFileHeader header,
			boolean writeHeader) throws IOException {
		init(output, header, writeHeader);
	}

	// Working around not being able to call a constructor other than as the
	// first statement...
	private void init(Path output, SAMFileHeader header, boolean writeHeader,
			Configuration cfg) throws IOException {
		FileSystem fs = output.getFileSystem(cfg);
		init(fs.create(output, true), header, writeHeader);
	}

	private void init(OutputStream output, SAMFileHeader header,
			boolean writeHeader) throws IOException {
		// BlockCompressedOutputStream.setDefaultCompressionLevel(0);
		final OutputStream compressedOut = new BlockCompressedOutputStream(
				output, new File(""));

		binaryCodec = new BinaryCodec(compressedOut);
		recordCodec = new BAMRecordCodec(header);
		recordCodec.setOutputStream(compressedOut);

		this.header = header;
		if (writeHeader)
			this.writeHeader(header);
	}

	public SAMFileHeader getFileHeader() {
		return this.header;
	}

	public void close() {
		binaryCodec.close();
	}

	public void writeAlignment(final SAMRecord rec) {
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
