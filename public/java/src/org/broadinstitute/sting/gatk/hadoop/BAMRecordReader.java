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

// File created: 2010-08-09 14:34:08

package org.broadinstitute.sting.gatk.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.broadinstitute.sting.gatk.hadoop.hbamsrc.SAMRecordWritable;
import org.broadinstitute.sting.gatk.hadoop.hbamsrc.WrapSeekable;

import net.sf.samtools.util.BlockCompressedInputStream;

import net.sf.samtools.BAMRecordCodec;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;

/**
 * The key is the bitwise OR of the reference sequence ID in the upper 32 bits
 * and the 0-based leftmost coordinate in the lower.
 */
public class BAMRecordReader extends
		RecordReader<LongWritable, SAMRecordWritable> {
	private final LongWritable key = new LongWritable();
	private final SAMRecordWritable record = new SAMRecordWritable();

	private BlockCompressedInputStream bci;
	private BAMRecordCodec codec;
	private long fileStart, virtualEnd;

	@Override
	public void initialize(InputSplit spl, TaskAttemptContext ctx)
			throws IOException {
		final FileVirtualSplit split = (FileVirtualSplit) spl;

		final Path file = split.getPath();
		final FileSystem fs = file.getFileSystem(ctx.getConfiguration());

		final FSDataInputStream in = fs.open(file);
		codec = new BAMRecordCodec(new SAMFileReader(in).getFileHeader());

		in.seek(0);
		bci = new BlockCompressedInputStream(
				new WrapSeekable<FSDataInputStream>(in, fs.getFileStatus(file)
						.getLen(), file));

		final long virtualStart = split.getStartVirtualOffset();

		fileStart = virtualStart >>> 16;
		virtualEnd = split.getEndVirtualOffset();

		bci.seek(virtualStart);
		codec.setInputStream(bci);
	}

	@Override
	public void close() throws IOException {
		bci.close();
	}

	/**
	 * Unless the end has been reached, this only takes file position into
	 * account, not the position within the block.
	 */
	@Override
	public float getProgress() {
		final long virtPos = bci.getFilePointer();
		final long filePos = virtPos >>> 16;
		if (virtPos >= virtualEnd)
			return 1;
		else {
			final long fileEnd = virtualEnd >>> 16;
			// Add 1 to the denominator to make sure it doesn't reach 1 here
			// when filePos == fileEnd.
			return (float) (filePos - fileStart) / (fileEnd - fileStart + 1);
		}
	}

	@Override
	public LongWritable getCurrentKey() {
		return key;
	}

	@Override
	public SAMRecordWritable getCurrentValue() {
		return record;
	}

	@Override
	public boolean nextKeyValue() {
		if (bci.getFilePointer() >= virtualEnd)
			return false;

		final SAMRecord r = codec.decode();
		if (r == null) {
			return false;
		}
		if (r.getReferenceName().equals("*"))
			key.set(Long.MAX_VALUE);
		else
			key.set((long) r.getReferenceIndex() << 32 | r.getAlignmentStart()
					- 1);
		record.set(r);
		return true;
	}
}
