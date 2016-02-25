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

// File created: 2010-08-11 12:19:23

package org.broadinstitute.sting.gatk.hadoop;

import java.io.InputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.broadinstitute.sting.gatk.hadoop.hbamsrc.BAMOutputFormat;
import org.broadinstitute.sting.gatk.hadoop.hbamsrc.SAMRecordWritable;

import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFileHeader;

/**
 * Writes only the BAM records, not the key.
 * 
 * <p>
 * A {@link SAMFileHeader} must be provided via {@link #setSAMHeader} or
 * {@link #readSAMHeaderFrom} before {@link #getRecordWriter} is called.
 * </p>
 * 
 * <p>
 * Optionally, the BAM header may be written to the output file(s). This
 * defaults to false, because in distributed usage one often ends up with (and,
 * for decent performance, wants to end up with) multiple files, and one does
 * not want the header in each file.
 * </p>
 */
public class KeyIgnoringBAMOutputFormat<K> extends BAMOutputFormat<K> {
	protected SAMFileHeader header;
	private boolean writeHeader = false;

	public KeyIgnoringBAMOutputFormat() {
	}

	/** Whether the header will be written or not. */
	public boolean getWriteHeader() {
		return writeHeader;
	}

	/** Set whether the header will be written or not. */
	public void setWriteHeader(boolean b) {
		writeHeader = b;
	}

	public void setSAMHeader(SAMFileHeader header) {
		this.header = header;
	}

	public void readSAMHeaderFrom(Path path, FileSystem fs) throws IOException {
		InputStream i = fs.open(path);
		readSAMHeaderFrom(i);
		i.close();
	}

	public void readSAMHeaderFrom(InputStream in) {
		this.header = new SAMFileReader(in).getFileHeader();
	}

	/**
	 * <code>setSAMHeader</code> or <code>readSAMHeaderFrom</code> must have been
	 * called first.
	 */
	@Override
	public RecordWriter<K, SAMRecordWritable> getRecordWriter(
			TaskAttemptContext ctx) throws IOException {
		if (this.header == null)
			throw new IOException(
					"Can't create a RecordWriter without the SAM header");

		return new KeyIgnoringBAMRecordWriter<K>(
				getDefaultWorkFile(ctx, "bam"), this.header, this.writeHeader,
				ctx);
	}
}
