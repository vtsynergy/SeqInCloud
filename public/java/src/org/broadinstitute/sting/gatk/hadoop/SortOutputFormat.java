/* Copyright (c) 2011 Aalto University
 * Copyright (c) 2012-2013 by Virginia Polytechnic Institute and State
 * University
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
 * THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.broadinstitute.sting.gatk.hadoop;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.broadinstitute.sting.gatk.hadoop.hbamsrc.SAMRecordWritable;

public class SortOutputFormat extends KeyIgnoringBAMOutputFormat<NullWritable> {

	public SortOutputFormat() {
	}

	@Override
	public RecordWriter<NullWritable, SAMRecordWritable> getRecordWriter(
			TaskAttemptContext context) throws IOException {
		int pos = 0;
                if (context == null) {
                    throw new IOException("context is NULL");
                }

		Path[] p = FileInputFormat.getInputPaths(context);
                assert(p.length > 0);

		FileSystem fs = p[0].getFileSystem(context.getConfiguration());
		FileStatus[] status = fs.listStatus(p[0]);

		for (int i = 0; i < status.length; i++) {
			if ((status[i].getPath().getName()).endsWith(".bam")) {
				pos = i;
				break;
			}
		}

		if (super.header == null) {
			Configuration c = context.getConfiguration();
			readSAMHeaderFrom(status[pos].getPath(), fs);
			if ((context.getConfiguration().getBoolean("gatk.hadoop.ismerge", false)) == false) {
				setWriteHeader(true);
			} else if (context.getTaskAttemptID().getTaskID().getId() == 0) {
				setWriteHeader(true);
			}
		}
		return super.getRecordWriter(context);
	}

	@Override
	public Path getDefaultWorkFile(TaskAttemptContext context, String ext)
			throws IOException {
		String filename = context.getTaskAttemptID().toString();
		String extension = ext.isEmpty() ? ext : "." + ext;
		int part = context.getTaskAttemptID().getTaskID().getId();
		return new Path(super.getDefaultWorkFile(context, ext).getParent(),
				String.format("%06d", part) + "-" + filename + extension);
	}

	// Allow the output directory to exist, so that we can make multiple jobs
	// that write into it.
	@Override
	public void checkOutputSpecs(JobContext job)
			throws FileAlreadyExistsException, IOException {
	}
}
