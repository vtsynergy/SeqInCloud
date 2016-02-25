/* Copyright (c) 2012-2013 by Virginia Polytechnic Institute and State
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
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT REPRESENTATIONS 
 * OR WARRANTIES OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT 
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS 
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT, OR THE 
 * ABSENCE OF LATENT OR OTHER DEFECTS, WHETHER OR NOT 
 * DISCOVERABLE. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT 
 * HOLDERS BE LIABLE FOR ANY CLAIM, INCIDENTAL OR CONSEQUENTIAL 
 * DAMAGES OF ANY KIND, OR OTHER LIABILITY, WHETHER IN AN ACTION 
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN 
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
 * IN THE SOFTWARE.
 */

package org.broadinstitute.sting.gatk.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.broadinstitute.sting.gatk.hadoop.hadoopsrc.LineRecordReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;

public class NLineXRecordReader extends RecordReader<LongWritable, Text> {

	private LongWritable key = new LongWritable();
	private Text value = new Text();

	private long end;
	private boolean terminate;
	private final String[] lines = new String[4];
	private final long[] poss = new long[4];
        private long fileId = 0, splitId = 0;
        private Configuration conf;

	private String filename;

	private LineRecordReader lrr;

	@Override
	public void close() throws IOException {
		this.lrr.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return this.lrr.getProgress();
	}

	@Override
	public void initialize(final InputSplit inputSplit,
			final TaskAttemptContext taskAttemptContext) throws IOException,
			InterruptedException {
		long start;

                this.conf = taskAttemptContext.getConfiguration();
		this.lrr = new LineRecordReader();
		this.lrr.initialize(inputSplit, taskAttemptContext);

                XFileSplit xfs = (XFileSplit) inputSplit;

		Path p = xfs.getPath();
		filename = new String(p.getName());

                fileId = xfs.getFileId();
                splitId = xfs.getSplitId();

		FileSystem fs = p.getFileSystem(conf);
		fs.open(p);
		start = xfs.getStart();
		end = start + xfs.getLength();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		int count = 0;
		boolean found = false;

		if (this.terminate)
			return false;

		while (!found) {

			if (!this.lrr.nextKeyValue()) {
				return false;
			}

			final String s = this.lrr.getCurrentValue().toString().trim();

			// Prevent empty lines
			if (s.length() == 0)
				continue;

			this.lines[count] = s;
			this.poss[count] = this.lrr.getCurrentKey().get();

			if (((this.poss[count] + s.length()) >= this.end) && count == 3) {
				this.terminate = true;
			}

			if (count < 3)
				count++;
			else {

				if (this.lines[0].charAt(0) == '@'
						&& this.lines[2].charAt(0) == '+')
					found = true;
				else {
					// Shift lines
					this.lines[0] = this.lines[1];
					this.lines[1] = this.lines[2];
					this.lines[2] = this.lines[3];

					// Shift positions
					this.poss[0] = this.poss[1];
					this.poss[1] = this.poss[2];
					this.poss[2] = this.poss[3];
				}
			}
		}
		// Set key
		this.key = new LongWritable(splitId);
		this.key.set(key.get() & 0x3FFFFFFFFFFFFFFFL);

		if (conf.getBoolean("gatk.hadoop.pairedend", false)) {
			if (fileId == 1) {
				this.key.set(key.get() | 0x4000000000000000L);
			} else {
				this.key.set(key.get() | 0x8000000000000000L);
			}
		}

		// Set value
		this.value = new Text(this.lines[0] + '\n' + this.lines[1] + '\n'
				+ this.lines[2] + '\n' + this.lines[3]);

		// Clean array
		this.lines[0] = this.lines[1] = this.lines[2] = this.lines[3] = null;

		return true;
	}
}
