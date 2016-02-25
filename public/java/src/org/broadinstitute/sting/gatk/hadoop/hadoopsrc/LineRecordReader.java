/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.broadinstitute.sting.gatk.hadoop.hadoopsrc;

import java.io.IOException;

// Removed logging
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;
import org.broadinstitute.sting.gatk.hadoop.XFileSplit;

/**
 * Treats keys as offset in file and value as line.
 */
public class LineRecordReader extends RecordReader<LongWritable, Text> {

	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength;
	private LongWritable key = null;
	private Text value = null;

	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		XFileSplit split = (XFileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		final Path file = split.getPath();

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		this.start = split.getStart();
		fileIn.seek(this.start);
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);
		in = new LineReader(fileIn, job);
		this.pos = start;
		this.end = Long.MAX_VALUE;
	}

	public boolean nextKeyValue() throws IOException {
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = new Text();
		}
		int newSize = 0;
		while (pos < end) {
			newSize = in.readLine(value, maxLineLength, Math.max((int) Math
					.min(Integer.MAX_VALUE, end - pos), maxLineLength));
			if (newSize == 0) {
				break;
			}
			pos += newSize;
			if (newSize < maxLineLength) {
				break;
			}

		}
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}

	@Override
	public LongWritable getCurrentKey() {
		return key;
	}

	@Override
	public Text getCurrentValue() {
		return value;
	}

	/**
	 * Get the progress within the split
	 */
	public float getProgress() {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	public synchronized void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}
}
