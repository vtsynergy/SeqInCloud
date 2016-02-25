//Copyright (c) 2010 Aalto University
//Copyright (c) 2012-2013 by Virginia Polytechnic Institute and State University
//All rights reserved.
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
//

package org.broadinstitute.sting.gatk.hadoop;

import org.broadinstitute.sting.gatk.hadoop.hbamsrc.SplittingBAMIndex;

import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.broadinstitute.sting.gatk.hadoop.FileVirtualSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class LociInputFormat extends FileInputFormat<LongWritable, Text> {
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext ctx)
			throws InterruptedException, IOException

	{
		final RecordReader<LongWritable, Text> arr = new LociRecordReader();
		return arr;
	}

	private Path getIdxPath(Path path) 
        {
		String pstr = path.toString();
		String nstr = pstr.replace(path.getName(), "_" + path.getName()
				+ ".splitting-bai");

		return (new Path(nstr));
	}

	/* The splits returned are FileVirtualSplits. */
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		final List<InputSplit> splits = super.getSplits(job);

		// Align the splits so that they don't cross blocks.
		// addIndexedSplits() requires the given splits to be sorted by file
		// path, so do so. Although FileInputFormat.getSplits() does, at the
		// time of writing this, generate them in that order, we shouldn't rely on
		// it.
		Collections.sort(splits, new Comparator<InputSplit>() {
			public int compare(InputSplit a, InputSplit b) {
				FileSplit fa = (FileSplit) a, fb = (FileSplit) b;
				return fa.getPath().compareTo(fb.getPath());
			}
		});

		final List<InputSplit> newSplits = new ArrayList<InputSplit>(splits
				.size());

		final Configuration cfg = job.getConfiguration();


		for (int i = 0; i < splits.size();) {
			try {
				i = addIndexedSplits(splits, i, newSplits, cfg);
			} catch (IOException e) {
				System.out.println(e.getMessage());
                                System.exit(-1);
			}
		}
		return newSplits;
	}

	// Handles all the splits that share the Path of the one at index i,
	// returning the next index to be used.
	private int addIndexedSplits(List<InputSplit> splits, int i,
			List<InputSplit> newSplits, Configuration cfg) throws IOException {
		final Path file = ((FileSplit) splits.get(i)).getPath();

		final SplittingBAMIndex idx = new SplittingBAMIndex(file.getFileSystem(
				cfg).open(getIdxPath(file)));

		int splitsEnd = splits.size();
		for (int j = i; j < splitsEnd; ++j)
			if (!file.equals(((FileSplit) splits.get(j)).getPath()))
				splitsEnd = j;

		for (int j = i; j < splitsEnd; ++j) {
			final FileSplit fileSplit = (FileSplit) splits.get(j);

			final long start = fileSplit.getStart();
			final long end = start + fileSplit.getLength();

			final Long blockStart = idx.nextAlignment(start);

			// The last split needs to end where the last alignment ends, but
			// the index doesn't store that data (whoops); we only know where the
			// last alignment begins. Fortunately there's no need to change the index
			// format for this: we can just set the end to the maximal length of
			// the final BGZF block (0xffff), and then read until BAMRecordCodec
			// hits EOF.
			Long blockEnd =
			// j == splitsEnd-1 ? idx.prevAlignment(end) | 0xffff
			j == splitsEnd - 1 ? idx.prevAlignment(end) : idx
					.nextAlignment(end);

			if (blockStart == null)
				throw new RuntimeException(
						"Internal error or invalid index: no block start for "
								+ start);

			if (blockEnd == null)
				throw new RuntimeException(
						"Internal error or invalid index: no block end for "
								+ end);
			newSplits.add(new FileVirtualSplit(file, blockStart, blockEnd,
					fileSplit.getLocations()));
		}
		return splitsEnd;
	}

	@Override
	public boolean isSplitable(JobContext job, Path path) {
		return true;
	}
}
