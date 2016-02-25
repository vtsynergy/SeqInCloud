/* Copyright (c) 2010 Aalto University
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

import org.broadinstitute.sting.gatk.hadoop.hbamsrc.SplittingBAMIndex;
import org.broadinstitute.sting.gatk.hadoop.hbamsrc.WrapSeekable;
import org.broadinstitute.sting.gatk.walkers.indels.IndelRealigner;

import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import net.sf.samtools.util.BlockCompressedInputStream;

import net.sf.samtools.BAMRecordCodec;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;

public class LociRecordReader extends RecordReader<LongWritable, Text> {
	private final LongWritable key = new LongWritable();
	private final Text record = new Text();

	private BlockCompressedInputStream bci;
	private BAMRecordCodec codec;

	private long virtualStart, virtualEnd, fileStart, fileEnd;
	private boolean called = false;
	private FileSystem fs;
	private Path file;
	private SplittingBAMIndex idx;
	private String jobDir;
	public static StringBuilder realign_intervals;
	private String attemptID;

	private Path getIdxPath(Path path) {
		String pstr = path.toString();
		String nstr = pstr.replace(path.getName(), "_" + path.getName()
				+ ".splitting-bai");

		return (new Path(nstr));
	}

	public void initialize(InputSplit spl, TaskAttemptContext ctx)
			throws IOException {
		final FileVirtualSplit split = (FileVirtualSplit) spl;

		file = split.getPath();
		fs = file.getFileSystem(ctx.getConfiguration());

		final FSDataInputStream in = fs.open(file);
		codec = new BAMRecordCodec(new SAMFileReader(in).getFileHeader());

		in.seek(0);
		bci = new BlockCompressedInputStream(
				new WrapSeekable<FSDataInputStream>(in, fs.getFileStatus(file)
						.getLen(), file));

		virtualStart = split.getStartVirtualOffset();
		fileStart = virtualStart >>> 16;
		virtualEnd = split.getEndVirtualOffset();
		fileEnd = virtualEnd >>> 16;
		idx = new SplittingBAMIndex(file.getFileSystem(ctx.getConfiguration())
				.open(getIdxPath(file)));
		codec.setInputStream(bci);
		bci.seek(virtualStart);

		JobConf job = new JobConf(ctx.getConfiguration());
		jobDir = new String(job.getJobLocalDir());
		attemptID = ctx.getTaskAttemptID().toString();
	}

	@Override
	public float getProgress() {
		final long virtPos = bci.getFilePointer();
		final long filePos = virtPos >>> 16;
		if (virtPos >= virtualEnd)
			return 1;
		else {
			// Add 1 to the denominator to make sure it doesn't reach 1 here
			// when filePos == fileEnd.
			return (float) (filePos - fileStart) / (fileEnd - fileStart + 1);
		}
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public LongWritable getCurrentKey() {
		return key;
	}

	@Override
	public Text getCurrentValue() {
		return record;
	}

	@Override
	public boolean nextKeyValue() throws IOException {

		SAMRecord firstrecord, lastrecord, r;
		long pblockEnd, blockEnd;
		int initialIndex, finalIndex;
		boolean lastSplit = false;

		if (called)
			return false;

		if (bci.getFilePointer() >= virtualEnd)
			return false;

		r = codec.decode();
		if (r == null) {
			System.out.println("r is NULL");
			return false;
		}

		if (r.getAlignmentStart() == 0) {
			System.out
					.println("Invalid split as the starting alignment is zero");
			return false;
		}

		if ((fs.getFileStatus(file).getLen() <= fs.getFileStatus(file)
				.getBlockSize())
				&& (fileEnd == 0)) {
			key.set(2);
			record.set(new Text("NO INTERVAL"));
			called = true;
			return true;
		}

		initialIndex = r.getReferenceIndex();
		firstrecord = r;

		int initialAlign = firstrecord.getAlignmentStart();

		IndelMapReduce.llimit = initialAlign - IndelRealigner.getMaxIsize();
		IndelMapReduce.lactual = initialAlign;
		RecalMapReduce.lactual = initialAlign;
		if (IndelMapReduce.llimit < 0)
			IndelMapReduce.llimit = 0;
		IndelMapReduce.lcontig = new String(firstrecord.getReferenceName());
		RecalMapReduce.lcontig = new String(firstrecord.getReferenceName());

		try {
			bci.seek(virtualEnd);
		} catch (IOException e) {
			System.out.println(e.getMessage());
                        System.exit(-1);
		}
		pblockEnd = blockEnd = virtualEnd;
		r = codec.decode();
		if (r == null) {
			lastSplit = true;

			blockEnd = idx.prevAlignment(fileEnd - 1);
			bci.seek(blockEnd);

			r = codec.decode();
		}

		if (r.getAlignmentStart() == 0) {
                        // Skip all the unaligned reads
			while (r != null && r.getAlignmentStart() == 0) {
				long end = blockEnd >>> 16;
				if (end == 0) {
					key.set(2);
					record.set(new Text("NO INTERVAL"));
					called = true;
					return true;
				}
				blockEnd = idx.prevAlignment(end - 1);
				bci.seek(blockEnd);
				r = codec.decode();
			}
			do {
				pblockEnd = blockEnd;
				blockEnd = bci.getFilePointer();
			} while (((r = codec.decode()) != null)
					            && (r.getAlignmentStart() != 0));
			bci.seek(pblockEnd);
			r = codec.decode();

		} else if (lastSplit) {
			do {
				pblockEnd = blockEnd;
				blockEnd = bci.getFilePointer();
			} while (((r = codec.decode()) != null)
					&& (r.getAlignmentStart() != 0));
			bci.seek(pblockEnd);
			r = codec.decode();
		}

		lastrecord = r;

		finalIndex = lastrecord.getReferenceIndex();
		int finalAlign = lastrecord.getAlignmentStart();

		if (!lastSplit)
			finalAlign = finalAlign - 1;

		int maxlimit = (lastrecord.getHeader().getSequence(lastrecord
				.getReferenceName())).getSequenceLength();

		IndelMapReduce.hlimit = finalAlign + IndelRealigner.getMaxIsize();
		IndelMapReduce.hactual = RecalMapReduce.hactual = finalAlign;
		if (IndelMapReduce.hlimit > maxlimit)
			IndelMapReduce.hlimit = maxlimit;
		IndelMapReduce.hcontig = RecalMapReduce.hcontig = new String(lastrecord.getReferenceName());

		boolean intervalsFile = false;
		StringBuilder intervals = new StringBuilder();
		realign_intervals = new StringBuilder();
                // If true, the split has multiple contigs
		if (initialIndex != finalIndex) {
			intervalsFile = true;
			StringBuilder b;

			boolean changeit = false;
			while (initialIndex != finalIndex) {

				if (r == lastrecord) {
					changeit = true;
				}

				while (r != null && r.getReferenceIndex() == finalIndex) {
					long end = blockEnd >>> 16;
					if (end == 0) {
						key.set(2);
						record.set(new Text("NO INTERVAL"));
						called = true;
						return true;
					}
					blockEnd = idx.prevAlignment(end - 1);
					bci.seek(blockEnd);
					r = codec.decode();
				}
				do {
					pblockEnd = blockEnd;
					blockEnd = bci.getFilePointer();
				} while (((r = codec.decode()) != null)
						&& (r.getReferenceIndex() != finalIndex));

				if (changeit) {
					StringBuilder bl = new StringBuilder();
					bl.append(r.getReferenceName());
					bl.append(":");
					bl.append(r.getAlignmentStart());
					bl.append("-");
					bl.append(IndelMapReduce.hlimit);
					bl.append("\n");
					realign_intervals.insert(0, bl.toString());
				}

				b = new StringBuilder();
				b.append(r.getReferenceName());
				b.append(":");
				b.append(r.getAlignmentStart());
				b.append("-");
				b.append(finalAlign);
				b.append("\n");

				intervals.insert(0, b.toString());

				if (!changeit)
					realign_intervals.insert(0, b.toString());

				if (changeit)
					changeit = false;

				bci.seek(pblockEnd);
				r = codec.decode();
				finalAlign = r.getAlignmentStart();
				finalIndex = r.getReferenceIndex();
				blockEnd = pblockEnd;
			}
			if (initialIndex == finalIndex) {
				b = new StringBuilder();
				b.append(firstrecord.getReferenceName());
				b.append(":");
				b.append(initialAlign);
				b.append("-");
				b.append(finalAlign);
				b.append("\n");
				intervals.insert(0, b.toString());

				StringBuilder bl = new StringBuilder();
				bl.append(firstrecord.getReferenceName());
				bl.append(":");
				bl.append(IndelMapReduce.llimit);
				bl.append("-");
				bl.append(finalAlign);
				bl.append("\n");
				realign_intervals.insert(0, bl.toString());
			}
		}
		if (initialIndex == finalIndex) {
			if (!intervalsFile) {
				key.set(0);
				record.set(new Text(firstrecord.getReferenceName() + ":"
						+ (new Integer(initialAlign)).toString() + "-"
						+ (new Integer(finalAlign)).toString()));
			} else {
				final File f = File.createTempFile(attemptID, ".intervals",
						new File(jobDir));
				FileWriter fstr = new FileWriter(f.toString(), true);
				BufferedWriter bw = new BufferedWriter(fstr);

				bw.append(intervals.toString());
				bw.close();

				key.set(1);
				record.set(new Text(f.getAbsolutePath()));
			}

			called = true;
			return true;
		}
		return false;
	}
}
