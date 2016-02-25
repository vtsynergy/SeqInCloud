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

import org.broad.tribble.TribbleException;
import org.broadinstitute.sting.commandline.CommandLineProgram;
import org.broadinstitute.sting.utils.exceptions.UserException;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.broadinstitute.sting.gatk.CommandLineGATK;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.util.Tool;
import org.broadinstitute.sting.gatk.hadoop.hbamsrc.SAMRecordWritable;
import org.apache.hadoop.conf.Configuration;

public class IndelMapReduce extends Configured implements Tool {

	@SuppressWarnings("unchecked")
	public static Mapper.Context IMContext;

	public static String lcontig, hcontig;
	public static int llimit, lactual = -1, hlimit,
			        hactual = Integer.MAX_VALUE;

	public static class IndelMapper extends
			Mapper<LongWritable, Text, LongWritable, SAMRecordWritable> {

		@Override
		public void setup(Context context) throws IOException {
		}

	 	public void map(LongWritable key, Text value, Context context) {
			boolean intervalsFile = false;
			String location = null;
		 	IMContext = context;

                        Configuration conf = context.getConfiguration();

                        CommandLineGATK.runningOnHadoop = 
                                                        conf.getBoolean("gatk.hadoop", false); 
                        GATKJobClient.is_azure =
                                               conf.getBoolean("gatk.hadoop.isazure", true); 

			if (key.get() == 1) {
				intervalsFile = true;
				location = value.toString();
			} else if (key.get() != 2) {
				location = value.toString();
			}

			try {
				JobConf job = new JobConf(conf);

				Path input = ((FileVirtualSplit) (context.getInputSplit())).getPath();

                                FileSystem srcFs = input.getFileSystem(conf);

                                int index = input.getName().indexOf("-");
                                String identifier = input.getName().substring(0, index);

                                Path indexPath = new Path(input.getParent() + Path.SEPARATOR + "_" +
                                                                                    input.getName() + ".bai");

                                GATKJobClient.bamIndexFile = new String(job.getJobLocalDir()
                                                                                    + Path.SEPARATOR
                                                                                    + context.getTaskAttemptID().toString() 
                                                                                    + ".bam.bai");
                                srcFs.copyToLocalFile(false, indexPath, new Path(
                                                                                GATKJobClient.bamIndexFile));

				String realignOutFile = job.getJobLocalDir() + Path.SEPARATOR
						                    + context.getTaskAttemptID().toString()
						                                            + "_realign.intervals";
				ArrayList<String> list = new ArrayList<String>();

				list.add("-T");
				list.add("RealignerTargetCreator");
				list.add("-I");
				list.add(input.toString());
				list.add("-R");
				list.add("ref.fa");
				list.add("-o");
				list.add(realignOutFile);

				if (location != null) {
					list.add("-L");
					list.add(location);
				}

				String cmdLineArg[] = new String[list.size()];
				list.toArray(cmdLineArg);

				CommandLineGATK instance = new CommandLineGATK();
				CommandLineProgram.start(instance, cmdLineArg);
				if (CommandLineProgram.result != 0) {
                                        System.out.println("RealignerTargetCreator failed: " + CommandLineProgram.result);
					System.exit(-1);
                                }

				if (intervalsFile) {
					File f = new File(location);
					f.delete();
					f = File.createTempFile(context.getTaskAttemptID()
									.toString()
									+ "-", ".intervals", new File(job
									.getJobLocalDir()));
					FileWriter fstr = new FileWriter(f.toString(), true);
					BufferedWriter bw = new BufferedWriter(fstr);

					bw.append(LociRecordReader.realign_intervals.toString());
					bw.close();
					location = f.getAbsolutePath();
				} else {
					if (location != null)
						location = lcontig + ":" + llimit + "-" + hlimit;
				}

				String indelOutFile = job.getJobLocalDir() + Path.SEPARATOR
						+ context.getTaskAttemptID().toString()
						+ "_realign.bam";

				list.clear();
				list.add("-T");
				list.add("IndelRealigner");
				list.add("-I");
				list.add(input.toString());
				list.add("-R");
				list.add("ref.fa");
				list.add("-o");
				list.add(indelOutFile);
				list.add("-targetIntervals");
				list.add(realignOutFile);
                                //list.add("-LOD");
                                //list.add("5");

				if (location != null) {
					list.add("-L");
					list.add(location);
				}

				String cmdLineArg1[] = new String[list.size()];
				list.toArray(cmdLineArg1);

				CommandLineGATK instance1 = new CommandLineGATK();
				CommandLineProgram.start(instance1, cmdLineArg1);
				if (CommandLineProgram.result != 0) {
                                        System.out.println("IndelRealigner failed: " + CommandLineProgram.result);
					System.exit(-1);
                                }

			} catch (UserException e) {
				System.out.println(e.getMessage());
				System.exit(-1);
			} catch (TribbleException e) {
				System.out.println(e.getMessage());
				System.exit(-1);
			} catch (net.sf.samtools.SAMException e) {
				System.out.println(e.getMessage());
				System.exit(-1);
			} catch (Throwable t) {
				System.out.println(t.getMessage());
				System.exit(-1);
			}
		}
	}

	public static class SortReducer
			extends
			Reducer<LongWritable, SAMRecordWritable, NullWritable, SAMRecordWritable> {
		@Override
		public void reduce(
				LongWritable ignored,
				Iterable<SAMRecordWritable> records,
				Reducer<LongWritable, SAMRecordWritable, NullWritable, SAMRecordWritable>.Context ctx)
				throws IOException, InterruptedException {
			for (SAMRecordWritable rec : records) {
				ctx.write(NullWritable.get(), rec);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
		}
	}

	public int run(String[] args) throws Exception {
		return 0;
	}
}
