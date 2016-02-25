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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.FileStatus;
import org.broadinstitute.sting.utils.QualityUtils;
import org.apache.hadoop.conf.Configuration;

public class RecalCovMapReduce extends Configured implements Tool {

	private static int MAX_REASONABLE_Q_SCORE = 40; // default

	@SuppressWarnings("unchecked")
	public static Mapper.Context RCMContext;

	public static class RecalCovMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void setup(Context context) throws IOException {
		}

		public void map(LongWritable key, Text value, Context context) {
			String location = null;

			RCMContext = context;

                        Configuration conf = context.getConfiguration();

                        CommandLineGATK.runningOnHadoop = 
                                                        conf.getBoolean("gatk.hadoop", false); 
                        GATKJobClient.is_azure =
                                               conf.getBoolean("gatk.hadoop.isazure", true); 

			if (key.get() == 1) {
				location = value.toString();
			} else if (key.get() != 2) {
				location = value.toString();
			}

			try {
				JobConf job = new JobConf(conf);

				Path input = ((FileVirtualSplit) (context.getInputSplit()))
						                                        .getPath();

				FileSystem srcFs = input.getFileSystem(context
						                        .getConfiguration());

                                Path indexPath = new Path(input.getParent() + Path.SEPARATOR + "_" +
                                                                                input.getName() + ".bai");

				GATKJobClient.bamIndexFile = new String(job.getJobLocalDir()
						+ Path.SEPARATOR
						+ context.getTaskAttemptID().toString() + ".bam.bai");
				srcFs.copyToLocalFile(false, indexPath, new Path(
						                    GATKJobClient.bamIndexFile));

				String CovaOutFile = job.getJobLocalDir() + Path.SEPARATOR
						+ context.getTaskAttemptID().toString() + "_recal.csv";

				/*
				 * Support for -nt option. Has to modify GATK to work with -nt
				 * and HDFS input
				 */

				// ClusterStatus status = (new
				//                          JobClient(job)).getClusterStatus();
				// int nthreads = Math.round((status.getMaxMapTasks() - status
				//                                            .getMapTasks())
				//                                                      / status.getTaskTrackers());
				// if (nthreads < 2)
				//     nthreads = 1;

				ArrayList<String> list = new ArrayList<String>();

				list.add("-T");
				list.add("CountCovariates");
				list.add("-I");
				list.add(input.toString());
				list.add("-R");
				list.add("ref.fa");
				list.add("-knownSites");
				list.add("ref.vcf");
				list.add("-cov");
				list.add("ReadGroupCovariate");
				list.add("-cov");
				list.add("QualityScoreCovariate");
				list.add("-cov");
				list.add("CycleCovariate");
				list.add("-cov");
				list.add("DinucCovariate");
				list.add("-recalFile");
				list.add(CovaOutFile);
				list.add("-dP");
				list.add("illumina"); // should be an argument

				if (location != null) {
					list.add("-L");
					list.add(location);
				}

				String cmdLineArg[] = new String[list.size()];
				list.toArray(cmdLineArg);

				CommandLineGATK instance = new CommandLineGATK();
				CommandLineProgram.start(instance, cmdLineArg);
				if (CommandLineProgram.result != 0) {
					System.out.println("CountCovariate failed: " + CommandLineProgram.result);
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

	public static class RecalCovCombiner extends
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context ctx)
				throws IOException, InterruptedException {
			int numObservations = 0;
			int numMisMatches = 0;
			StringBuilder sb = new StringBuilder(50);
			for (Text val : values) {
				String[] fields = val.toString().split(",");
				numObservations += Integer.parseInt(fields[0]);
				numMisMatches += Integer.parseInt(fields[1]);
			}
			sb.append(numObservations).append(",").append(numMisMatches);
			Text value = new Text(sb.toString());
			ctx.write(key, value);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
		}
	}

	public static class RecalCovReducer extends
			Reducer<Text, Text, NullWritable, Text> {
		private boolean header_written = false;

		@Override
		public void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, NullWritable, Text>.Context ctx)
				throws IOException, InterruptedException {
			int numObservations = 0;
			int numMisMatches = 0;
			int empQuality = 0;
			StringBuilder sb = new StringBuilder(50);
			for (Text val : values) {
				String[] fields = val.toString().split(",");
				numObservations += Integer.parseInt(fields[0]);
				numMisMatches += Integer.parseInt(fields[1]);
			}

                        // Assume default SMOOTHING of 0

                        /*empQuality = (int) Math.round(-10.0
                                                * Math.log10((double) numMisMatches
                                                                    / (double) numObservations));*/

                        empQuality = (int) QualityUtils.probToQual( 1.0 - (double) numMisMatches  / (double) numObservations );

			if (empQuality > MAX_REASONABLE_Q_SCORE || empQuality < 0)
				empQuality = MAX_REASONABLE_Q_SCORE;
			if (empQuality == 0)
				empQuality = 1;

			sb.append(key).append(numObservations).append(",").append(
					numMisMatches).append(",").append(empQuality);
			Text value = new Text(sb.toString());
			if (!header_written) {
				header_written = true;
				ctx.write(
					NullWritable.get(),
					new Text(
					"ReadGroup,QualityScore,Cycle,Dinuc,nObservations,nMismatches,Qempirical"));
			}
			ctx.write(NullWritable.get(), value);
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
