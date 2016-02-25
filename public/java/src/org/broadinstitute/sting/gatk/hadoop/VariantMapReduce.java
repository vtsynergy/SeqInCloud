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

import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.broadinstitute.sting.gatk.CommandLineGATK;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.broadinstitute.sting.commandline.CommandLineProgram;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configuration;

public class VariantMapReduce extends Configured implements Tool {

	static ArrayList<String> vcfFiles = new ArrayList<String>();

	public static class VariantReducer extends
			Reducer<LongWritable, Text, NullWritable, NullWritable> {
		@Override
		public void reduce(
				LongWritable key,
				Iterable<Text> values,
				Reducer<LongWritable, Text, NullWritable, NullWritable>.Context ctx)
				throws IOException, InterruptedException {
			for (Text path : values) {
				vcfFiles.add(path.toString());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

                        Configuration conf = context.getConfiguration();

                        CommandLineGATK.runningOnHadoop = 
                                                        conf.getBoolean("gatk.hadoop", false); 
                        GATKJobClient.is_azure =
                                               conf.getBoolean("gatk.hadoop.isazure", true); 
			// Do CombineVariant here
			Path p = FileOutputFormat.getOutputPath(context);
			Path mergedVCF = new Path((p.makeQualified(p.getFileSystem(context
					.getConfiguration()))).toString()
					+ Path.SEPARATOR + "GATKAnalysisVariants.vcf");

			File Curdir = new File(".");
			String parentPath = Curdir.getAbsolutePath();
			StringBuilder b = new StringBuilder(parentPath);
			b.replace(parentPath.lastIndexOf(File.separator), parentPath
					.lastIndexOf(File.separator) + 2, File.separator);
			parentPath = b.toString();

			// Enabling localFS output
			// String localmergedVCF =
                                        // parentPath.concat("GATKAnalysisVariants.vcf");

			ArrayList<String> list = new ArrayList<String>();

			list.add("-T");
			list.add("CombineVariants");
			list.add("-R");
			list.add("ref.fa");
			list.add("-o");
			list.add(mergedVCF.toString());
			// list.add(localmergedVCF); // For Local FS
			// list.add("-genotypeMergeOptions");
			// list.add("UNIQUIFY");
			list.add("--assumeIdenticalSamples");

			FileSystem srcFs = null; // should correspond to input FS

			for (String file : vcfFiles) {
				p = new Path(file);
				String localFile = new String(p.getName());
				if (srcFs == null)
					srcFs = p.getFileSystem(conf);
				srcFs.copyToLocalFile(false, p, new Path(parentPath,
				                				localFile));
				list.add("--variant");
				list.add(localFile);
			}

			String cmdLineArg[] = new String[list.size()];
			list.toArray(cmdLineArg);

			try {
				CommandLineGATK instance = new CommandLineGATK();
				CommandLineProgram.start(instance, cmdLineArg);
				if (CommandLineProgram.result != 0) {
			            System.out.println("Combine Variant failed \t"
									    + CommandLineProgram.result);
                                    System.exit(-1);
				}
			} catch (Exception E) {
				System.out.println("Exception Raised: " + E.getMessage());
                                System.exit(-1);
			}

			// For Local FS
			// srcFs = p.getFileSystem(conf);
			// srcFs.copyFromLocalFile(false, true, new Path(localmergedVCF),
			//                          mergedVCF);
		}
	}

	public int run(String[] args) throws Exception {
		return 0;
	}
}
