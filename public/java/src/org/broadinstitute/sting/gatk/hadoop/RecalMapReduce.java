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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.broadinstitute.sting.gatk.CommandLineGATK;
import org.broadinstitute.sting.gatk.hadoop.hbamsrc.SAMRecordWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapred.ClusterStatus;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.conf.Configuration;

import net.sf.samtools.BAMIndexer;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import org.apache.hadoop.fs.FileStatus;

public class RecalMapReduce extends Configured implements Tool {
	
        public static Mapper.Context RMContext;
        public static boolean isVariant;
        public static int lactual = -1, hactual = Integer.MAX_VALUE;
	public static String lcontig, hcontig;

	public static class RecalMapper extends
			Mapper<LongWritable, Text, LongWritable, SAMRecordWritable> {

                private Configuration conf;

		@Override
		public void setup(Context context) throws IOException {
		}

		public void map(LongWritable key, Text value, Context context) {
			String location = null;
			Path indexPath;
			FileStatus[] content;
                        FileSystem srcFs;
                        boolean isRecab, isFVariant;

			String recalOutFile = null;
                        RMContext = context;
			LongWritable k = new LongWritable();
			SAMRecordWritable v = new SAMRecordWritable();
		        ArrayList<String> list = new ArrayList<String>();
			String[] cmdLineArg;
                        CommandLineGATK instance;

			if (key.get() == 1) {
				location = value.toString();
			} else if (key.get() != 2) {
				location = value.toString();
			}

			try {
                                conf = context.getConfiguration();
				JobConf job = new JobConf(conf);
                                CommandLineGATK.runningOnHadoop = 
                                                                 conf.getBoolean("gatk.hadoop", false); 
                                GATKJobClient.is_azure =
                                                        conf.getBoolean("gatk.hadoop.isazure", true); 
                                isVariant = conf.getBoolean("gatk.hadoop.variant", false);
                                isRecab = conf.getBoolean("gatk.hadoop.recab", false);
                                isFVariant = conf.getBoolean("gatk.hadoop.fvariant", false);

                                String outputDir = conf.get("gatk.hadoop.outputpath");

				Path input = ((FileVirtualSplit) (context.getInputSplit()))
						.getPath();
				long start = ((FileVirtualSplit) (context.getInputSplit()))
						.getStart();
				int index = input.getName().indexOf("-");
				String identifier = input.getName().substring(0, index);

				srcFs = input.getFileSystem(conf);

                                indexPath = new Path(input.getParent() + Path.SEPARATOR + "_" +
                                                                                        input.getName() + ".bai");

				GATKJobClient.bamIndexFile = new String(job.getJobLocalDir()
						+ Path.SEPARATOR
						+ context.getTaskAttemptID().toString() + ".bam.bai");
				srcFs.copyToLocalFile(false, indexPath, new Path(
						            GATKJobClient.bamIndexFile));

                            if (isRecab) {
				Path CovaOutPath = new Path(outputDir + Path.SEPARATOR + "CovariateOut");
				srcFs = CovaOutPath.getFileSystem(conf);
				content = srcFs
						.listStatus(CovaOutPath);
				String CovaOutFile = job.getJobLocalDir() + Path.SEPARATOR
						    + context.getTaskAttemptID().toString() + ".csv";

				for (int i = 0; i < content.length; i++) {
					if ((content[i].getPath().getName()).startsWith("part")) {
						CovaOutPath = content[i].getPath();
						break;
					}
				}
				srcFs
				    .copyToLocalFile(false, CovaOutPath, new Path(
					                                    CovaOutFile));
				// For Local FS
				recalOutFile = job.getJobLocalDir() + Path.SEPARATOR +
				                  context.getTaskAttemptID().toString() + "_recal.bam";

                                /*
				// For HDFS
				Path recalOutPath = new Path(outputDir + Path.SEPARATOR + "IRecalOut");
				srcFs = recalOutPath.getFileSystem(conf);
				srcFs.mkdirs(recalOutPath);
				recalOutPath = recalOutPath.makeQualified(srcFs);
				recalOutFile = recalOutPath + Path.SEPARATOR + identifier + "_"
						+ start + "_recal.bam";
                                */

                                list.clear();
				list.add("-T");
				list.add("TableRecalibration");
				list.add("-I");
				list.add(input.toString());
				list.add("-R");
				list.add("ref.fa");
				list.add("-o");
				list.add(recalOutFile);
				list.add("-recalFile");
				list.add(CovaOutFile);
				list.add("-baq");
				list.add("RECALCULATE");
				list.add("--doNotWriteOriginalQuals");
				list.add("-dP");
				list.add("illumina");

				if (location != null) {
					list.add("-L");
					list.add(location);
				}

			        cmdLineArg = new String[list.size()];
				list.toArray(cmdLineArg);

				instance = new CommandLineGATK();
				CommandLineProgram.start(instance, cmdLineArg);
				if (CommandLineProgram.result != 0) {
				    System.out.println("Table Recalibration Failed: "
									+ CommandLineProgram.result);
                                    System.exit(-1);
                                }
                            }

                            if (isVariant) {
                                String tmpVariantOutFile = null, tmpIVariantOutFile = null;
                                int nthreads = 1;
                                if (isRecab) {
                                    // For Local
                                    if (conf.getInt("gatk.hadoop.nthreads", 0) == 0) {
                                        ClusterStatus status = (new
                                                            JobClient(job)).getClusterStatus();
                                        nthreads = (int)
                                                Math.ceil((double)(status.getMaxMapTasks() -
                                                        status.getMapTasks()) / (double)status.getTaskTrackers());
                                        if (nthreads < 2)
                                            nthreads = 1;
                                    } else {
                                        nthreads = conf.getInt("gatk.hadoop.nthreads", 0);
                                    }
                                    // For Local FS
                                    GATKJobClient.bamIndexFile = recalOutFile.replace(".bam", ".bai");

                                    /*
                                    // For HDFS
                                    SAMFileReader fileReader;
                                    Path recaloutput = new Path(recalOutFile);
                                    FSDataInputStream instream = recaloutput.getFileSystem(conf.open(recaloutput);
                                    fileReader = new SAMFileReader(instream);
                                    fileReader.setValidationStringency(ValidationStringency.SILENT);
                                    fileReader.enableFileSource(true);

                                    GATKJobClient.bamIndexFile = job.getJobLocalDir()
                                                    + Path.SEPARATOR
                                                    + context.getTaskAttemptID().toString() + "_recal.bai";

                                    BAMIndexer indexer = new BAMIndexer(new File(
                                                    GATKJobClient.bamIndexFile), fileReader.getFileHeader());

                                    for (SAMRecord rec : fileReader) {
                                            indexer.processAlignment(rec);
                                    }

                                    indexer.finish();
                                    fileReader.close();
                                    instream.close();
                                    */
                                }

                                // Variant Calling for SNP or Both

                                if (isFVariant) {
				    tmpVariantOutFile = job.getJobLocalDir() + Path.SEPARATOR +
				                        context.getTaskAttemptID().toString() + "_snp.vcf";
                                }

				Path variantPath = new Path(outputDir + Path.SEPARATOR + "VariantOut");
				srcFs = variantPath.getFileSystem(conf);
				srcFs.mkdirs(variantPath);
				variantPath = variantPath.makeQualified(srcFs);
                                String VariantOutFile;

                                if (conf.getBoolean("gatk.hadoop.xvariant", false)) {
				    VariantOutFile = variantPath + Path.SEPARATOR
						                + identifier + "_" + start + "_snp.vcf";
                                } else {
				    VariantOutFile = variantPath + Path.SEPARATOR
						                + identifier + "_" + start + ".vcf";
                                }

				list.clear();
				list.add("-T");
				list.add("UnifiedGenotyper");
				list.add("-I");
                                if (isRecab) {
				    list.add(recalOutFile);
				    list.add("-nt");
				    list.add(String.valueOf(nthreads));
                                } else {
				    list.add(input.toString());
                                }
				list.add("-R");
				list.add("ref.fa");
				list.add("-o");
                                if (isFVariant)
				    list.add(tmpVariantOutFile);
                                else
				    list.add(VariantOutFile);
				list.add("-dcov");
				list.add("1000");
                                list.add("-A"); list.add("AlleleBalance");
                                list.add("-A"); list.add("DepthOfCoverage");
                                list.add("-A"); list.add("MappingQualityZero");
                                list.add("-baq");
                                list.add("CALCULATE_AS_NECESSARY");
                                list.add("-stand_call_conf"); list.add("30.0");
                                list.add("-stand_emit_conf"); list.add("10.0");
                                if (!conf.getBoolean("gatk.hadoop.xvariant", false)) {
                                    list.add("-glm");
                                    list.add("BOTH");
                                }

				if (location != null) {
					list.add("-L");
					list.add(location);
				}

				cmdLineArg = new String[list.size()];
				list.toArray(cmdLineArg);

				instance = new CommandLineGATK();
				CommandLineProgram.start(instance, cmdLineArg);
				if (CommandLineProgram.result != 0) {
					System.out.println("Unified Genotyper Failed: "
							            + CommandLineProgram.result);
                                        System.exit(-1);
                                }
                               
                                if (isFVariant) {
                                    // Filter SNP variants

                                    list.clear();
                                    list.add("-T");
                                    list.add("VariantFiltration");
                                    list.add("-o");
                                    list.add(VariantOutFile);
                                    list.add("-R");
                                    list.add("ref.fa");
                                    list.add("-V");
                                    list.add(tmpVariantOutFile);
                                    list.add("-window");
                                    list.add("10");
                                    list.add("-filter");
                                    list.add("(AB ?: 0) > 0.75 || QUAL < 50.0 || DP > 360 || SB > -0.1 || MQ0>=4");
                                    list.add("-filterName");
                                    list.add("filter");
                                    if (location != null) {
                                            list.add("-L");
                                            list.add(location);
                                    }

                                    cmdLineArg = new String[list.size()];
                                    list.toArray(cmdLineArg);

                                    instance = new CommandLineGATK();
                                    CommandLineProgram.start(instance, cmdLineArg);
                                    if (CommandLineProgram.result != 0) {
                                            System.out.println("Variant Filtration failed: "
                                                                                    + CommandLineProgram.result);
                                            System.exit(-1);
                                    }
                                }

                                // Variant Calling for INDEL
                                if (conf.getBoolean("gatk.hadoop.xvariant", false)) {
                                    if (isFVariant) {
                                        tmpIVariantOutFile = job.getJobLocalDir() + Path.SEPARATOR +
                                                      context.getTaskAttemptID().toString() + "_indel.vcf";
                                    }

                                    Path IvariantPath = new Path(outputDir + Path.SEPARATOR + "IVariantOut");
                                    srcFs = IvariantPath.getFileSystem(conf);
                                    srcFs.mkdirs(IvariantPath);
                                    IvariantPath = IvariantPath.makeQualified(srcFs);
                                    String IVariantOutFile = IvariantPath + Path.SEPARATOR
                                                                + identifier + "_" + start + "_indel.vcf";

                                    list.clear();
                                    list.add("-T");
                                    list.add("UnifiedGenotyper");
				    list.add("-I");
                                    if (isRecab) {
				        list.add(recalOutFile);
				        list.add("-nt");
				        list.add(String.valueOf(nthreads));
                                    } else {
				        list.add(input.toString());
                                    }
                                    list.add("-R");
                                    list.add("ref.fa");
                                    list.add("-o");
                                    if (isFVariant)
                                        list.add(tmpIVariantOutFile);
                                    else
                                        list.add(IVariantOutFile);
                                    list.add("-dcov");
                                    list.add("1000");
                                    list.add("-A"); list.add("AlleleBalance");
                                    list.add("-A"); list.add("DepthOfCoverage");
                                    list.add("-A"); list.add("MappingQualityZero");
                                    list.add("-baq");
                                    list.add("CALCULATE_AS_NECESSARY");
                                    list.add("-stand_call_conf"); list.add("30.0");
                                    list.add("-stand_emit_conf"); list.add("10.0");
                                    list.add("-glm");
                                    list.add("INDEL");

                                    if (location != null) {
                                            list.add("-L");
                                            list.add(location);
                                    }

                                    cmdLineArg = new String[list.size()];
                                    list.toArray(cmdLineArg);

                                    instance = new CommandLineGATK();
                                    CommandLineProgram.start(instance, cmdLineArg);
                                    if (CommandLineProgram.result != 0) {
                                            System.out.println("Unified Genotyper for INDEL failed: "
                                                                            + CommandLineProgram.result);
                                            System.exit(-1);
                                    }

                                    if (isFVariant) {
                                        // Filter INDEL variants

                                        list.clear();
                                        list.add("-T");
                                        list.add("VariantFiltration");
                                        list.add("-o");
                                        list.add(IVariantOutFile);
                                        list.add("-R");
                                        list.add("ref.fa");
                                        list.add("-V");
                                        list.add(tmpIVariantOutFile);
                                        list.add("-window");
                                        list.add("10");
                                        list.add("-filter");
                                        list.add("(AB ?: 0) > 0.75 || QUAL < 50.0 || DP > 360 || SB > -0.1 || MQ0>=4");
                                        list.add("-filterName");
                                        list.add("filter");
                                        if (location != null) {
                                                list.add("-L");
                                                list.add(location);
                                        }

                                        cmdLineArg = new String[list.size()];
                                        list.toArray(cmdLineArg);

                                        instance = new CommandLineGATK();
                                        CommandLineProgram.start(instance, cmdLineArg);
                                        if (CommandLineProgram.result != 0) {
                                                System.out.println("Variant Filtration for INDEL failed: "
                                                                                        + CommandLineProgram.result);
                                                System.exit(-1);
                                        }
                                    }
                                }
                            }
                            if (isRecab && isVariant) {
				// For Local FS
                                SAMFileReader fileReader;
				File bamFile = new File(recalOutFile);
				fileReader = new SAMFileReader(bamFile);

                                /*
				// For HDFS
				recaloutput = new Path(recalOutFile);
				instream = recaloutput
						.getFileSystem(conf).open(
								recaloutput);
				fileReader = new SAMFileReader(instream);
                                */

				SAMRecordIterator SAMIterator = fileReader.iterator();

				fileReader.setValidationStringency(ValidationStringency.SILENT);

				while (SAMIterator.hasNext()) {
					SAMRecord rec = SAMIterator.next();

					if (rec.getReferenceName().equals("*"))
						k.set(Long.MAX_VALUE);
					else
						k.set((long) rec.getReferenceIndex() << 32
								| rec.getAlignmentStart() - 1);
					v.set(rec);
					context.write(k, v);
				}
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

	public int run(String[] args) throws Exception {
		return 0;
	}
}
