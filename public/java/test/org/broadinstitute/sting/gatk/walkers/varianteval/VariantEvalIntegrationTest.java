/*
 * Copyright (c) 2012, The Broad Institute
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
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.broadinstitute.sting.gatk.walkers.varianteval;

import org.broadinstitute.sting.WalkerTest;
import org.broadinstitute.sting.utils.exceptions.UserException;
import org.testng.annotations.Test;

import java.util.Arrays;

public class VariantEvalIntegrationTest extends WalkerTest {
    private static String variantEvalTestDataRoot = validationDataLocation + "VariantEval";
    private static String fundamentalTestVCF = variantEvalTestDataRoot + "/" + "FundamentalsTest.annotated.db.subset.snps_and_indels.vcf";
    private static String fundamentalTestSNPsVCF = variantEvalTestDataRoot + "/" + "FundamentalsTest.annotated.db.subset.final.vcf";
    private static String fundamentalTestSNPsSplit1of2VCF = variantEvalTestDataRoot + "/" + "FundamentalsTest.annotated.db.subset.final.split_1_of_2.vcf";
    private static String fundamentalTestSNPsSplit2of2VCF = variantEvalTestDataRoot + "/" + "FundamentalsTest.annotated.db.subset.final.split_2_of_2.vcf";
    private static String fundamentalTestSNPsOneSampleVCF = variantEvalTestDataRoot + "/" + "FundamentalsTest.annotated.db.subset.final.NA12045.vcf";

    private static String cmdRoot = "-T VariantEval" +
            " -R " + b36KGReference;

    @Test
    public void testFunctionClassWithSnpeff() {
        WalkerTestSpec spec = new WalkerTestSpec(
                                buildCommandLine(
                                        "-T VariantEval",
                                        "-R " + b37KGReference,
                                        "--dbsnp " + b37dbSNP132,
                                        "--eval " + validationDataLocation + "snpEff2.0.5.AFR.unfiltered.VariantAnnotator.output.vcf",
                                        "-noEV",
                                        "-EV TiTvVariantEvaluator",
                                        "-noST",
                                        "-ST FunctionalClass",
                                        "-L " + validationDataLocation + "snpEff2.0.5.AFR.unfiltered.VariantAnnotator.output.vcf",
                                        "-o %s"
                                ),
                                1,
                                Arrays.asList("e87932ffa1d310cecee49e7829a0f056")
                              );
        executeTest("testFunctionClassWithSnpeff", spec);
    }

    @Test
    public void testStratifySamplesAndExcludeMonomorphicSites() {
        WalkerTestSpec spec = new WalkerTestSpec(
                                buildCommandLine(
                                        "-T VariantEval",
                                        "-R " + b37KGReference,
                                        "--dbsnp " + b37dbSNP132,
                                        "--eval " + variantEvalTestDataRoot + "/CEU.trio.callsForVE.vcf",
                                        "-noEV",
                                        "-EV TiTvVariantEvaluator",
                                        "-ST Sample",
                                        "-L " + variantEvalTestDataRoot + "/CEU.trio.callsForVE.vcf",
                                        "-o %s"
                                ),
                                1,
                                Arrays.asList("8279ee42a6785f9c2b3dda8d82674e00")
                              );
        executeTest("testStratifySamplesAndExcludeMonomorphicSites", spec);
    }

    @Test
    public void testFundamentalsCountVariantsSNPsAndIndels() {
        WalkerTestSpec spec = new WalkerTestSpec(
                                buildCommandLine(
                                        "-T VariantEval",
                                        "-R " + b37KGReference,
                                        "--dbsnp " + b37dbSNP132,
                                        "--eval " + fundamentalTestVCF,
                                        "-noEV",
                                        "-EV CountVariants",
                                        "-noST",
                                        "-L " + fundamentalTestVCF,
                                        "-o %s"
                                ),
                                1,
                                Arrays.asList("0bac64d5615f901d3005247c6d016549")
                              );
        executeTest("testFundamentalsCountVariantsSNPsandIndels", spec);
    }

    @Test
    public void testFundamentalsCountVariantsSNPsAndIndelsWithNovelty() {
        WalkerTestSpec spec = new WalkerTestSpec(
                buildCommandLine(
                        "-T VariantEval",
                        "-R " + b37KGReference,
                        "--dbsnp " + b37dbSNP132,
                        "--eval " + fundamentalTestVCF,
                        "-noEV",
                        "-EV CountVariants",
                        "-noST",
                        "-ST Novelty",
                        "-L " + fundamentalTestVCF,
                        "-o %s"
                ),
                1,
                Arrays.asList("b84d8b4429116c887ceb5489c8782f00")
        );
        executeTest("testFundamentalsCountVariantsSNPsandIndelsWithNovelty", spec);
    }

    @Test
    public void testFundamentalsCountVariantsSNPsAndIndelsWithNoveltyAndFilter() {
        WalkerTestSpec spec = new WalkerTestSpec(
                buildCommandLine(
                        "-T VariantEval",
                        "-R " + b37KGReference,
                        "--dbsnp " + b37dbSNP132,
                        "--eval " + fundamentalTestVCF,
                        "-noEV",
                        "-EV CountVariants",
                        "-noST",
                        "-ST Novelty",
                        "-ST Filter",
                        "-L " + fundamentalTestVCF,
                        "-o %s"
                ),
                1,
                Arrays.asList("e4f37642d9113a65fbe8bc1d091c206f")
        );
        executeTest("testFundamentalsCountVariantsSNPsandIndelsWithNoveltyAndFilter", spec);
    }

    @Test
    public void testFundamentalsCountVariantsSNPsAndIndelsWithCpG() {
        WalkerTestSpec spec = new WalkerTestSpec(
                buildCommandLine(
                        "-T VariantEval",
                        "-R " + b37KGReference,
                        "--dbsnp " + b37dbSNP132,
                        "--eval " + fundamentalTestVCF,
                        "-noEV",
                        "-EV CountVariants",
                        "-noST",
                        "-ST CpG",
                        "-L " + fundamentalTestVCF,
                        "-o %s"
                ),
                1,
                Arrays.asList("c5412ee824b4815dc8eea62a4c5462ef")
        );
        executeTest("testFundamentalsCountVariantsSNPsandIndelsWithCpG", spec);
    }

    @Test
    public void testFundamentalsCountVariantsSNPsAndIndelsWithFunctionalClasses() {
        WalkerTestSpec spec = new WalkerTestSpec(
                buildCommandLine(
                        "-T VariantEval",
                        "-R " + b37KGReference,
                        "--dbsnp " + b37dbSNP132,
                        "--eval " + fundamentalTestVCF,
                        "-noEV",
                        "-EV CountVariants",
                        "-noST",
                        "-ST FunctionalClass",
                        "-L " + fundamentalTestVCF,
                        "-o %s"
                ),
                1,
                Arrays.asList("1d42e97643afd3e7f5f8c9f6416c5883")
        );
        executeTest("testFundamentalsCountVariantsSNPsandIndelsWithFunctionalClass", spec);
    }

    @Test
    public void testFundamentalsCountVariantsSNPsAndIndelsWithDegeneracy() {
        WalkerTestSpec spec = new WalkerTestSpec(
                buildCommandLine(
                        "-T VariantEval",
                        "-R " + b37KGReference,
                        "--dbsnp " + b37dbSNP132,
                        "--eval " + fundamentalTestVCF,
                        "-noEV",
                        "-EV CountVariants",
                        "-noST",
                        "-ST Degeneracy",
                        "-L " + fundamentalTestVCF,
                        "-o %s"
                ),
                1,
                Arrays.asList("8c2ba70bed2f0fdb0ca371f7038819ef")
        );
        executeTest("testFundamentalsCountVariantsSNPsandIndelsWithDegeneracy", spec);
    }

    @Test
    public void testFundamentalsCountVariantsSNPsAndIndelsWithSample() {
        WalkerTestSpec spec = new WalkerTestSpec(
                buildCommandLine(
                        "-T VariantEval",
                        "-R " + b37KGReference,
                        "--dbsnp " + b37dbSNP132,
                        "--eval " + fundamentalTestVCF,
                        "-noEV",
                        "-EV CountVariants",
                        "-noST",
                        "-ST Sample",
                        "-L " + fundamentalTestVCF,
                        "-o %s"
                ),
                1,
                Arrays.asList("c912b4b0bf1925d042119b301c183b93")
        );
        executeTest("testFundamentalsCountVariantsSNPsandIndelsWithSample", spec);
    }

    @Test
    public void testFundamentalsCountVariantsSNPsAndIndelsWithJexlExpression() {
        WalkerTestSpec spec = new WalkerTestSpec(
                buildCommandLine(
                        "-T VariantEval",
                        "-R " + b37KGReference,
                        "--dbsnp " + b37dbSNP132,
                        "--eval " + fundamentalTestVCF,
                        "-noEV",
                        "-EV CountVariants",
                        "-noST",
                        "-ST JexlExpression",
                        "-select 'DP < 20'",
                        "-selectName DepthSelect",
                        "-L " + fundamentalTestVCF,
                        "-o %s"
                ),
                1,
                Arrays.asList("dea3d2cc53265ff8ed2f0030c40f3747")
        );
        executeTest("testFundamentalsCountVariantsSNPsandIndelsWithJexlExpression", spec);
    }

    @Test
    public void testFundamentalsCountVariantsSNPsAndIndelsWithMultipleJexlExpressions() {
        WalkerTestSpec spec = new WalkerTestSpec(
                buildCommandLine(
                        "-T VariantEval",
                        "-R " + b37KGReference,
                        "--dbsnp " + b37dbSNP132,
                        "--eval " + fundamentalTestVCF,
                        "-noEV",
                        "-EV CountVariants",
                        "-noST",
                        "-ST JexlExpression",
                        "-select 'DP < 20'",
                        "-selectName DepthLt20",
                        "-select 'DP > 20'",
                        "-selectName DepthGt20",
                        "-L " + fundamentalTestVCF,
                        "-o %s"
                ),
                1,
                Arrays.asList("dede22b15936c38e29b850c805c7b706")
        );
        executeTest("testFundamentalsCountVariantsSNPsandIndelsWithMultipleJexlExpressions", spec);
    }

    @Test
    public void testFundamentalsCountVariantsNoCompRod() {
        WalkerTestSpec spec = new WalkerTestSpec(
                buildCommandLine(
                        "-T VariantEval",
                        "-R " + b37KGReference,
                        "--eval " + fundamentalTestVCF,
                        "-noEV",
                        "-EV CountVariants",
                        "-noST",
                        "-L " + fundamentalTestVCF,
                        "-o %s"
                ),
                1,
                Arrays.asList("9a94c4c613bf69feb3d9579c353baaf2")
        );
        executeTest("testFundamentalsCountVariantsNoCompRod", spec);
    }

    @Test
    public void testSelect1() {
        String extraArgs = "-L 1:1-10,000,000";
        String tests = cmdRoot +
                " --dbsnp " + b36dbSNP129 +
                " --eval " + validationDataLocation + "yri.trio.gatk_glftrio.intersection.annotated.filtered.chr1.vcf" +
                " --comp:comp_genotypes,VCF3 " + validationDataLocation + "yri.trio.gatk.ug.head.vcf";
        WalkerTestSpec spec = new WalkerTestSpec(withSelect(tests, "DP < 50", "DP50") + " " + extraArgs + " -ST CpG -o %s",
                1, Arrays.asList("8d4530e9cef8531c46bbb693b84d04c7"));
        executeTestParallel("testSelect1", spec);
    }

    @Test
    public void testVEGenotypeConcordance() {
        String vcfFile = "GenotypeConcordanceEval.vcf";

        WalkerTestSpec spec = new WalkerTestSpec(cmdRoot + " -ST CpG --eval:VCF3 " + validationDataLocation + vcfFile + " --comp:VCF3 " + validationDataLocation + "GenotypeConcordanceComp.vcf -noEV -EV GenotypeConcordance -o %s",
                1,
                Arrays.asList("9bbc762f459023af0480774eb2986af4"));
        executeTestParallel("testVEGenotypeConcordance" + vcfFile, spec);
    }

    @Test
    public void testVEMendelianViolationEvaluator() {
        String vcfFile = "/MendelianViolationEval.vcf";
        String pedFile = "/MendelianViolationEval.ped";

        WalkerTestSpec spec = new WalkerTestSpec("-T VariantEval -R "+b37KGReference+" --eval " + variantEvalTestDataRoot + vcfFile + " -ped "+ variantEvalTestDataRoot + pedFile +" -noEV -EV MendelianViolationEvaluator -L 1:10109-10315 -o %s -mvq 0 -noST",
                1,
                Arrays.asList("ddcabc30c88a755a78100e30e0d491d2"));
        executeTestParallel("testVEMendelianViolationEvaluator" + vcfFile, spec);
    }

    @Test
    public void testCompVsEvalAC() {
        String extraArgs = "-T VariantEval -R "+b36KGReference+" -o %s -ST CpG -EV GenotypeConcordance --eval:evalYRI,VCF3 " + validationDataLocation + "yri.trio.gatk.ug.very.few.lines.vcf --comp:compYRI,VCF3 " + validationDataLocation + "yri.trio.gatk.fake.genotypes.ac.test.vcf";
        WalkerTestSpec spec = new WalkerTestSpec(extraArgs,1,Arrays.asList("bb076f7239039191fde883c5e68483ea"));
        executeTestParallel("testCompVsEvalAC",spec);
    }

    private static String withSelect(String cmd, String select, String name) {
        return String.format("%s -select '%s' -selectName %s", cmd, select, name);
    }

    @Test(enabled = false) // no longer supported in the GATK
    public void testTranches() {
        String extraArgs = "-T VariantEval -R "+ hg18Reference +" --eval " + validationDataLocation + "GA2.WEx.cleaned.ug.snpfiltered.indelfiltered.optimized.vcf -o %s -EV TiTvVariantEvaluator -L chr1 -noEV -ST CpG -tf " + testDir + "tranches.6.txt";
        WalkerTestSpec spec = new WalkerTestSpec(extraArgs,1,Arrays.asList("6af2b9959aa1778a5b712536de453952"));
        executeTestParallel("testTranches",spec);
    }

    @Test
    public void testCompOverlap() {
        String extraArgs = "-T VariantEval -R " + b37KGReference + " -L " + validationDataLocation + "VariantEval/pacbio.hg19.intervals --comp:comphapmap " + comparisonDataLocation + "Validated/HapMap/3.3/genotypes_r27_nr.b37_fwd.vcf --eval " + validationDataLocation + "VariantEval/pacbio.ts.recalibrated.vcf -noEV -EV CompOverlap -sn NA12878 -noST -ST Novelty -o %s";
        WalkerTestSpec spec = new WalkerTestSpec(extraArgs,1,Arrays.asList("98f9c2f5fef43dbda688d32360908615"));
        executeTestParallel("testCompOverlap",spec);
    }

    @Test
    public void testEvalTrackWithoutGenotypes() {
        String extraArgs = "-T VariantEval -R " +
                           b37KGReference +
                           " -L 20" +
                           " --dbsnp " + b37dbSNP132 +
                           " --eval:evalBI " + validationDataLocation + "VariantEval/ALL.20100201.chr20.bi.sites.vcf" +
                           " -noST -ST Novelty -o %s";
        WalkerTestSpec spec = new WalkerTestSpec(extraArgs,1,Arrays.asList("9d24f34d94d74417e00e3b7bcf84650f"));
        executeTestParallel("testEvalTrackWithoutGenotypes",spec);
    }

    @Test
    public void testMultipleEvalTracksWithoutGenotypes() {
        String extraArgs = "-T VariantEval -R " + b37KGReference +
                " -L 20" +
                " --dbsnp " + b37dbSNP132 +
                " --eval:evalBI " + validationDataLocation + "VariantEval/ALL.20100201.chr20.bi.sites.vcf" +
                " --eval:evalBC " + validationDataLocation + "VariantEval/ALL.20100201.chr20.bc.sites.vcf" +
                " -noST -ST Novelty -o %s";
        WalkerTestSpec spec = new WalkerTestSpec(extraArgs,1,Arrays.asList("7329b0bc73c9ccaf5facd754f3410c38"));
        executeTestParallel("testMultipleEvalTracksWithoutGenotypes",spec);
    }

    @Test
    public void testMultipleCompTracks() {
        String dbsnp = GATKDataLocation + "dbsnp_132_b37.vcf";

        String extraArgs =  "-T VariantEval" +
                           " -R " + b37KGReference +
                           " --comp " + validationDataLocation + "/VariantEval/ALL.phase1.chr20.broad.snps.genotypes.subset.vcf" +
                           " --eval " + validationDataLocation + "/VariantEval/NA12878.hg19.HiSeq.WGS.cleaned.ug.snpfiltered.indelfiltered.optimized.cut.subset.vcf" +
                           " --dbsnp " + dbsnp +
                           " -L 20:10000000-10100000" +
                           " -noST -noEV -ST Novelty -EV CompOverlap" +
                           " -o %s";

        WalkerTestSpec spec = new WalkerTestSpec(extraArgs,1,Arrays.asList("d0218c5435c8601f2355b7d183ab032f"));
        executeTestParallel("testMultipleCompTracks",spec);
    }

    @Test
    public void testPerSampleAndSubsettedSampleHaveSameResults1() {
        String md5 = "b5cd5c286d459b8edd4ca54320e560a3";

        WalkerTestSpec spec = new WalkerTestSpec(
                buildCommandLine(
                        "-T VariantEval",
                        "-R " + b37KGReference,
                        "--dbsnp " + b37dbSNP132,
                        "--eval " + fundamentalTestSNPsVCF,
                        "-noEV",
                        "-EV CompOverlap",
                        "-sn NA12045",
                        "-noST",
                        "-L " + fundamentalTestSNPsVCF,
                        "-o %s"
                ),
                1,
                Arrays.asList(md5)
        );
        executeTestParallel("testPerSampleAndSubsettedSampleHaveSameResults-subset", spec);

        WalkerTestSpec spec2 = new WalkerTestSpec(
                buildCommandLine(
                        "-T VariantEval",
                        "-R " + b37KGReference,
                        "--dbsnp " + b37dbSNP132,
                        "--eval " + fundamentalTestSNPsOneSampleVCF,
                        "-noEV",
                        "-EV CompOverlap",
                        "-noST",
                        "-L " + fundamentalTestSNPsOneSampleVCF,
                        "-o %s"
                ),
                1,
                Arrays.asList(md5)
        );
        executeTestParallel("testPerSampleAndSubsettedSampleHaveSameResults-onesample", spec2);
    }


    @Test
    public void testAlleleCountStrat() {
        WalkerTestSpec spec = new WalkerTestSpec(
                buildCommandLine(
                        "-T VariantEval",
                        "-R " + b37KGReference,
                        "--dbsnp " + b37dbSNP132,
                        "--eval " + fundamentalTestSNPsVCF,
                        "-noEV",
                        "-EV CountVariants",
                        "-noST",
                        "-ST AlleleCount",
                        "-L " + fundamentalTestSNPsVCF,
                        "-o %s"
                ),
                1,
                Arrays.asList("1198bfea6183bd43219071a84c79a386")
        );
        executeTest("testAlleleCountStrat", spec);
    }

    @Test
    public void testMultipleEvalTracksAlleleCountWithMerge() {
        WalkerTestSpec spec = new WalkerTestSpec(
                buildCommandLine(
                        "-T VariantEval",
                        "-R " + b37KGReference,
                        "--dbsnp " + b37dbSNP132,
                        "--eval " + fundamentalTestSNPsSplit1of2VCF,
                        "--eval " + fundamentalTestSNPsSplit2of2VCF,
                        "--mergeEvals",
                        "-noEV",
                        "-EV CountVariants",
                        "-noST",
                        "-ST AlleleCount",
                        "-L " + fundamentalTestSNPsVCF,
                        "-o %s"
                ),
                1,
                Arrays.asList("1198bfea6183bd43219071a84c79a386")
        );
        executeTest("testMultipleEvalTracksAlleleCountWithMerge", spec);
    }

    @Test
    public void testMultipleEvalTracksAlleleCountWithoutMerge() {
        WalkerTestSpec spec = new WalkerTestSpec(
                buildCommandLine(
                        "-T VariantEval",
                        "-R " + b37KGReference,
                        "--dbsnp " + b37dbSNP132,
                        "--eval " + fundamentalTestSNPsSplit1of2VCF,
                        "--eval " + fundamentalTestSNPsSplit2of2VCF,
                        //"--mergeEvals", No merge with AC strat ==> error
                        "-noEV",
                        "-EV CountVariants",
                        "-noST",
                        "-ST AlleleCount",
                        "-L " + fundamentalTestSNPsVCF
                ),
                0,
                UserException.class
        );
        executeTest("testMultipleEvalTracksAlleleCountWithoutMerge", spec);
    }

    @Test
    public void testIntervalStrat() {
        WalkerTestSpec spec = new WalkerTestSpec(
                                buildCommandLine(
                                        "-T VariantEval",
                                        "-R " + b37KGReference,
                                        "-eval " + testDir + "/withSymbolic.b37.vcf",
                                        "-noEV",
                                        "-EV CountVariants",
                                        "-noST",
                                        "-stratIntervals " + testDir + "/overlapTest.bed",
                                        "-ST IntervalStratification",
                                        "-L 20",
                                        "-o %s"
                                ),
                                1,
                                Arrays.asList("6decba040051daafad4ecad5a411e1e1")
                              );
        executeTest("testIntervalStrat", spec);
    }

    @Test
    public void testModernVCFWithLargeIndels() {
        WalkerTestSpec spec = new WalkerTestSpec(
                                buildCommandLine(
                                        "-T VariantEval",
                                        "-R " + b37KGReference,
                                        "-eval " + validationDataLocation + "/NA12878.HiSeq.WGS.b37_decoy.indel.recalibrated.vcf",
                                        "-L 20",
                                        "-D " + b37dbSNP132,
                                        "-o %s"
                                ),
                                1,
                                Arrays.asList("aad01b26198b30da5d59a05c08d863bb")
                              );
        executeTest("testModernVCFWithLargeIndels", spec);
    }

    @Test
    public void testStandardIndelEval() {
        WalkerTestSpec spec = new WalkerTestSpec(
                buildCommandLine(
                        "-T VariantEval",
                        "-R " + b37KGReference,
                        "-eval " + validationDataLocation + "/NA12878.HiSeq.WGS.b37_decoy.indel.recalibrated.vcf",
                        "-L 20",
                        "-noST -ST Sample -ST OneBPIndel -ST TandemRepeat",
                        "-noEV -EV IndelSummary -EV IndelLengthHistogram",
                        "-gold " + validationDataLocation + "/Mills_and_1000G_gold_standard.indels.b37.sites.vcf",
                        "-D " + b37dbSNP132,
                        "-o %s"
                ),
                1,
                Arrays.asList("4fa2557663ef8fb4cdeecd667791985c")
        );
        executeTest("testStandardIndelEval", spec);
    }


    @Test()
    public void testIncompatibleEvalAndStrat() {
        WalkerTestSpec spec = new WalkerTestSpec(
                buildCommandLine(
                        "-T VariantEval",
                        "-R " + b37KGReference,
                        "-eval " + validationDataLocation + "/NA12878.HiSeq.WGS.b37_decoy.indel.recalibrated.vcf",
                        "-L 20 -noST -ST AlleleCount -noEV -EV VariantSummary"
                ),
                0,
                UserException.class);
        executeTest("testIncompatibleEvalAndStrat", spec);
    }

    public void testIncludingAC0(boolean includeAC0, final String md5) {
        WalkerTestSpec spec = new WalkerTestSpec(
                buildCommandLine(
                        "-T VariantEval",
                        "-R " + b37KGReference,
                        "-eval " + testDir + "/ac0.vcf",
                        "-L 20:81006 -noST -noEV -EV VariantSummary -o %s" + (includeAC0 ? " -keepAC0" : "")
                ),
                1,
                Arrays.asList(md5));
        executeTest("testIncludingAC0 keep ac 0 = " + includeAC0, spec);
    }

    @Test public void testWithAC0() { testIncludingAC0(true, "0ed2c8e4b4e06973a06838bc930a132d"); }
    @Test public void testWithoutAC0() { testIncludingAC0(false, "79d28ddd0ab9584776b6cbefe48331df"); }

}
