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

package org.broadinstitute.sting.gatk.walkers.diagnostics.targets;

/**
 * Short one line description of the walker.
 *
 * @author Mauricio Carneiro
 * @since 2/1/12
 */
public enum CallableStatus {
    /**
     * the reference base was an N, which is not considered callable the GATK
     */
    // todo -- implement this status
    REF_N("the reference base was an N, which is not considered callable the GATK"),
    /**
     * the base satisfied the min. depth for calling but had less than maxDepth to avoid having EXCESSIVE_COVERAGE
     */
    PASS("the base satisfied the min. depth for calling but had less than maxDepth to avoid having EXCESSIVE_COVERAGE"),
    /**
     * absolutely no reads were seen at this locus, regardless of the filtering parameters
     */
    NO_COVERAGE("absolutely no reads were seen at this locus, regardless of the filtering parameters"),
    /**
     * there were less than min. depth bases at the locus, after applying filters
     */
    LOW_COVERAGE("there were less than min. depth bases at the locus, after applying filters"),
    /**
     * more than -maxDepth read at the locus, indicating some sort of mapping problem
     */
    EXCESSIVE_COVERAGE("more than -maxDepth read at the locus, indicating some sort of mapping problem"),
    /**
     * more than --maxFractionOfReadsWithLowMAPQ at the locus, indicating a poor mapping quality of the reads
     */
    POOR_QUALITY("more than --maxFractionOfReadsWithLowMAPQ at the locus, indicating a poor mapping quality of the reads"),

    BAD_MATE(""),

    INCONSISTENT_COVERAGE("");


    public String description;

    private CallableStatus(String description) {
        this.description = description;
    }
}
