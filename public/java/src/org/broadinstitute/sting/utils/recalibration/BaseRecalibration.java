/*
 * Copyright (c) 2012 The Broad Institute
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

package org.broadinstitute.sting.utils.recalibration;

import org.broadinstitute.sting.gatk.walkers.bqsr.*;
import org.broadinstitute.sting.utils.BitSetUtils;
import org.broadinstitute.sting.utils.QualityUtils;
import org.broadinstitute.sting.utils.exceptions.ReviewedStingException;
import org.broadinstitute.sting.utils.sam.GATKSAMRecord;

import java.io.File;
import java.util.*;

/**
 * Utility methods to facilitate on-the-fly base quality score recalibration.
 *
 * User: carneiro and rpoplin
 * Date: 2/4/12
 */

public class BaseRecalibration {
    private QuantizationInfo quantizationInfo;                                                                          // histogram containing the map for qual quantization (calculated after recalibration is done)
    private LinkedHashMap<BQSRKeyManager, Map<BitSet, RecalDatum>> keysAndTablesMap;                                    // quick access reference to the read group table and its key manager
    private ArrayList<Covariate> requestedCovariates = new ArrayList<Covariate>();                                      // list of all covariates to be used in this calculation


    /**
     * Constructor using a GATK Report file
     * 
     * @param RECAL_FILE a GATK Report file containing the recalibration information
     * @param quantizationLevels number of bins to quantize the quality scores
     */
    public BaseRecalibration(final File RECAL_FILE, int quantizationLevels) {
        RecalibrationReport recalibrationReport = new RecalibrationReport(RECAL_FILE);

        keysAndTablesMap = recalibrationReport.getKeysAndTablesMap();
        requestedCovariates = recalibrationReport.getRequestedCovariates();
        quantizationInfo = recalibrationReport.getQuantizationInfo();
        if (quantizationLevels == 0)                                                                                    // quantizationLevels == 0 means no quantization, preserve the quality scores
            quantizationInfo.noQuantization();
        else if (quantizationLevels > 0 && quantizationLevels != quantizationInfo.getQuantizationLevels())              // any other positive value means, we want a different quantization than the one pre-calculated in the recalibration report. Negative values mean the user did not provide a quantization argument, and just wnats to use what's in the report.
            quantizationInfo.quantizeQualityScores(quantizationLevels);
    }

    /**
     * This constructor only exists for testing purposes.
     *
     * @param quantizationInfo the quantization info object
     * @param keysAndTablesMap the map of key managers and recalibration tables
     * @param requestedCovariates the list of requested covariates
     */
    protected BaseRecalibration(QuantizationInfo quantizationInfo, LinkedHashMap<BQSRKeyManager, Map<BitSet, RecalDatum>> keysAndTablesMap, ArrayList<Covariate> requestedCovariates) {
        this.quantizationInfo = quantizationInfo;
        this.keysAndTablesMap = keysAndTablesMap;
        this.requestedCovariates = requestedCovariates;
    }

    /**
     * Recalibrates the base qualities of a read
     *
     * It updates the base qualities of the read with the new recalibrated qualities (for all event types)
     *
     * @param read the read to recalibrate
     */
    public void recalibrateRead(final GATKSAMRecord read) {
        final ReadCovariates readCovariates = RecalDataManager.computeCovariates(read, requestedCovariates);            // compute all covariates for the read
        for (final EventType errorModel : EventType.values()) {                                                         // recalibrate all three quality strings
            final byte[] originalQuals = read.getBaseQualities(errorModel);
            final byte[] recalQuals = originalQuals.clone();

            for (int offset = 0; offset < read.getReadLength(); offset++) {                                             // recalibrate all bases in the read
                byte qualityScore = originalQuals[offset];

                if (qualityScore >= QualityUtils.MIN_USABLE_Q_SCORE) {                                                  // only recalibrate usable qualities (the original quality will come from the instrument -- reported quality)
                    final BitSet[] keySet = readCovariates.getKeySet(offset, errorModel);                               // get the keyset for this base using the error model
                    qualityScore = performSequentialQualityCalculation(keySet, errorModel);                             // recalibrate the base
                }
                recalQuals[offset] = qualityScore;
            }
            read.setBaseQualities(recalQuals, errorModel);
        }
    }


    
    /**
     * Implements a serial recalibration of the reads using the combinational table.
     * First, we perform a positional recalibration, and then a subsequent dinuc correction.
     *
     * Given the full recalibration table, we perform the following preprocessing steps:
     *
     * - calculate the global quality score shift across all data [DeltaQ]
     * - calculate for each of cycle and dinuc the shift of the quality scores relative to the global shift
     * -- i.e., DeltaQ(dinuc) = Sum(pos) Sum(Qual) Qempirical(pos, qual, dinuc) - Qreported(pos, qual, dinuc) / Npos * Nqual
     * - The final shift equation is:
     *
     * Qrecal = Qreported + DeltaQ + DeltaQ(pos) + DeltaQ(dinuc) + DeltaQ( ... any other covariate ... )
     * 
     * @param key        The list of Comparables that were calculated from the covariates
     * @param errorModel the event type
     * @return A recalibrated quality score as a byte
     */
    protected byte performSequentialQualityCalculation(BitSet[] key, EventType errorModel) {
        final String UNRECOGNIZED_REPORT_TABLE_EXCEPTION = "Unrecognized table. Did you add an extra required covariate? This is a hard check that needs propagate through the code";
        final String TOO_MANY_KEYS_EXCEPTION = "There should only be one key for the RG collapsed table, something went wrong here";

        final byte qualFromRead = (byte) BitSetUtils.shortFrom(key[1]);

        double globalDeltaQ = 0.0;
        double deltaQReported = 0.0;
        double deltaQCovariates = 0.0;

        for (Map.Entry<BQSRKeyManager, Map<BitSet, RecalDatum>> mapEntry : keysAndTablesMap.entrySet()) {
            BQSRKeyManager keyManager = mapEntry.getKey();
            Map<BitSet, RecalDatum> table = mapEntry.getValue();

            switch(keyManager.getRequiredCovariates().size()) {
                case 1:                                                                                                 // this is the ReadGroup table                    
                    List<BitSet> bitKeys = keyManager.bitSetsFromAllKeys(key, errorModel);                              // calculate the shift in quality due to the read group
                    if (bitKeys.size() > 1)
                        throw new ReviewedStingException(TOO_MANY_KEYS_EXCEPTION);

                    final RecalDatum empiricalQualRG = table.get(bitKeys.get(0));
                    if (empiricalQualRG != null) {
                        final double globalDeltaQEmpirical = empiricalQualRG.getEmpiricalQuality();
                        final double aggregrateQReported = empiricalQualRG.getEstimatedQReported();
                        globalDeltaQ = globalDeltaQEmpirical - aggregrateQReported;
                    }
                    break;
                case 2:
                    if (keyManager.getOptionalCovariates().isEmpty()) {                                                 // this is the QualityScore table
                        bitKeys = keyManager.bitSetsFromAllKeys(key, errorModel);                                       // calculate the shift in quality due to the reported quality score
                        if (bitKeys.size() > 1)
                            throw new ReviewedStingException(TOO_MANY_KEYS_EXCEPTION);

                        final RecalDatum empiricalQualQS = table.get(bitKeys.get(0));
                        if (empiricalQualQS != null) {
                            final double deltaQReportedEmpirical = empiricalQualQS.getEmpiricalQuality();
                            deltaQReported = deltaQReportedEmpirical - qualFromRead - globalDeltaQ;
                        }
                    }
                    else {                                                                                              // this is the table with all the covariates                        
                        bitKeys = keyManager.bitSetsFromAllKeys(key, errorModel);                                       // calculate the shift in quality due to each covariate by itself in turn
                        for (BitSet k : bitKeys) {
                            final RecalDatum empiricalQualCO = table.get(k);
                            if (empiricalQualCO != null) {
                                double deltaQCovariateEmpirical = empiricalQualCO.getEmpiricalQuality();
                                deltaQCovariates += (deltaQCovariateEmpirical - qualFromRead - (globalDeltaQ + deltaQReported));
                            }
                        }
                    }
                    break;
                default:
                    throw new ReviewedStingException(UNRECOGNIZED_REPORT_TABLE_EXCEPTION);
            }
        }

        double recalibratedQual = qualFromRead + globalDeltaQ + deltaQReported + deltaQCovariates;                      // calculate the recalibrated qual using the BQSR formula 
        recalibratedQual = QualityUtils.boundQual((int) Math.round(recalibratedQual), QualityUtils.MAX_RECALIBRATED_Q_SCORE);     // recalibrated quality is bound between 1 and MAX_QUAL

        return quantizationInfo.getQuantizedQuals().get((int) recalibratedQual);                                        // return the quantized version of the recalibrated quality
    }

}
