package org.broadinstitute.sting.gatk.walkers.varianteval.stratifications;

import org.broadinstitute.sting.gatk.contexts.ReferenceContext;
import org.broadinstitute.sting.gatk.refdata.RefMetaDataTracker;
import org.broadinstitute.sting.gatk.walkers.varianteval.evaluators.VariantEvaluator;
import org.broadinstitute.sting.gatk.walkers.varianteval.evaluators.VariantSummary;
import org.broadinstitute.sting.utils.variantcontext.VariantContext;

import java.util.*;

/**
 * Stratifies the eval RODs by each sample in the eval ROD.
 *
 * This allows the system to analyze each sample separately.  Since many evaluations
 * only consider non-reference sites, stratifying by sample results in meaningful
 * calculations for CompOverlap
 */
public class Sample extends VariantStratifier {
    @Override
    public void initialize() {
        states.addAll(getVariantEvalWalker().getSampleNamesForStratification());
    }

    public List<Object> getRelevantStates(ReferenceContext ref, RefMetaDataTracker tracker, VariantContext comp, String compName, VariantContext eval, String evalName, String sampleName) {
        return Collections.singletonList((Object) sampleName);
    }

    @Override
    public Set<Class<? extends VariantEvaluator>> getIncompatibleEvaluators() {
        return new HashSet<Class<? extends VariantEvaluator>>(Arrays.asList(VariantSummary.class));
    }
}
