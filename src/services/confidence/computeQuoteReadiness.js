function unique(values = []) {
  return [...new Set(values.filter(Boolean))];
}

export function computeQuoteReadiness({
  overallStatus,
  vehicleConfidence = null,
  pricingConfidence = null,
  maintenancePlan = null,
  resolverMissingConfirmations = [],
} = {}) {
  const warnings = unique([
    ...(vehicleConfidence?.warnings || []),
    ...(pricingConfidence?.warnings || []),
  ]);

  const hardBlockers = unique([
    ...(vehicleConfidence?.blockers || []),
    ...(pricingConfidence?.blockers || []),
  ]);

  const manualReviewFlags = resolverMissingConfirmations.length > 0 ? ['manual_confirmation_required'] : [];
  const vehicleRisk = vehicleConfidence?.metrics?.weightedScore != null ? 1 - Number(vehicleConfidence.metrics.weightedScore) : 1;
  const pricingRisk = pricingConfidence?.level === 'high' ? 0 : pricingConfidence?.level === 'medium' ? 0.35 : 0.8;
  const totalRisk = Number((vehicleRisk * 0.6 + pricingRisk * 0.4).toFixed(2));
  const operationalReadiness = vehicleConfidence?.metrics?.operationalReadiness || (overallStatus === 'ready_for_planning' ? 'quote_ready' : 'manual_review_required');

  let status = 'REQUIRES_REVIEW';
  if (
    operationalReadiness === 'quote_ready' &&
    pricingConfidence?.level === 'high' &&
    maintenancePlan &&
    hardBlockers.length === 0 &&
    manualReviewFlags.length === 0
  ) {
    status = 'READY_FOR_QUOTE';
  } else if (
    ['quote_ready', 'estimate_ready'].includes(operationalReadiness) &&
    maintenancePlan &&
    hardBlockers.length === 0 &&
    totalRisk < 0.65
  ) {
    status = 'ESTIMATE_ONLY';
  }

  const blockers = unique([
    ...hardBlockers,
    ...(status === 'REQUIRES_REVIEW' ? manualReviewFlags : []),
  ]);

  const label =
    status === 'READY_FOR_QUOTE'
      ? 'Spremno za ponudu'
      : status === 'ESTIMATE_ONLY'
      ? 'Procena — potrebna provera'
      : 'Potrebna dodatna provera';

  return {
    status,
    label,
    warnings,
    blockers,
    decision: operationalReadiness,
    metrics: { totalRisk },
  };
}

export default computeQuoteReadiness;
