function unique(values = []) {
  return [...new Set(values.filter(Boolean))];
}

export function computePricingConfidence({ pricingMeta = null } = {}) {
  const coverage = Number(pricingMeta?.pricingCoveragePercent ?? 0);
  const missingPriceEventCount = Number(pricingMeta?.missingPriceEventCount ?? 0);
  const genericEventCount = Number(pricingMeta?.genericEventCount ?? 0);
  const familyEventCount = Number(pricingMeta?.familyEventCount ?? 0);
  const fallbackLineCount = Number(pricingMeta?.fallbackLineCount ?? 0);
  const warnings = [];
  const blockers = [];

  let level = 'low';
  if (missingPriceEventCount === 0 && fallbackLineCount === 0 && genericEventCount === 0 && familyEventCount === 0 && coverage >= 99) {
    level = 'high';
  } else if (missingPriceEventCount === 0 && fallbackLineCount <= 1 && coverage >= 95) {
    level = 'medium';
  }

  if (missingPriceEventCount > 0) {
    blockers.push('missing_price_events');
    warnings.push('Plan sadrži događaje bez potpune cene.');
  }

  if (fallbackLineCount > 0) {
    warnings.push('Korišćene su fallback procene za deo pricing stavki.');
  }

  if (genericEventCount > 0 || familyEventCount > 0) {
    warnings.push('Deo pricing-a je baziran na generičkom ili model-family profilu.');
  }

  for (const warning of pricingMeta?.warnings || []) {
    warnings.push(warning);
  }

  return {
    level,
    warnings: unique(warnings),
    blockers: unique(blockers),
    metrics: {
      pricingCoveragePercent: coverage,
      exactEventCount: Number(pricingMeta?.exactEventCount ?? 0),
      familyEventCount,
      genericEventCount,
      missingPriceEventCount,
      fallbackLineCount,
      pricingRange: pricingMeta?.pricingRange || null,
    },
  };
}

export default computePricingConfidence;
