function unique(values = []) {
  return [...new Set(values.filter(Boolean))];
}

export function buildPatternInsights(decoded = {}, core = null) {
  const possibleModels = unique([
    decoded?.model,
    decoded?.model_info?.name,
    ...(decoded?.possible_matches || []).map((item) => item?.model || item?.name || null),
  ]);

  const ruleset = decoded?.model_info?.resolved_ruleset || decoded?.enrichment?.patternRule?.name || null;

  return {
    matched: Boolean(decoded?.patternMatch || decoded?.enrichment?.patternRule),
    matchedRuleset: ruleset,
    platformCode: core?.segments?.platformCode || null,
    bodyCode: core?.segments?.fullBodyCode || core?.segments?.bodyCode || null,
    ambiguousModel: Boolean(decoded?.special_flags?.ambiguous_model) || possibleModels.length > 1,
    possibleModels,
  };
}

export default buildPatternInsights;
