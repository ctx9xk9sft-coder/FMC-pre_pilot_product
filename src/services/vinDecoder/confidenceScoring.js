export function buildConfidenceSnapshot(decoded = {}) {
  const exactVinMatch = Boolean(decoded?.enrichment?.exactVinMatch);
  const engineCandidates = decoded?.enrichment?.possibleEngineCodes?.length || 0;
  const gearboxCandidates = decoded?.enrichment?.possibleGearboxCodes?.length || 0;
  const warnings = decoded?.warnings?.length || 0;
  const patternMatch = Boolean(decoded?.patternMatch);

  let score = 35;
  if (decoded?.valid) score += 20;
  if (patternMatch) score += 10;
  if (exactVinMatch) score += 25;
  if (engineCandidates === 1) score += 5;
  if (gearboxCandidates === 1) score += 5;
  score -= warnings * 5;

  if (decoded?.confidence === "high") score = Math.max(score, 85);
  if (decoded?.confidence === "medium") score = Math.max(score, 65);
  if (decoded?.confidence === "low") score = Math.min(score, 45);

  return {
    label: decoded?.confidence || "low",
    score: Math.max(0, Math.min(100, score)),
    factors: {
      exactVinMatch,
      patternMatch,
      engineCandidates,
      gearboxCandidates,
      warnings,
    },
  };
}

export default buildConfidenceSnapshot;
