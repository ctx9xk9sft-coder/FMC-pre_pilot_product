function unique(values = []) {
  return [...new Set(values.filter(Boolean))];
}

export function buildEnrichmentSnapshot(decoded = {}) {
  const enrichment = decoded?.enrichment || {};

  return {
    exactVinMatch: Boolean(enrichment.exactVinMatch),
    engineCandidatesCount: unique([
      ...(enrichment.possibleEngineCodes || []),
      ...(enrichment.engineCandidates || []).map((item) => item?.code || null),
    ]).length,
    gearboxCandidatesCount: unique([
      ...(enrichment.possibleGearboxCodes || []),
      ...(enrichment.gearboxTechCandidates || []),
    ]).length,
    selectedEngineCode: enrichment?.selectedEngine?.code || null,
    selectedGearboxCode: enrichment?.selectedGearbox?.code || null,
    engineSource: enrichment?.engineSource || "not_enriched",
    gearboxSource: enrichment?.gearboxSource || "not_enriched",
  };
}

export default buildEnrichmentSnapshot;
