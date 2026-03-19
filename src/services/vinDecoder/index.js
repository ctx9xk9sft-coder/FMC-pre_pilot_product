import { decodeSkodaVin as decodeSkodaVinLegacy } from "./legacyDecoder.js";
import { parseVinCore } from "./parseVinCore.js";
import { buildPatternInsights } from "./patternMatcher.js";
import { buildEnrichmentSnapshot } from "./enrichmentEngine.js";
import { buildConfidenceSnapshot } from "./confidenceScoring.js";

function getGearboxTypeBucket(code) {
  const value = String(code || "").trim().toUpperCase();
  if (!value) return null;
  if (["W", "V", "U"].includes(value[0])) return "dsg";
  if (["Q", "T", "S"].includes(value[0])) return "manual";
  return null;
}

function applyGearboxDisplayGuardrails(decoded) {
  const candidates = Array.isArray(decoded?.enrichment?.possibleGearboxCodes)
    ? decoded.enrichment.possibleGearboxCodes
    : [];

  if (candidates.length < 2) return decoded;

  const buckets = [...new Set(candidates.map(getGearboxTypeBucket).filter(Boolean))];
  if (buckets.length < 2) return decoded;

  return {
    ...decoded,
    menjac: null,
    gearboxCode: candidates.join(", "),
    menjacSource: "ambiguous_candidates",
  };
}

function attachSprint6Meta(decoded, core) {
  return {
    ...decoded,
    decoderMeta: {
      architectureVersion: "vin-decoder-sprint6-modular-wrapper",
      parsedCore: {
        vin: core.cleanVin,
        segments: core.segments,
        derived: core.derived,
        validationErrors: core.validationErrors,
      },
      patternInsights: buildPatternInsights(decoded, core),
      enrichmentSnapshot: buildEnrichmentSnapshot(decoded),
      confidenceSnapshot: buildConfidenceSnapshot(decoded),
    },
  };
}

export function decodeSkodaVin(vin) {
  const core = parseVinCore(vin);
  const decoded = applyGearboxDisplayGuardrails(decodeSkodaVinLegacy(vin));
  return attachSprint6Meta(decoded, core);
}

export default decodeSkodaVin;
