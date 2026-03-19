function isNonEmptyString(value) {
  return typeof value === "string" && value.trim().length > 0;
}

function uniqueStrings(values) {
  return [...new Set((values || []).filter(isNonEmptyString).map((v) => v.trim()))];
}

import { deriveGearboxClosure, getGearboxSemanticProfile } from "./resolution/gearboxSemantic.js";
import { resolveDrivetrainField } from "./resolution/drivetrainResolver.js";
import { inferVinConfiguration, getInferenceCandidateList } from "../lib/vinInferenceEngine.js";

function normalizeDrivetrain(value) {
  if (!isNonEmptyString(value)) return null;
  const normalized = value.trim().toUpperCase();

  if (["AWD", "4X4"].includes(normalized)) return "AWD";
  if (["FWD"].includes(normalized)) return "FWD";
  if (["2WD"].includes(normalized)) return "2WD";

  return null;
}

function normalizeFuelType(decoded = {}) {
  const raw = String(decoded?.fuelType || decoded?.gorivo || "").trim().toLowerCase();
  const engineText = String(decoded?.motorKod || decoded?.engineCode || decoded?.motor || "").trim().toLowerCase();

  if (raw.includes("diesel") || raw.includes("dizel") || engineText.includes("tdi")) return "diesel";
  if (raw.includes("petrol") || raw.includes("benzin") || engineText.includes("tsi") || engineText.includes("mpi")) return "petrol";
  return null;
}

function normalizeGearboxType(value, semantic = null) {
  const semanticType = semantic?.transmissionType || semantic?.type || null;
  if (semanticType) return semanticType;
  if (!isNonEmptyString(value)) return null;
  const normalized = value.trim().toUpperCase();
  if (normalized.includes("DSG") || normalized.includes("AUTOMAT")) return normalized.includes("DSG") ? "dsg" : "automatic";
  if (normalized.includes("MANUAL")) return "manual";
  return null;
}

function normalizeCandidateValue(value) {
  return isNonEmptyString(value) ? value.trim().toUpperCase() : null;
}

function getCompetingCandidates(field = {}) {
  const selected = normalizeCandidateValue(field?.value);
  const candidates = uniqueStrings(field?.candidates || []).map(normalizeCandidateValue).filter(Boolean);
  return uniqueStrings(candidates.filter((candidate) => candidate !== selected));
}

function hasCompetingCandidates(field = {}) {
  return getCompetingCandidates(field).length > 0;
}

function isStrongResolved(field = {}) {
  return Boolean(field?.resolved) && ["exact", "high", "confirmed"].includes(String(field?.confidence || "").toLowerCase());
}

function usesInferenceSource(field = {}) {
  return String(field?.source || "").toLowerCase() === "inference";
}

function getFieldDominanceRatio(field = {}) {
  const ratio = Number(field?.inferenceMeta?.dominanceScore ?? field?.inferenceMeta?.dominanceRatio ?? field?.inferenceMeta?.supportRatio);
  if (!Number.isFinite(ratio)) return 0;
  return ratio;
}

function getFieldSupportCount(field = {}) {
  const sampleCount = Number(field?.inferenceMeta?.supportCount ?? field?.inferenceMeta?.sampleCount);
  if (!Number.isFinite(sampleCount)) return 0;
  return sampleCount;
}

function getFieldConflictCount(field = {}) {
  const conflictCount = Number(field?.inferenceMeta?.conflictCount ?? field?.semantic?.conflictCount ?? 0);
  if (!Number.isFinite(conflictCount)) return 0;
  return conflictCount;
}

function hasMeaningfulGearboxCompetition(field = {}) {
  if (field?.field !== "gearbox") return hasCompetingCandidates(field);
  const semantic = field?.semantic || deriveGearboxClosure({ code: field?.value, candidates: field?.candidates || [] });
  if ((semantic?.familyClosed || semantic?.transmissionTypeClosed) && !semantic?.hasConflict) {
    return false;
  }
  return hasCompetingCandidates(field);
}

function isDominantInferenceField(field = {}) {
  if (!field?.resolved || !usesInferenceSource(field)) return false;
  if (!isStrongResolved(field)) return false;

  if (field?.field === "gearbox") {
    const semantic = field?.semantic || deriveGearboxClosure({ code: field?.value, candidates: field?.candidates || [] });
    if (!(semantic?.familyClosed || semantic?.transmissionTypeClosed)) return false;
    if (semantic?.hasConflict) return false;
  } else if (hasCompetingCandidates(field)) {
    return false;
  }

  return getFieldDominanceRatio(field) >= 0.6 && getFieldSupportCount(field) >= 2 && getFieldConflictCount(field) <= 1;
}

function isDrivetrainConsistent(fields = {}) {
  const gearboxDrive = normalizeDrivetrain(fields?.gearbox?.semantic?.drivetrain);
  const drivetrain = normalizeDrivetrain(fields?.drivetrain?.value);
  if (!drivetrain) return false;
  if (!gearboxDrive || gearboxDrive === "2WD") return true;
  return gearboxDrive === drivetrain;
}

function getManualOverrideWarnings(field, overrideValue, candidates = [], resolvedValue = null) {
  if (!isNonEmptyString(overrideValue)) return [];
  const normalizedOverride = overrideValue.trim().toLowerCase();
  const normalizedCandidates = uniqueStrings(candidates).map((item) => item.toLowerCase());
  const warnings = [];

  if (normalizedCandidates.length > 0 && !normalizedCandidates.includes(normalizedOverride)) {
    warnings.push(`${field}_manual_override_outside_candidates`);
  }

  if (resolvedValue && String(resolvedValue).trim().toLowerCase() !== normalizedOverride) {
    warnings.push(`${field}_manual_override_changed_decoded_value`);
  }

  return warnings;
}

function buildCanonicalVehicle({ decoded, fields, manualOverrides = {} }) {
  const fuelType = normalizeFuelType(decoded);
  const gearboxDisplay = fields?.gearbox?.displayValue || fields?.gearbox?.value || decoded?.menjac || null;

  return {
    vin: String(decoded?.vin || "").trim() || null,
    brand: decoded?.marka || "Skoda",
    model: fields?.model?.value || null,
    modelYear: fields?.modelYear?.value ? Number(fields.modelYear.value) : null,
    engineType: fuelType,
    engineCode: fields?.engine?.value || null,
    engine: fields?.engine?.value || null,
    displacement: decoded?.zapremina || decoded?.engineDisplacement || null,
    powerHp: Number.isFinite(Number(decoded?.snagaKs)) ? Number(decoded.snagaKs) : null,
    gearboxType: normalizeGearboxType(gearboxDisplay, fields?.gearbox?.semantic),
    gearboxCode: fields?.gearbox?.value || null,
    gearbox: gearboxDisplay,
    drivetrain: fields?.drivetrain?.value || null,
    fuelType,
    confidence:
      fields?.engine?.confidence === "confirmed" || fields?.gearbox?.confidence === "confirmed"
        ? "high"
        : [fields?.engine, fields?.gearbox, fields?.drivetrain].every((field) => field?.resolved && ["exact", "high", "confirmed"].includes(field?.confidence))
        ? "high"
        : fields?.engine?.resolved
        ? "medium"
        : "low",
    source: {
      vinDecoded: Boolean(decoded?.supported),
      manualOverrideUsed: Object.values(manualOverrides).some((value) => isNonEmptyString(value)),
      fieldsResolved: Object.values(fields || {})
        .filter((field) => field?.resolved)
        .map((field) => field.field),
    },
  };
}

function resolveSimpleField({ field, value, source, confidence = "high" }) {
  if (!isNonEmptyString(value)) {
    return { field, resolved: false, value: null, source: "missing", confidence: "none", exact: false, reason: `${field}_missing` };
  }

  return { field, resolved: true, value: value.trim(), source: source || "decoded", confidence, exact: source === "exactVin", reason: null };
}

function resolveEngine(decoded, manualOverrides = {}) {
  const override = manualOverrides.engine || manualOverrides.engineCode || null;
  const exactCode = decoded?.enrichment?.exactVinMatch?.engineCode || null;
  const selectedCode = decoded?.enrichment?.selectedEngine?.code || null;
  const candidates = uniqueStrings([
    exactCode,
    selectedCode,
    ...(decoded?.enrichment?.possibleEngineCodes || []),
    ...(decoded?.enrichment?.engineCandidates || []).map((item) => item?.code),
    decoded?.motorKod,
  ]);

  if (isNonEmptyString(override)) {
    const value = override.trim();
    return {
      field: "engine",
      resolved: true,
      value,
      source: "manual",
      confidence: "confirmed",
      exact: candidates.length > 0 ? candidates.includes(value) : true,
      candidates,
      warnings: getManualOverrideWarnings("engine", value, candidates, selectedCode || decoded?.motorKod),
      reason: null,
    };
  }

  if (exactCode) {
    return { field: "engine", resolved: true, value: exactCode, source: "exactVin", confidence: "exact", exact: true, candidates, warnings: [], reason: null };
  }
  if (selectedCode) {
    return { field: "engine", resolved: true, value: selectedCode, source: decoded?.enrichment?.engineSource || "decoded", confidence: candidates.length <= 1 ? "high" : "medium", exact: candidates.length <= 1, candidates, warnings: [], reason: null };
  }
  if (isNonEmptyString(decoded?.motorKod) && !String(decoded.motorKod).includes(",")) {
    return { field: "engine", resolved: true, value: decoded.motorKod.trim(), source: "decoded", confidence: candidates.length <= 1 ? "high" : "medium", exact: candidates.length <= 1, candidates, warnings: [], reason: null };
  }

  return { field: "engine", resolved: false, value: null, source: "missing", confidence: "none", exact: false, candidates, warnings: [], reason: candidates.length > 1 ? "engine_ambiguous" : "engine_missing" };
}

function resolveGearbox(decoded, manualOverrides = {}) {
  const override = manualOverrides.gearbox || manualOverrides.gearboxCode || null;
  const exactCode = decoded?.enrichment?.exactVinMatch?.transmissionCode || null;
  const selectedCode = decoded?.enrichment?.selectedGearbox?.code || null;
  const inferredDisplay = isNonEmptyString(decoded?.menjac) ? decoded.menjac.trim() : null;

  const candidates = uniqueStrings([
    exactCode,
    selectedCode,
    ...(decoded?.enrichment?.possibleGearboxCodes || []),
    ...(decoded?.enrichment?.gearboxTechCandidates || []),
  ]);

  if (isNonEmptyString(override)) {
    const value = override.trim();
    const semantic = deriveGearboxClosure({ code: value, candidates, allowLabelInference: true });

    return {
      field: "gearbox",
      resolved: true,
      value,
      displayValue: semantic.displayLabel || value,
      source: "manual",
      confidence: "confirmed",
      exact: true,
      candidates,
      warnings: getManualOverrideWarnings("gearbox", value, candidates, selectedCode),
      reason: null,
      semantic,
    };
  }

  if (exactCode) {
    const semantic = deriveGearboxClosure({ code: exactCode, candidates });
    return {
      field: "gearbox",
      resolved: true,
      value: exactCode,
      displayValue: inferredDisplay || semantic.displayLabel || exactCode,
      source: "exactVin",
      confidence: "exact",
      exact: true,
      candidates,
      warnings: [],
      reason: null,
      semantic,
    };
  }

  if (selectedCode) {
    const semantic = deriveGearboxClosure({ code: selectedCode, candidates });
    const candidateCount = candidates.length;
    const safelyClosed = (semantic.familyClosed || semantic.transmissionTypeClosed) && !semantic.hasConflict;
    const confidence = safelyClosed ? (candidateCount <= 4 ? "high" : "medium") : candidateCount <= 2 ? "medium" : "low";

    return {
      field: "gearbox",
      resolved: true,
      value: selectedCode,
      displayValue: inferredDisplay || semantic.displayLabel || selectedCode,
      source: decoded?.enrichment?.gearboxSource || "candidate",
      confidence,
      exact: candidateCount <= 1,
      candidates,
      warnings: uniqueStrings([
        ...(candidateCount > 2 && !safelyClosed ? ["gearbox_selected_from_broad_candidate_cluster"] : []),
        ...(semantic.hasConflict ? ["gearbox_semantic_conflict"] : []),
      ]),
      reason: null,
      semantic,
    };
  }

  if (inferredDisplay) {
    return {
      field: "gearbox",
      resolved: false,
      value: null,
      displayValue: inferredDisplay,
      source: "inferred_type_only",
      confidence: "low",
      exact: false,
      candidates,
      warnings: [],
      reason: candidates.length > 1 ? "gearbox_ambiguous" : "gearbox_code_missing",
      semantic: getGearboxSemanticProfile(inferredDisplay, candidates, { allowLabelInference: false }),
    };
  }

  return {
    field: "gearbox",
    resolved: false,
    value: null,
    displayValue: null,
    source: "missing",
    confidence: "none",
    exact: false,
    candidates,
    warnings: [],
    reason: candidates.length > 1 ? "gearbox_ambiguous" : "gearbox_missing",
    semantic: null,
  };
}

function resolveDrivetrain(decoded, manualOverrides = {}) {
  return resolveDrivetrainField(decoded, manualOverrides);
}

function applyInferenceFallback({ decoded, fields, overrides = {} }) {
  const inference = inferVinConfiguration(decoded);
  const manualEngine = isNonEmptyString(overrides?.engine || overrides?.engineCode);
  const manualGearbox = isNonEmptyString(overrides?.gearbox || overrides?.gearboxCode);
  let inferredEngine = false;
  let inferredGearbox = false;

  if (!inference?.matched) {
    return { fields, inference, inferredEngine, inferredGearbox };
  }

  if (!manualEngine && !fields?.engine?.resolved && inference?.engine?.selected) {
    const inferenceCandidates = getInferenceCandidateList(inference, "engine");
    const mergedCandidates = uniqueStrings([
      ...(fields?.engine?.candidates || []),
      ...inferenceCandidates,
    ]);

    const engineDominant =
      inference.engine.dominanceScore >= 0.6 &&
      inference.engine.supportCount >= 2 &&
      inference.engine.conflictCount <= 1;

    fields.engine = {
      ...fields.engine,
      resolved: true,
      value: inference.engine.selected,
      source: "inference",
      confidence: engineDominant ? "high" : inference.engine.confidence === "high" ? "high" : "medium",
      exact: false,
      candidates: mergedCandidates,
      warnings: uniqueStrings([
        ...(fields?.engine?.warnings || []),
        "engine_inferred_from_dataset_pattern",
      ]),
      reason: null,
      inferenceMeta: {
        field: "engine",
        source: inference.source,
        statsLevel: inference.statsLevel,
        sampleCount: inference.engine.sampleCount,
        supportCount: inference.engine.supportCount,
        supportRatio: inference.engine.supportRatio,
        dominanceRatio: inference.engine.ratio,
        dominanceScore: inference.engine.dominanceScore,
        conflictCount: inference.engine.conflictCount,
      },
    };
    inferredEngine = true;
  }

  if (!manualGearbox && !fields?.gearbox?.resolved && inference?.gearbox?.selected) {
    const inferenceCandidates = getInferenceCandidateList(inference, "gearbox");
    const mergedCandidates = uniqueStrings([
      ...(fields?.gearbox?.candidates || []),
      ...inferenceCandidates,
    ]);
    const semantic = deriveGearboxClosure({ code: inference.gearbox.selected, candidates: mergedCandidates });
    const safelyClosed = (semantic.familyClosed || semantic.transmissionTypeClosed) && !semantic.hasConflict;
    const dominantInference =
      safelyClosed &&
      inference.gearbox.dominanceScore >= 0.6 &&
      inference.gearbox.supportCount >= 2 &&
      inference.gearbox.conflictCount <= 1;
    const confidence = dominantInference ? "high" : safelyClosed ? "medium" : "low";

    fields.gearbox = {
      ...fields.gearbox,
      resolved: true,
      value: inference.gearbox.selected,
      displayValue: semantic.displayLabel || inference.gearbox.selected,
      source: "inference",
      confidence,
      exact: false,
      candidates: mergedCandidates,
      warnings: uniqueStrings([
        ...(fields?.gearbox?.warnings || []),
        "gearbox_inferred_from_dataset_pattern",
        ...(semantic.hasConflict ? ["gearbox_semantic_conflict"] : []),
      ]),
      reason: null,
      semantic,
      inferenceMeta: {
        field: "gearbox",
        source: inference.source,
        statsLevel: inference.statsLevel,
        sampleCount: inference.gearbox.sampleCount,
        supportCount: inference.gearbox.supportCount,
        supportRatio: inference.gearbox.supportRatio,
        dominanceRatio: inference.gearbox.ratio,
        dominanceScore: inference.gearbox.dominanceScore,
        conflictCount: inference.gearbox.conflictCount,
      },
    };
    inferredGearbox = true;
  }

  return { fields, inference, inferredEngine, inferredGearbox };
}

function isExactish(field) {
  return field?.resolved && ["exact", "high", "confirmed"].includes(field?.confidence);
}

function deriveResolutionStatus(fields) {
  const requiredForProvisional = [fields.model, fields.modelYear, fields.engine];

  if (requiredForProvisional.some((field) => !field?.resolved)) {
    return "unresolved";
  }

  const gearboxExactish = isExactish(fields.gearbox) || ((fields?.gearbox?.semantic?.familyClosed || fields?.gearbox?.semantic?.transmissionTypeClosed) && !fields?.gearbox?.semantic?.hasConflict);
  const drivetrainExactish = isExactish(fields.drivetrain);

  if (gearboxExactish && drivetrainExactish) {
    return "fully_resolved";
  }

  if (fields.gearbox?.resolved || fields.drivetrain?.resolved || fields.gearbox?.displayValue || fields.drivetrain?.displayValue) {
    return "partially_resolved";
  }

  return "ready_for_provisional_maintenance";
}

function determineInternalStatus({ supported, fields, usedInference, inferredEngine, inferredGearbox }) {
  if (!supported) return "invalid";

  const modelClosed = Boolean(fields?.model?.resolved) && Boolean(fields?.modelYear?.resolved);
  const engineResolved = Boolean(fields?.engine?.resolved);
  const engineClosed = isStrongResolved(fields?.engine);
  const gearboxResolved = Boolean(fields?.gearbox?.resolved);
  const gearboxSemanticallyClosed =
    (fields?.gearbox?.semantic?.familyClosed || fields?.gearbox?.semantic?.transmissionTypeClosed) &&
    !fields?.gearbox?.semantic?.hasConflict;
  const gearboxClosed = gearboxResolved && gearboxSemanticallyClosed;
  const drivetrainResolved = Boolean(fields?.drivetrain?.resolved);
  const drivetrainClosed = isStrongResolved(fields?.drivetrain);
  const drivetrainConsistent = isDrivetrainConsistent(fields);
  const inferenceUsed = Boolean(usedInference || inferredEngine || inferredGearbox);

  const fullyResolvedWithoutInference =
    !inferenceUsed &&
    modelClosed &&
    engineResolved &&
    gearboxClosed &&
    drivetrainResolved &&
    drivetrainConsistent;

  if (!modelClosed || !engineResolved) {
    return inferenceUsed ? "partial_inferred" : "needs_manual_input";
  }

  if (fullyResolvedWithoutInference) {
    return "ready_exact";
  }

  const engineInferenceOk = inferredEngine ? isDominantInferenceField(fields?.engine) : engineClosed;
  const gearboxInferenceOk = inferredGearbox ? isDominantInferenceField(fields?.gearbox) : gearboxClosed;
  const noCompetingCandidates =
    !hasCompetingCandidates(fields?.engine) && !hasMeaningfulGearboxCompetition(fields?.gearbox);

  // Sprint 10 contract:
  // inference fallback remains non-plannable even when high-confidence.
  // Therefore it must stay partial_inferred, not ready_high_confidence_inferred.
  if (
    inferenceUsed &&
    modelClosed &&
    engineInferenceOk &&
    gearboxInferenceOk &&
    drivetrainClosed &&
    drivetrainConsistent &&
    noCompetingCandidates
  ) {
    return "partial_inferred";
  }

  if (inferenceUsed) {
    return "partial_inferred";
  }

  return "needs_manual_input";
}

function determineOperationalReadiness(internalStatus) {
  if (internalStatus === "invalid") return "blocked";
  if (internalStatus === "ready_exact") return "quote_ready";
  if (internalStatus === "ready_high_confidence_inferred") return "estimate_ready";
  return "manual_review_required";
}

function toVehicleStatus({ internalStatus }) {
  if (internalStatus === "invalid") return "invalid";
  if (internalStatus === "ready_exact") return "ready_for_planning";
  if (["ready_high_confidence_inferred", "partial_inferred"].includes(internalStatus)) return "partial_inferred";
  return "needs_manual_input";
}

function buildMissingConfirmations(fields, internalStatus) {
  const unresolved = Object.values(fields)
    .filter((item) => !item?.resolved)
    .map((item) => item.field);

  if (internalStatus === "ready_high_confidence_inferred") {
    const inferredFields = [fields?.engine, fields?.gearbox, fields?.drivetrain]
      .filter((field) => usesInferenceSource(field))
      .map((field) => field.field);
    return uniqueStrings([...unresolved, ...inferredFields]);
  }

  return unresolved;
}

export function resolveVehicleConfiguration({ vin, decoded, validation = null, manualInput = {}, manualOverrides = null }) {
  const overrides = manualOverrides || manualInput || {};

  if (!decoded?.supported) {
    const emptyVehicle = {
      vin: String(vin || decoded?.vin || "").trim() || null,
      brand: "Skoda",
      model: null,
      modelYear: null,
      engineType: null,
      engineCode: null,
      displacement: null,
      powerHp: null,
      gearboxType: null,
      gearboxCode: null,
      drivetrain: null,
      fuelType: null,
      confidence: "low",
      source: { vinDecoded: false, manualOverrideUsed: false, fieldsResolved: [] },
    };

    return {
      supported: false,
      status: "invalid",
      canonicalStatus: "invalid",
      internalStatus: "invalid",
      operationalReadiness: "blocked",
      resolutionStatus: "unresolved",
      reason: decoded?.reason || "vin_not_supported",
      vehicle: emptyVehicle,
      canonicalVehicle: emptyVehicle,
      fields: {},
      missingFields: ["model", "modelYear", "engine", "gearbox", "drivetrain"],
      missingConfirmations: ["model", "modelYear", "engine", "gearbox", "drivetrain"],
      warnings: [],
      readyForExactMaintenance: false,
      readyForProvisionalMaintenance: false,
      readinessFlags: {
        estimateReady: false,
        quoteReady: false,
        manualReviewRequired: false,
        blocked: true,
      },
      resolutionSummary: {
        unresolved: true,
        partiallyResolved: false,
        provisionalReady: false,
        fullyResolved: false,
        partialInferred: false,
        internalStatus: "invalid",
        operationalReadiness: "blocked",
      },
    };
  }

  const model = resolveSimpleField({ field: "model", value: decoded?.model, source: "decoded", confidence: "high" });
  const modelYear = decoded?.modelYear
    ? { field: "modelYear", resolved: true, value: String(decoded.modelYear), source: "decoded", confidence: "high", exact: true, reason: null }
    : { field: "modelYear", resolved: false, value: null, source: "missing", confidence: "none", exact: false, reason: "modelYear_missing" };
  const engine = resolveEngine(decoded, overrides);
  const gearbox = resolveGearbox(decoded, overrides);
  const drivetrain = resolveDrivetrain(decoded, overrides);

  let fields = { model, modelYear, engine, gearbox, drivetrain };
  const inferenceResult = applyInferenceFallback({ decoded: { ...decoded, vin: vin || decoded?.vin }, fields, overrides });
  fields = inferenceResult.fields;
  const missingFields = Object.values(fields).filter((item) => !item.resolved).map((item) => item.field);
  const manualWarnings = Object.values(fields).flatMap((field) => field?.warnings || []);
  const baseResolutionStatus = deriveResolutionStatus(fields);
  const usedInference = inferenceResult.inferredEngine || inferenceResult.inferredGearbox;
  const internalStatus = determineInternalStatus({
    supported: true,
    fields,
    usedInference,
    inferredEngine: inferenceResult.inferredEngine,
    inferredGearbox: inferenceResult.inferredGearbox,
  });
  const operationalReadiness = determineOperationalReadiness(internalStatus);
  const canonicalStatus = toVehicleStatus({ internalStatus });
  const missingConfirmations = buildMissingConfirmations(fields, internalStatus);

  const hasMissingConfirmations =
    Array.isArray(missingConfirmations) && missingConfirmations.length > 0;

  const isInferencePath =
    internalStatus === "ready_high_confidence_inferred" ||
    internalStatus === "partial_inferred";

  const resolutionStatus =
    hasMissingConfirmations || operationalReadiness !== "quote_ready" || isInferencePath
      ? "partial_inferred"
      : baseResolutionStatus;

  const readyForExactMaintenance = internalStatus === "ready_exact";
  const readyForProvisionalMaintenance = ["ready_exact", "ready_high_confidence_inferred"].includes(internalStatus) || resolutionStatus !== "unresolved";

  const canonicalVehicle = buildCanonicalVehicle({ decoded: { ...decoded, vin: vin || decoded?.vin }, fields, manualOverrides: overrides });

  return {
    supported: true,
    status: canonicalStatus,
    canonicalStatus,
    internalStatus,
    operationalReadiness,
    resolutionStatus,
    vehicle: canonicalVehicle,
    canonicalVehicle,
    fields,
    missingFields,
    missingConfirmations,
    warnings: uniqueStrings([...(validation?.warnings || []), ...manualWarnings]),
    inferredEngine: inferenceResult.inferredEngine,
    inferredGearbox: inferenceResult.inferredGearbox,
    inference: inferenceResult.inference,
    readyForQuote: operationalReadiness === "quote_ready",
    readyForExactMaintenance,
    readyForProvisionalMaintenance,
    readinessFlags: {
      estimateReady: operationalReadiness === "estimate_ready",
      quoteReady: operationalReadiness === "quote_ready",
      manualReviewRequired: operationalReadiness === "manual_review_required",
      blocked: operationalReadiness === "blocked",
    },
    resolutionSummary: {
      unresolved: resolutionStatus === "unresolved",
      partiallyResolved: resolutionStatus === "partially_resolved" || resolutionStatus === "partial_inferred",
      provisionalReady: readyForProvisionalMaintenance,
      fullyResolved: resolutionStatus === "fully_resolved",
      partialInferred: resolutionStatus === "partial_inferred",
      internalStatus,
      operationalReadiness,
    },
  };
}

export function resolveVehicleForMaintenance(args) {
  return resolveVehicleConfiguration(args);
}

export default resolveVehicleForMaintenance;