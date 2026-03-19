function isPositiveNumber(value) {
  return typeof value === "number" && Number.isFinite(value) && value > 0;
}

function normalizeString(value) {
  return typeof value === "string" ? value.trim() : "";
}

function unique(values) {
  return [...new Set((values || []).filter(Boolean))];
}

function toCanonicalStatus({ vehicleStatus, planningStatus }) {
  if (vehicleStatus === "invalid") return "invalid";
  if (planningStatus === "invalid") return "invalid";
  if (vehicleStatus !== "ready_for_planning") return "needs_manual_input";
  if (planningStatus !== "ready_for_planning") return "needs_manual_input";
  return "ready_for_planning";
}

function toResolutionStatus({ modelResolved, modelYearResolved, engineResolved, gearboxResolved, drivetrainResolved }) {
  if (!modelResolved || !modelYearResolved || !engineResolved) return "unresolved";
  if (gearboxResolved && drivetrainResolved) return "fully_resolved";
  if (gearboxResolved || drivetrainResolved) return "partially_resolved";
  return "ready_for_provisional_maintenance";
}

function getEngineResolution(decoded) {
  const possibleCodes = unique(decoded?.enrichment?.possibleEngineCodes || []);
  const selectedCode = decoded?.enrichment?.selectedEngine?.code || null;
  const exactCode = decoded?.enrichment?.exactVinMatch?.engineCode || null;
  const uiCode = normalizeString(decoded?.motorKod);

  if (exactCode) {
    return {
      field: "engine",
      resolved: true,
      exact: true,
      confidence: "exact",
      value: exactCode,
      source: "vin_training_dataset_exact",
      candidates: [exactCode],
      reason: null,
    };
  }

  if (selectedCode) {
    return {
      field: "engine",
      resolved: true,
      exact: possibleCodes.length <= 1,
      confidence: possibleCodes.length <= 1 ? "high" : "medium",
      value: selectedCode,
      source: decoded?.enrichment?.engineSource || "selected_engine_candidate",
      candidates: unique([selectedCode, ...possibleCodes]),
      reason: null,
    };
  }

  if (possibleCodes.length === 1) {
    return {
      field: "engine",
      resolved: true,
      exact: false,
      confidence: "high",
      value: possibleCodes[0],
      source: decoded?.enrichment?.engineSource || "single_candidate",
      candidates: possibleCodes,
      reason: null,
    };
  }

  if (possibleCodes.length > 1 || uiCode.includes(",")) {
    return {
      field: "engine",
      resolved: false,
      exact: false,
      confidence: "low",
      value: null,
      source: decoded?.enrichment?.engineSource || "ambiguous",
      candidates: possibleCodes,
      reason: "engine_ambiguous",
    };
  }

  return {
    field: "engine",
    resolved: false,
    exact: false,
    confidence: "low",
    value: null,
    source: "missing",
    candidates: [],
    reason: "engine_missing",
  };
}

function getGearboxResolution(decoded) {
  const possibleCodes = unique(decoded?.enrichment?.possibleGearboxCodes || []);
  const selectedCode = decoded?.enrichment?.selectedGearbox?.code || null;
  const exactCode = decoded?.enrichment?.exactVinMatch?.transmissionCode || null;
  const uiGearboxCode = normalizeString(decoded?.gearboxCode);
  const inferredType = normalizeString(decoded?.menjac);

  if (exactCode) {
    return {
      field: "gearbox",
      resolved: true,
      exact: true,
      confidence: "exact",
      value: exactCode,
      type: inferredType || decoded?.enrichment?.selectedGearbox?.type || null,
      source: "vin_training_dataset_exact",
      candidates: [exactCode],
      reason: null,
    };
  }

  if (selectedCode) {
    return {
      field: "gearbox",
      resolved: true,
      exact: possibleCodes.length <= 1,
      confidence: possibleCodes.length <= 1 ? "high" : "medium",
      value: selectedCode,
      type: inferredType || decoded?.enrichment?.selectedGearbox?.type || null,
      source: decoded?.enrichment?.gearboxSource || "selected_gearbox_candidate",
      candidates: unique([selectedCode, ...possibleCodes]),
      reason: null,
    };
  }

  if (possibleCodes.length === 1) {
    return {
      field: "gearbox",
      resolved: true,
      exact: false,
      confidence: "high",
      value: possibleCodes[0],
      type: inferredType || null,
      source: decoded?.enrichment?.gearboxSource || "single_candidate",
      candidates: possibleCodes,
      reason: null,
    };
  }

  if (possibleCodes.length > 1 || uiGearboxCode.includes(",")) {
    return {
      field: "gearbox",
      resolved: false,
      exact: false,
      confidence: "low",
      value: null,
      type: inferredType || null,
      source: decoded?.enrichment?.gearboxSource || "ambiguous",
      candidates: possibleCodes,
      reason: "gearbox_ambiguous",
    };
  }

  return {
    field: "gearbox",
    resolved: false,
    exact: false,
    confidence: "low",
    value: null,
    type: inferredType || null,
    source: "missing",
    candidates: [],
    reason: "gearbox_missing",
  };
}

function getDrivetrainResolution(decoded) {
  const raw = normalizeString(decoded?.drivetrain).toUpperCase();

  if (raw === "FWD" || raw === "AWD") {
    return {
      field: "drivetrain",
      resolved: true,
      exact: true,
      confidence: "high",
      value: raw,
      source: decoded?.gearboxCodeSource || decoded?.menjacSource || "decoded",
      reason: null,
    };
  }

  if (raw === "2WD") {
    return {
      field: "drivetrain",
      resolved: false,
      exact: false,
      confidence: "low",
      value: null,
      source: "normalized_2wd",
      reason: "drivetrain_needs_fwd_confirmation",
    };
  }

  return {
    field: "drivetrain",
    resolved: false,
    exact: false,
    confidence: "low",
    value: null,
    source: "missing",
    reason: "drivetrain_missing",
  };
}

function getModelResolution(decoded) {
  const model = normalizeString(decoded?.model);
  const modelYear = decoded?.modelYear;

  return {
    modelResolved: Boolean(model && model !== "N/A"),
    model,
    modelYearResolved: Number.isInteger(modelYear) && modelYear >= 2000,
    modelYear,
  };
}

function buildVehicleGate(decoded) {
  const model = getModelResolution(decoded);
  const engine = getEngineResolution(decoded);
  const gearbox = getGearboxResolution(decoded);
  const drivetrain = getDrivetrainResolution(decoded);

  const missingFields = [];
  const reasons = [];
  const warnings = [];

  if (!model.modelResolved) {
    missingFields.push("model");
    reasons.push("model_missing");
  }
  if (!model.modelYearResolved) {
    missingFields.push("modelYear");
    reasons.push("model_year_missing");
  }
  if (!engine.resolved) {
    missingFields.push("engine");
    reasons.push(engine.reason || "engine_missing");
  }
  if (!gearbox.resolved) {
    missingFields.push("gearbox");
    reasons.push(gearbox.reason || "gearbox_missing");
  }
  if (!drivetrain.resolved) {
    missingFields.push("drivetrain");
    reasons.push(drivetrain.reason || "drivetrain_missing");
  }

  if (engine.resolved && !engine.exact) {
    warnings.push("engine_not_exact");
  }
  if (gearbox.resolved && !gearbox.exact) {
    warnings.push("gearbox_not_exact");
  }

  let status = "ready_for_planning";

  if (!model.modelResolved || !model.modelYearResolved || !engine.resolved) {
    status = "needs_manual_input";
  } else if (!gearbox.resolved || !drivetrain.resolved) {
    status = "needs_manual_input";
  }

  const manualInputPrompts = [];
  if (missingFields.includes("gearbox")) {
    manualInputPrompts.push("Potvrdi tip menjača ili tačan kod menjača.");
  }
  if (missingFields.includes("drivetrain")) {
    manualInputPrompts.push("Potvrdi pogon: FWD ili AWD.");
  }
  if (missingFields.includes("engine")) {
    manualInputPrompts.push("Potvrdi tačan kod motora.");
  }

  const resolutionStatus = toResolutionStatus({
    modelResolved: model.modelResolved,
    modelYearResolved: model.modelYearResolved,
    engineResolved: engine.resolved,
    gearboxResolved: gearbox.resolved,
    drivetrainResolved: drivetrain.resolved,
  });

  return {
    status,
    canonicalStatus: status,
    resolutionStatus,
    missingFields,
    reasons,
    warnings,
    manualInputPrompts,
    resolvedVehicle: {
      model: model.model || null,
      modelYear: model.modelYear || null,
      engine,
      gearbox,
      drivetrain,
    },
    // legacy alias
    resolved: {
      model: model.model,
      modelYear: model.modelYear,
      engine,
      gearbox,
      drivetrain,
    },
  };
}

function buildPlanningGate(inputs = {}) {
  const missingFields = [];
  const reasons = [];

  const hasServiceRegime = Boolean(normalizeString(inputs.serviceRegime));
  const hasUsageProfile = Boolean(normalizeString(inputs.usageProfile));
  const validPlannedKm = isPositiveNumber(inputs.plannedKm);
  const validContractMonths = isPositiveNumber(inputs.contractMonths);

  if (!hasServiceRegime) {
    missingFields.push("serviceRegime");
    reasons.push("service_regime_missing");
  }
  if (!hasUsageProfile) {
    missingFields.push("usageProfile");
    reasons.push("usage_profile_missing");
  }
  if (!validPlannedKm) {
    missingFields.push("plannedKm");
    reasons.push("planned_km_invalid");
  }
  if (!validContractMonths) {
    missingFields.push("contractMonths");
    reasons.push("contract_months_invalid");
  }

  const hasInvalidNumericInput = !validPlannedKm || !validContractMonths;
  const status = hasInvalidNumericInput
    ? "invalid"
    : missingFields.length === 0
    ? "ready_for_planning"
    : "needs_manual_input";

  return {
    status,
    canonicalStatus: status,
    missingFields,
    reasons,
    normalized: {
      serviceRegime: hasServiceRegime ? normalizeString(inputs.serviceRegime) : null,
      usageProfile: hasUsageProfile ? normalizeString(inputs.usageProfile) : null,
      plannedKm: validPlannedKm ? inputs.plannedKm : null,
      contractMonths: validContractMonths ? inputs.contractMonths : null,
    },
  };
}


function toEnhancedStatus(vehicleGate, planningGate) {
  if (vehicleGate.status === "invalid" || planningGate.status === "invalid") {
    return "invalid";
  }

  if (vehicleGate.status !== "ready_for_planning" || planningGate.status !== "ready_for_planning") {
    return "needs_manual_input";
  }

  const engineExact = vehicleGate.resolvedVehicle.engine?.exact === true;
  const gearboxExact = vehicleGate.resolvedVehicle.gearbox?.exact === true;
  const drivetrainResolved = vehicleGate.resolvedVehicle.drivetrain?.resolved === true;

  if (engineExact && gearboxExact && drivetrainResolved) {
    return "ready_exact";
  }

  const engineResolved = vehicleGate.resolvedVehicle.engine?.resolved === true;
  const gearboxResolved = vehicleGate.resolvedVehicle.gearbox?.resolved === true;

  if (engineResolved && gearboxResolved && drivetrainResolved) {
    return "ready_high_confidence_inferred";
  }

  return "partial_inferred";
}

export function validateVinForMaintenance(decoded, planningInputs = {}) {
  const vehicleGate = buildVehicleGate(decoded);
  const planningGate = buildPlanningGate(planningInputs);

  const status = toCanonicalStatus({
    vehicleStatus: vehicleGate.status,
    planningStatus: planningGate.status,
  });
  const enhancedStatus = toEnhancedStatus(vehicleGate, planningGate);

  const missingFields = unique([
    ...vehicleGate.missingFields,
    ...planningGate.missingFields,
  ]);

  const warnings = unique([
    ...(vehicleGate.warnings || []),
  ]);

  const resolvedVehicle = {
    model: vehicleGate.resolvedVehicle.model,
    modelYear: vehicleGate.resolvedVehicle.modelYear,
    engine: vehicleGate.resolvedVehicle.engine,
    gearbox: vehicleGate.resolvedVehicle.gearbox,
    drivetrain: vehicleGate.resolvedVehicle.drivetrain,
  };

  return {
    status,
    resolutionStatus: vehicleGate.resolutionStatus,
    missingFields,
    warnings,
    resolvedVehicle,
    vehicleGate,
    planningGate,
    canBuildQuotePlan: enhancedStatus === "ready_exact",
    canBuildEstimatePlan:
      enhancedStatus === "ready_high_confidence_inferred" ||
      enhancedStatus === "partial_inferred",
    canBuildExactPlan:
      vehicleGate.status === "ready_for_planning" &&
      planningGate.status === "ready_for_planning" &&
      vehicleGate.resolvedVehicle.engine?.exact === true &&
      vehicleGate.resolvedVehicle.gearbox?.exact === true &&
      vehicleGate.resolvedVehicle.drivetrain?.resolved === true,
    canBuildProvisionalPlan:
      vehicleGate.status === "ready_for_planning" && planningGate.status === "ready_for_planning",
    // legacy alias kept for backward compatibility during Sprint 1
    overallStatus: status,
    overallEnhancedStatus: enhancedStatus,
  };
}
