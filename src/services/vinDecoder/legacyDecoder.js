import vinDatabase from "../../data/vin_database.json" with { type: 'json' };
import engineCodesDb from "../../data/engine_codes.json" with { type: 'json' };
import gearboxCodesDb from "../../data/gearbox_codes.json" with { type: 'json' };
import vinTrainingDataset from "../../data/vin_training_dataset.json" with { type: 'json' };
import engineCodesMaster from "../../data/engine_codes_master.json" with { type: 'json' };
import gearboxCodesMaster from "../../data/gearbox_codes_master.json" with { type: 'json' };
import vinPatternRules from "../../data/vin_pattern_rules.json" with { type: 'json' };

function getBodyCode(vin) {
  if (!vin || vin.length < 6) return null;
  return vin.slice(3, 6);
}

function getPlatformCode(vin) {
  if (!vin || vin.length < 8) return null;
  return vin.slice(6, 8);
}

function decodeModelYearFromVinCode(code) {
  const map = {
    L: 2020,
    M: 2021,
    N: 2022,
    P: 2023,
    R: 2024,
    S: 2025,
    T: 2026,
    V: 2027,
    W: 2028,
    X: 2029,
    Y: 2030,
  };

  return map[code] || null;
}

function decodePlantFromVinCode(code) {
  const map = {
    M: "Mlada Boleslav",
    K: "Kvasiny",
    V: "Vrchlabi",
    4: "Poznan",
    Y: "Mlada Boleslav",
  };

  return map[code] || null;
}

function decodeEngineFamilyFromVinCode(code) {
  const map = {
    A: "1.0 TSI",
    B: "1.2 TSI",
    C: "1.5 TSI",
    D: "2.0 TDI",
    E: "1.6 TDI",
    F: "2.0 TSI",
    G: "1.4 TSI",
    H: "1.8 TSI",
  };

  return map[code] || null;
}

function getMasterGearboxClassification(code) {
  if (!code) return null;

  const upper = String(code).toUpperCase();
  const master = gearboxCodesMaster?.[upper] || null;

  if (!master) return null;

  return {
    code: upper,
    type: master.type || null,
    family: master.family || null,
    drivetrain: master.drivetrain || null,
    master,
  };
}

function inferGearboxFromCode(code) {
  const classified = getMasterGearboxClassification(code);

  if (classified?.type === "manual") return "Manual";
  if (classified?.type === "DSG") return "DSG";
  if (classified?.type === "automatic") return "Automatic";
  if (classified?.type === "ev") return "EV";

  return null;
}


function isPatternRuleCompatible(result, rule) {
  if (!rule) return false;

  const modelYear = result?.model_year?.year || null;
  if (!modelYear) return true;

  if (Array.isArray(rule.modelYears) && rule.modelYears.length > 0) {
    return rule.modelYears.includes(modelYear);
  }

  return true;
}

function decodeVin(vin) {
  const db = vinDatabase;

  const result = {
    vin: vin || "",
    valid: true,
    supported: true,
    reason: null,
    validation_errors: [],

    wmi: {
      code: null,
      manufacturer: null,
      country_hint: null,
    },

    model_info: {
      raw_code: null,
      resolved_ruleset: null,
      name: null,
      generation: null,
    },

    body: {
      code: null,
      style: null,
      normalized_style: null,
      steering: null,
      drivetrain: null,
    },

    engine: {
      code: null,
      description: null,
      fuel_type: null,
      displacement_l: null,
      power_kw: [],
      power_kw_display: null,
      unit_code: null,
      family: null,
    },

    restraint_system: {
      code: null,
      description: null,
    },

    model_year: {
      code: null,
      year: null,
    },

    plant: {
      code: null,
      name: null,
    },

    serial_number: null,

    special_flags: {
      n1: false,
      motorsport: false,
      ambiguous_model: false,
    },

    confidence: "high",
    warnings: [],
    possible_matches: [],
    patternMatch: false,

    vin_summary: {
      manufacturer: null,
      country_hint: null,
      model: null,
      generation: null,
      body_style: null,
      steering: null,
      drivetrain: null,
      fuel_type: null,
      power_kw: null,
      model_year: null,
      plant: null,
    },

    vin_codes: {
      wmi: null,
      body_code: null,
      engine_code: null,
      restraint_code: null,
      model_code: null,
      year_code: null,
      plant_code: null,
      serial_number: null,
    },

    enrichment: {
      exactVinMatch: null,
      patternRule: null,
      possibleEngineCodes: [],
      engineCandidates: [],
      possibleGearboxCodes: [],
      gearboxTechCandidates: [],
      engineSource: "not_enriched",
      gearboxSource: "not_enriched",
      selectedEngine: null,
      selectedGearbox: null,
      masterEngine: null,
      masterGearbox: null,
      source: "legacy_vin_decoder",
    },

    marka: "Skoda",
    model: null,
    motorKod: null,
    motor: null,
    menjac: null,
    menjacSource: "inferred",
    modelYear: null,
    drivetrain: null,
    gearboxCode: "N/A",
    gearboxCodeSource: "not_available_from_vin",
    fuelType: null,
    oilCapacity: "N/A",
    oilSpec: "N/A",
    oilSae: "N/A",
    hourlyRate: 5500,
    candidates: [],
  };

  if (typeof vin !== "string") {
    result.valid = false;
    result.supported = false;
    result.validation_errors.push("VIN must be a string.");
    result.reason = "VIN mora biti tekst.";
    result.confidence = "low";
    return result;
  }

  const cleanVin = vin.trim().toUpperCase();
  result.vin = cleanVin;

  if (cleanVin.length !== 17) {
    result.valid = false;
    result.supported = false;
    result.validation_errors.push("VIN must be exactly 17 characters long.");
  }

  if (/[^A-HJ-NPR-Z0-9]/.test(cleanVin)) {
    result.valid = false;
    result.supported = false;
    result.validation_errors.push("VIN contains invalid characters.");
  }

  if (!result.valid) {
    result.confidence = "low";
    result.reason =
      result.validation_errors.join(", ") || "VIN nije podržan";
    return result;
  }

  const wmi = cleanVin.slice(0, 3);
  const shortBodyCode = cleanVin[3];
  const engineCodeFromVin = cleanVin[4];
  const airbagCode = cleanVin[5];
  const modelCode = cleanVin.slice(6, 8);
  const yearCode = cleanVin[9];
  const plantCode = cleanVin[10];
  const serialNumber = cleanVin.slice(11, 17);
  const fullBodyCode = getBodyCode(cleanVin);
  const platformCode = getPlatformCode(cleanVin);
  const engineFamilyFromVin = decodeEngineFamilyFromVinCode(engineCodeFromVin);

  result.wmi.code = wmi;
  result.body.code = shortBodyCode;
  result.engine.code = engineCodeFromVin;
  result.restraint_system.code = airbagCode;
  result.model_info.raw_code = modelCode;
  result.model_year.code = yearCode;
  result.plant.code = plantCode;
  result.serial_number = serialNumber;

  result.vin_codes.wmi = wmi;
  result.vin_codes.body_code = shortBodyCode;
  result.vin_codes.engine_code = engineCodeFromVin;
  result.vin_codes.restraint_code = airbagCode;
  result.vin_codes.model_code = modelCode;
  result.vin_codes.year_code = yearCode;
  result.vin_codes.plant_code = plantCode;
  result.vin_codes.serial_number = serialNumber;

  result.wmi.manufacturer = resolveManufacturer(wmi);
  result.wmi.country_hint = resolveCountryHint(wmi);

  if (engineFamilyFromVin) {
    result.engine.family = engineFamilyFromVin;
  }

  applyLegacyModelRules(result, db, {
    wmi,
    shortBodyCode,
    engineCodeFromVin,
    airbagCode,
    modelCode,
    yearCode,
    plantCode,
  });

  enrichWithExactVinDataset(result);

  if (!isExactDatasetMatch(result)) {
    const patternKey = `${fullBodyCode}|${platformCode}`;
    const rawPatternRule = vinPatternRules?.[patternKey] || null;
    const patternRule = isPatternRuleCompatible(result, rawPatternRule) ? rawPatternRule : null;
    applyVinPatternRule(result, patternRule);
  }

  if (!isExactDatasetMatch(result)) {
    enrichWithEngineCodes(result);
    enrichWithGearboxCodes(result);
  }

  finalizeFromMasters(result);
  finalizeUiFields(result);

  if (!result.model_info.name) {
    result.supported = false;
    result.reason = "VIN nije dovoljno poznat za tačan model.";
    downgradeConfidence(result, "low");
  }

  result.reason = result.valid
    ? result.reason
    : (result.validation_errors.join(", ") || "VIN nije podržan");

  return result;
}

function applyLegacyModelRules(result, db, context) {
  const {
    wmi,
    shortBodyCode,
    engineCodeFromVin,
    airbagCode,
    modelCode,
    yearCode,
    plantCode,
  } = context;

  const resolvedModelId = resolveModelRuleset(modelCode, shortBodyCode, db, result);

  if (!resolvedModelId) {
    result.warnings.push(`Legacy ruleset not resolved for model code ${modelCode}.`);
    downgradeConfidence(result, "medium");
    applyBasicVinFallbacks(result, context);
    refreshVinSummary(result);
    return;
  }

  const modelRules = db?.models?.[resolvedModelId];
  if (!modelRules) {
    result.warnings.push(`Ruleset not found: ${resolvedModelId}.`);
    downgradeConfidence(result, "medium");
    applyBasicVinFallbacks(result, context);
    refreshVinSummary(result);
    return;
  }

  result.model_info.resolved_ruleset = resolvedModelId;
  result.model_info.name = modelRules.name || result.model_info.name;
  result.model_info.generation = modelRules.generation || result.model_info.generation;

  if (Array.isArray(modelRules.supported_wmi) && !modelRules.supported_wmi.includes(wmi)) {
    result.warnings.push(`WMI ${wmi} is not listed for ruleset ${resolvedModelId}.`);
    downgradeConfidence(result, "medium");
  }

  const bodyData = modelRules.body_map?.[shortBodyCode];
  if (bodyData) {
    result.body.style = bodyData.style ?? result.body.style ?? null;
    result.body.normalized_style =
      db.normalization?.body_styles?.[bodyData.style] ??
      result.body.normalized_style ??
      null;
    result.body.steering = bodyData.steering ?? result.body.steering ?? null;
    result.body.drivetrain =
      bodyData.drivetrain
        ? db.normalization?.drivetrain?.[bodyData.drivetrain] ?? bodyData.drivetrain
        : result.body.drivetrain;

    if (bodyData.special_flags?.n1) {
      result.special_flags.n1 = true;
    }

    if (bodyData.special_flags?.motorsport) {
      result.special_flags.motorsport = true;
    }
  }

  const engineData = modelRules.engine_map?.[engineCodeFromVin];
  if (engineData) {
    result.engine.description = engineData.description ?? result.engine.description ?? null;
    result.engine.fuel_type = engineData.fuel_type ?? result.engine.fuel_type ?? null;
    result.engine.displacement_l = engineData.displacement_l ?? result.engine.displacement_l ?? null;
    result.engine.power_kw = Array.isArray(engineData.power_kw)
      ? engineData.power_kw
      : result.engine.power_kw;
    result.engine.power_kw_display = result.engine.power_kw.join("/") || result.engine.power_kw_display || null;

    if (engineData.special_flags?.motorsport) {
      result.special_flags.motorsport = true;
    }

    if (Array.isArray(result.engine.power_kw) && result.engine.power_kw.length > 1) {
      result.warnings.push("Engine code maps to multiple possible power outputs.");
      downgradeConfidence(result, "medium");
    }
  }

  const restraintData = modelRules.airbag_map?.[airbagCode];
  if (restraintData) {
    result.restraint_system.description = restraintData;
  }

  const yearFromRules = modelRules.year_map?.[yearCode];
  const yearFromVinPattern = decodeModelYearFromVinCode(yearCode);

  if (yearFromRules) {
    result.model_year.year = yearFromRules;
  } else if (yearFromVinPattern) {
    result.model_year.year = yearFromVinPattern;
  }

  const plantFromRules = modelRules.plant_map?.[plantCode];
  const plantFromVinPattern = decodePlantFromVinCode(plantCode);

  if (plantFromRules) {
    result.plant.name = plantFromRules;
  } else if (plantFromVinPattern) {
    result.plant.name = plantFromVinPattern;
  }

  refreshVinSummary(result);
}

function applyBasicVinFallbacks(result, context) {
  const { yearCode, plantCode } = context;

  if (!result.model_year.year) {
    result.model_year.year = decodeModelYearFromVinCode(yearCode);
  }

  if (!result.plant.name) {
    result.plant.name = decodePlantFromVinCode(plantCode);
  }
}

function enrichWithExactVinDataset(result) {
  const exact = findExactVinMatch(result.vin);
  if (!exact) return;

  result.enrichment.exactVinMatch = exact;
  result.enrichment.source = "vin_training_dataset_exact";
  result.confidence = "exact";

  if (exact.model) {
    result.model_info.name = exact.model;
  }

  if (exact.modelYear != null) {
    result.model_year.year = exact.modelYear;
  }

  if (exact.engineCode) {
    result.enrichment.possibleEngineCodes = [exact.engineCode];
    result.enrichment.engineSource = "vin_training_dataset_exact";
  }

  if (exact.transmissionCode) {
    result.enrichment.possibleGearboxCodes = [exact.transmissionCode];
    result.enrichment.gearboxSource = "vin_training_dataset_exact";
    result.gearboxCode = exact.transmissionCode;
    result.gearboxCodeSource = "exact_vin_training_dataset";
    result.menjacSource = "vin_training_dataset_exact";
  }

  if (exact.drivetrain) {
    result.body.drivetrain = exact.drivetrain;
  }

  if (exact.serviceRegime) {
    result.serviceRegime = exact.serviceRegime;
  }

  if (exact.serviceIndicator) {
    result.serviceIndicator = exact.serviceIndicator;
  }

  if (exact.engineUnitCode || exact.engineUnit) {
    result.engine.unit_code = exact.engineUnitCode || exact.engineUnit;
  }

  if (exact.fuel) {
    result.engine.fuel_type = exact.fuel;
  }

  if (exact.powerKw != null) {
    result.engine.power_kw = [exact.powerKw];
    result.engine.power_kw_display = String(exact.powerKw);
  }

  const masterEngine = exact.engineCode ? engineCodesMaster?.[exact.engineCode] : null;
  if (masterEngine) {
    result.enrichment.masterEngine = masterEngine;
    result.enrichment.selectedEngine = {
      code: exact.engineCode,
      ...masterEngine,
      kw: masterEngine.powerKw ?? null,
      fuel_type: masterEngine.fuel ?? null,
      displacement_l: masterEngine.displacementL ?? null,
      oil_capacity_l: masterEngine.oilCapacityL ?? null,
      oil_spec: masterEngine.oilSpec ?? null,
      oil_viscosity: masterEngine.oilViscosity ?? null,
      notes: masterEngine.description ?? null,
    };

    result.engine.family = masterEngine.family ?? result.engine.family ?? null;
    result.engine.unit_code = masterEngine.engineUnit ?? result.engine.unit_code ?? null;
    result.engine.description = masterEngine.description ?? result.engine.description ?? null;
    result.engine.fuel_type = masterEngine.fuel ?? result.engine.fuel_type ?? null;
    result.engine.displacement_l = masterEngine.displacementL ?? result.engine.displacement_l ?? null;

    if (masterEngine.powerKw != null) {
      result.engine.power_kw = [masterEngine.powerKw];
      result.engine.power_kw_display = String(masterEngine.powerKw);
    }

    if (masterEngine.oilCapacityL != null) {
      result.oilCapacity = `${masterEngine.oilCapacityL} L`;
    }
    if (masterEngine.oilSpec) {
      result.oilSpec = masterEngine.oilSpec;
    }
    if (masterEngine.oilViscosity) {
      result.oilSae = masterEngine.oilViscosity;
    }
  }

  const masterGearbox = exact.transmissionCode
    ? gearboxCodesMaster?.[exact.transmissionCode]
    : null;

  if (masterGearbox) {
    result.enrichment.masterGearbox = masterGearbox;
    result.enrichment.selectedGearbox = {
      code: exact.transmissionCode,
      ...masterGearbox,
    };

    if (masterGearbox.drivetrain) {
      result.body.drivetrain = masterGearbox.drivetrain;
    }
  }

  refreshVinSummary(result);
}

function applyVinPatternRule(result, rule) {
  if (!rule) return;

  result.patternMatch = true;
  result.enrichment.patternRule = rule;
  if (result.enrichment.source !== "vin_training_dataset_exact") {
    result.enrichment.source = "vin_pattern_rule";
  }

  if ((!result.model_info.name || result.model_info.name === "Unknown") && rule.models?.length === 1) {
    result.model_info.name = rule.models[0];
  }

  if (!result.model_year.year && rule.modelYears?.length === 1) {
    result.model_year.year = rule.modelYears[0];
  }

  if (Array.isArray(rule.engineCodes) && rule.engineCodes.length > 0) {
    result.enrichment.possibleEngineCodes = unique([
      ...result.enrichment.possibleEngineCodes,
      ...rule.engineCodes,
    ]);
    if (result.enrichment.engineSource === "not_enriched") {
      result.enrichment.engineSource = "vin_pattern_rule";
    }
  }

  if (Array.isArray(rule.gearboxCodes) && rule.gearboxCodes.length > 0) {
    result.enrichment.possibleGearboxCodes = unique([
      ...result.enrichment.possibleGearboxCodes,
      ...rule.gearboxCodes,
    ]);
    if (result.enrichment.gearboxSource === "not_enriched") {
      result.enrichment.gearboxSource = "vin_pattern_rule";
    }
  }

  if ((!result.body.drivetrain || result.body.drivetrain === "N/A") && rule.drivetrains?.length === 1) {
    result.body.drivetrain = rule.drivetrains[0];
  }

  if ((!result.engine.fuel_type || result.engine.fuel_type === "N/A") && rule.fuels?.length === 1) {
    result.engine.fuel_type = rule.fuels[0];
  }

  if (rule.engineCodes?.length === 1 && !result.enrichment.selectedEngine) {
    const code = rule.engineCodes[0];
    const master = engineCodesMaster?.[code] || null;
    if (master) {
      result.enrichment.masterEngine = master;
      result.enrichment.selectedEngine = {
        code,
        ...master,
        kw: master.powerKw ?? null,
        fuel_type: master.fuel ?? null,
        displacement_l: master.displacementL ?? null,
        oil_capacity_l: master.oilCapacityL ?? null,
        oil_spec: master.oilSpec ?? null,
        oil_viscosity: master.oilViscosity ?? null,
        notes: master.description ?? null,
      };

      result.engine.description = master.description ?? result.engine.description ?? null;
      result.engine.fuel_type = master.fuel ?? result.engine.fuel_type ?? null;
      result.engine.displacement_l = master.displacementL ?? result.engine.displacement_l ?? null;
      result.engine.family = master.family ?? result.engine.family ?? null;
      result.engine.unit_code = master.engineUnit ?? result.engine.unit_code ?? null;

      if (master.powerKw != null) {
        result.engine.power_kw = [master.powerKw];
        result.engine.power_kw_display = String(master.powerKw);
      }
    }
  }

  if (rule.gearboxCodes?.length === 1 && !result.enrichment.selectedGearbox) {
    const code = rule.gearboxCodes[0];
    const master = gearboxCodesMaster?.[code] || null;
    if (master) {
      result.enrichment.masterGearbox = master;
      result.enrichment.selectedGearbox = { code, ...master };

      if (master.drivetrain && !result.body.drivetrain) {
        result.body.drivetrain = master.drivetrain;
      }
    }
  }

  if (rule.engineCodes?.length === 1 && rule.gearboxCodes?.length === 1) {
    if (result.confidence !== "exact") {
      result.confidence = "high";
    }
  } else {
    downgradeConfidence(result, "medium");
  }

  refreshVinSummary(result);
}

function isExactDatasetMatch(result) {
  return result?.enrichment?.source === "vin_training_dataset_exact";
}

function findExactVinMatch(vin) {
  if (!Array.isArray(vinTrainingDataset)) return null;
  return vinTrainingDataset.find((item) => item?.vin === vin) || null;
}

function enrichWithEngineCodes(result) {
  if (isExactDatasetMatch(result) && result.enrichment?.exactVinMatch?.engineCode) {
    return;
  }

  const modelKey = toEnrichmentModelKey(result.model_info.name);
  if (!modelKey) {
    return;
  }

  const exactEngineCodes = result.enrichment?.exactVinMatch?.engineCode
    ? [result.enrichment.exactVinMatch.engineCode]
    : [];

  const seedCodes = unique([
    ...exactEngineCodes,
    ...result.enrichment.possibleEngineCodes,
  ]);

  const engines = engineCodesDb?.engines || {};
  const modelYear = result.model_year?.year || null;
  const displacement = result.engine?.displacement_l ?? null;
  const fuelType = result.engine?.fuel_type ?? null;
  const powers = Array.isArray(result.engine?.power_kw) ? result.engine.power_kw : [];

  const candidates = Object.values(engines)
    .filter((item) =>
      isEngineCandidateCompatible(
        item,
        modelKey,
        modelYear,
        fuelType,
        displacement,
        powers,
        seedCodes
      )
    )
    .map((item) => {
      const matchedApplications = filterMatchingApplications(item, modelKey, modelYear);
      return {
        ...item,
        matchedApplications,
        _score: scoreEngineCandidate(item, matchedApplications, seedCodes),
      };
    })
    .sort(compareEngineCandidates);

  result.enrichment.engineCandidates = candidates;

  const combinedCodes = unique([
    ...seedCodes,
    ...candidates.map((item) => item.code),
  ]);

  result.enrichment.possibleEngineCodes = combinedCodes;

  if (combinedCodes.length > 0 && result.enrichment.engineSource === "not_enriched") {
    result.enrichment.engineSource =
      exactEngineCodes.length > 0
        ? "vin_training_dataset_exact"
        : seedCodes.length > 0
          ? "vin_pattern_rule"
          : "enriched_from_engine_master";
  }

  if (combinedCodes.length > 1 && exactEngineCodes.length === 0 && seedCodes.length === 0) {
    result.warnings.push("Multiple possible ETKA engine codes matched the VIN profile.");
    downgradeConfidence(result, "medium");
  }

  if (combinedCodes.length === 0) {
    return;
  }

  const preferredCode =
    exactEngineCodes[0] ||
    (seedCodes.length === 1 ? seedCodes[0] : null);

  if (preferredCode) {
    const preferredCandidate =
      candidates.find((item) => item.code === preferredCode) ||
      buildMasterEngineCandidate(preferredCode);

    if (preferredCandidate) {
      applySelectedEngineCandidate(result, preferredCandidate);
    }
    return;
  }

  const bestCandidate = candidates[0] || null;
  if (bestCandidate && shouldAutoSelectEngineCandidate(bestCandidate, candidates)) {
    applySelectedEngineCandidate(result, bestCandidate);
    result.enrichment.possibleEngineCodes = [bestCandidate.code];
    return;
  }

  const commonOil = getCommonOilData(candidates);
  if (commonOil.capacity_l !== null && result.oilCapacity === "N/A") {
    result.oilCapacity = `${commonOil.capacity_l} L`;
  }
  if (commonOil.spec && result.oilSpec === "N/A") {
    result.oilSpec = commonOil.spec;
  }
  if (commonOil.viscosity && result.oilSae === "N/A") {
    result.oilSae = commonOil.viscosity;
  }
}

function enrichWithGearboxCodes(result) {
  if (isExactDatasetMatch(result) && result.enrichment?.exactVinMatch?.transmissionCode) {
    return;
  }

  const exactTransmissionCode = result.enrichment?.exactVinMatch?.transmissionCode || null;
  const seedCodes = unique([
    exactTransmissionCode,
    ...result.enrichment.possibleGearboxCodes,
  ]);

  const seedMasters = seedCodes
    .map((code) => (code ? { code, ...(gearboxCodesMaster?.[code] || {}) } : null))
    .filter(Boolean);

  if (seedMasters.length === 1) {
    result.enrichment.selectedGearbox = seedMasters[0];
    result.enrichment.masterGearbox =
      gearboxCodesMaster?.[seedMasters[0].code] || result.enrichment.masterGearbox;

    if (seedMasters[0].drivetrain && !result.body.drivetrain) {
      result.body.drivetrain = seedMasters[0].drivetrain;
    }

    result.gearboxCode = seedMasters[0].code;
    result.gearboxCodeSource =
      exactTransmissionCode ? "exact_vin_training_dataset" : "vin_pattern_rule";
    result.menjacSource =
      exactTransmissionCode ? "vin_training_dataset_exact" : "vin_pattern_rule";

    result.enrichment.possibleGearboxCodes = [seedMasters[0].code];
    result.enrichment.gearboxTechCandidates = unique(
      [seedMasters[0].tech_info].filter(Boolean)
    );

    return;
  }

  if (seedCodes.length > 1) {
    result.enrichment.possibleGearboxCodes = unique(seedCodes);

    const seedTechCandidates = seedCodes
      .map((code) => gearboxCodesDb?.models?.[toEnrichmentModelKey(result.model_info.name)]?.find((item) => item.code === code)?.tech_info)
      .filter(Boolean);

    result.enrichment.gearboxTechCandidates = unique(seedTechCandidates);

    if (result.enrichment.gearboxSource === "not_enriched") {
      result.enrichment.gearboxSource =
        exactTransmissionCode
          ? "vin_training_dataset_exact"
          : "vin_pattern_rule";
    }

    return;
  }

  const modelKey = toEnrichmentModelKey(result.model_info.name);
  if (!modelKey) {
    return;
  }

  const candidates = gearboxCodesDb?.models?.[modelKey] || [];
  if (!Array.isArray(candidates) || candidates.length === 0) {
    return;
  }

  const modelYear = result.model_year?.year || null;
  const inferredGearbox = inferGearbox(result);
  const allowedTech = mapInferredGearboxToTechCandidates(inferredGearbox);

  const filtered = candidates.filter((item) => {
    if (modelYear && !isYearCompatible(modelYear, item.mounting)) return false;
    if (allowedTech.length > 0 && !allowedTech.includes(item.tech_info)) return false;
    return true;
  });

  const combinedCodes = unique(filtered.map((item) => item.code));

  result.enrichment.possibleGearboxCodes = combinedCodes;
  result.enrichment.gearboxTechCandidates = unique(filtered.map((item) => item.tech_info));

  if (combinedCodes.length > 0 && result.enrichment.gearboxSource === "not_enriched") {
    result.enrichment.gearboxSource = "enriched_from_model_year_inferred_gearbox";
  }

  if (combinedCodes.length > 1) {
    result.warnings.push("Multiple possible gearbox codes matched the VIN profile.");
    downgradeConfidence(result, "medium");
  }

  if (!result.enrichment.selectedGearbox && combinedCodes.length === 1) {
    const code = combinedCodes[0];
    const master = gearboxCodesMaster?.[code] || null;
    if (master) {
      result.enrichment.selectedGearbox = { code, ...master };
      result.enrichment.masterGearbox = master;
      result.gearboxCode = code;
      result.gearboxCodeSource = result.enrichment.gearboxSource;

      if (master.drivetrain && !result.body.drivetrain) {
        result.body.drivetrain = master.drivetrain;
      }
    }
  }
}

function finalizeFromMasters(result) {
  const selectedEngine = result.enrichment?.selectedEngine;
  const selectedGearbox = result.enrichment?.selectedGearbox;

  if (selectedEngine) {
    if (!result.engine.description && selectedEngine.notes) {
      result.engine.description = selectedEngine.notes;
    }
    if (!result.engine.fuel_type && selectedEngine.fuel_type) {
      result.engine.fuel_type = selectedEngine.fuel_type;
    }
    if (result.engine.displacement_l == null && selectedEngine.displacement_l != null) {
      result.engine.displacement_l = selectedEngine.displacement_l;
    }
    if ((!result.engine.power_kw || result.engine.power_kw.length === 0) && selectedEngine.kw != null) {
      result.engine.power_kw = [selectedEngine.kw];
      result.engine.power_kw_display = String(selectedEngine.kw);
    }
    if (!result.engine.family && selectedEngine.family) {
      result.engine.family = selectedEngine.family;
    }
    if (!result.engine.unit_code && selectedEngine.engineUnit) {
      result.engine.unit_code = selectedEngine.engineUnit;
    }
  }

  if (selectedGearbox?.drivetrain && !result.body.drivetrain) {
    result.body.drivetrain = selectedGearbox.drivetrain;
  }

  if (!result.model_year.year && result.model_year.code) {
    result.model_year.year = decodeModelYearFromVinCode(result.model_year.code);
  }

  if (!result.plant.name && result.plant.code) {
    result.plant.name = decodePlantFromVinCode(result.plant.code);
  }

  refreshVinSummary(result);
}

function finalizeUiFields(result) {
  result.marka = "Skoda";

  result.model = [result.model_info.name, result.model_info.generation]
    .filter(Boolean)
    .join(" ") || result.model_info.name || "N/A";

  if (result.enrichment.possibleEngineCodes.length === 1) {
    result.motorKod = result.enrichment.possibleEngineCodes[0];
  } else if (result.enrichment.possibleEngineCodes.length > 1) {
    result.motorKod = result.enrichment.possibleEngineCodes.join(", ");
  } else if (result.enrichment.exactVinMatch?.engineCode) {
    result.motorKod = result.enrichment.exactVinMatch.engineCode;
  } else {
    result.motorKod = result.engine.code || "N/A";
  }

  result.motor = buildUiEngineLabel(result);
  result.menjac = inferGearbox(result);
  result.modelYear = result.model_year.year || null;
  result.drivetrain = result.body.drivetrain || "N/A";
  result.fuelType = normalizeFuelLabel(result.engine.fuel_type);

  if (isExactDatasetMatch(result)) {
    result.gearboxCode =
      result.enrichment?.exactVinMatch?.transmissionCode ||
      result.gearboxCode ||
      "N/A";
    result.gearboxCodeSource = "exact_vin_training_dataset";
    result.menjacSource = "vin_training_dataset_exact";
    result.confidence = "exact";
  } else if (result.enrichment?.selectedGearbox?.code) {
    result.gearboxCode = result.enrichment.selectedGearbox.code;
    if (result.gearboxCodeSource === "not_available_from_vin") {
      result.gearboxCodeSource = result.enrichment.gearboxSource;
    }
  } else if (result.enrichment.possibleGearboxCodes.length === 1) {
    result.gearboxCode = result.enrichment.possibleGearboxCodes[0];
    result.gearboxCodeSource = result.enrichment.gearboxSource;
  } else if (result.enrichment.possibleGearboxCodes.length > 1) {
    result.gearboxCode = result.enrichment.possibleGearboxCodes.join(", ");
    result.gearboxCodeSource = result.enrichment.gearboxSource;
  }

  applyOilDataToUi(result);

  result.candidates = buildUiCandidates(result);
  result.possible_matches = [...result.candidates];
  refreshVinSummary(result);
}

function buildUiCandidates(result) {
  const candidates = [];

  if (Array.isArray(result.enrichment?.engineCandidates)) {
    for (const item of result.enrichment.engineCandidates.slice(0, 5)) {
      const parts = [`Engine ${item.code}`];

      if (item.kw != null) {
        parts.push(`${item.kw} kW`);
      }

      const desc = item.notes || item.description || "";
      if (desc) {
        parts.push(desc);
      }

      candidates.push(parts.join(" - "));
    }
  }

  if (Array.isArray(result.enrichment?.possibleGearboxCodes)) {
    for (const code of result.enrichment.possibleGearboxCodes.slice(0, 5)) {
      const master = gearboxCodesMaster?.[code] || null;
      const desc = master?.description || master?.type || "";
      candidates.push(desc ? `Gearbox ${code} - ${desc}` : `Gearbox ${code}`);
    }
  }

  return [...new Set(candidates)];
}

function buildMasterEngineCandidate(code) {
  const master = engineCodesMaster?.[code];
  if (!master) return null;

  return {
    code,
    ...master,
    kw: master.powerKw ?? null,
    fuel_type: master.fuel ?? null,
    displacement_l: master.displacementL ?? null,
    oil_capacity_l: master.oilCapacityL ?? null,
    oil_spec: master.oilSpec ?? null,
    oil_viscosity: master.oilViscosity ?? null,
    notes: master.description ?? null,
    _score: 999,
    matchedApplications: [],
  };
}

function applySelectedEngineCandidate(result, candidate) {
  if (!candidate) return;

  result.enrichment.selectedEngine = candidate;
  result.enrichment.masterEngine = engineCodesMaster?.[candidate.code] || result.enrichment.masterEngine;
  result.enrichment.possibleEngineCodes = [candidate.code];

  if (!result.engine.description && candidate.notes) {
    result.engine.description = candidate.notes;
  }

  if (!result.engine.fuel_type && candidate.fuel_type) {
    result.engine.fuel_type = candidate.fuel_type;
  }

  if (result.engine.displacement_l == null && candidate.displacement_l != null) {
    result.engine.displacement_l = candidate.displacement_l;
  }

  if ((!result.engine.power_kw || result.engine.power_kw.length === 0) && candidate.kw != null) {
    result.engine.power_kw = [candidate.kw];
    result.engine.power_kw_display = String(candidate.kw);
  }

  if (!result.engine.family && candidate.family) {
    result.engine.family = candidate.family;
  }

  if (!result.engine.unit_code && candidate.engineUnit) {
    result.engine.unit_code = candidate.engineUnit;
  }

  if (candidate.oil_capacity_l != null && result.oilCapacity === "N/A") {
    result.oilCapacity = `${candidate.oil_capacity_l} L`;
  }
  if (candidate.oil_spec && result.oilSpec === "N/A") {
    result.oilSpec = candidate.oil_spec;
  }
  if (candidate.oil_viscosity && result.oilSae === "N/A") {
    result.oilSae = candidate.oil_viscosity;
  }
}

function isEngineCandidateCompatible(item, modelKey, modelYear, fuelType, displacement, powers, seedCodes = []) {
  if (!item) return false;

  if (seedCodes.length > 0 && !seedCodes.includes(item.code)) {
    if (!Array.isArray(item.applications) || item.applications.length === 0) {
      return false;
    }
  }

  const hasApplications =
    Array.isArray(item.applications) && item.applications.length > 0;

  const matchingApplications = filterMatchingApplications(item, modelKey, modelYear);

  if (hasApplications) {
    if (matchingApplications.length === 0 && !seedCodes.includes(item.code)) return false;
  } else {
    const matchesModel =
      Array.isArray(item.models) && item.models.includes(modelKey);

    if (!matchesModel && !seedCodes.includes(item.code)) return false;
  }

  if (fuelType && item.fuel_type && !isFuelCompatible(item.fuel_type, fuelType)) {
    return false;
  }

  if (
    displacement !== null &&
    item.displacement_l != null &&
    Number(item.displacement_l) !== Number(displacement)
  ) {
    return false;
  }

  if (powers.length > 0 && item.kw != null && !powers.includes(item.kw)) {
    return false;
  }

  return true;
}

function filterMatchingApplications(item, modelKey, modelYear) {
  const applications = Array.isArray(item?.applications) ? item.applications : [];

  return applications.filter((app) => {
    if (modelKey && app.model !== modelKey) return false;
    if (modelYear && !isYearCompatible(modelYear, { start: app.start, end: app.end })) return false;
    return true;
  });
}

function scoreEngineCandidate(item, matchedApplications, preferredCodes = []) {
  let score = 0;

  if (Array.isArray(matchedApplications) && matchedApplications.length > 0) {
    score += 100;
    score += matchedApplications.length === 1 ? 20 : 10;
  }

  if (preferredCodes.includes(item.code)) {
    score += 1000;
  }

  if (item.oil_capacity_l != null) score += 5;
  if (item.oil_spec) score += 5;
  if (item.oil_viscosity) score += 5;
  if (item.timing_drive) score += 2;
  if (item.kw != null) score += 1;

  return score;
}

function compareEngineCandidates(a, b) {
  const scoreDiff = (b?._score || 0) - (a?._score || 0);
  if (scoreDiff !== 0) return scoreDiff;

  const aCode = a?.code || "";
  const bCode = b?.code || "";
  return aCode.localeCompare(bCode);
}

function shouldAutoSelectEngineCandidate(bestCandidate, candidates) {
  if (!bestCandidate) return false;
  if (!Array.isArray(candidates) || candidates.length === 0) return false;
  if (candidates.length === 1) return true;

  const second = candidates[1];
  if (!second) return true;

  const bestHasApplications =
    Array.isArray(bestCandidate.matchedApplications) &&
    bestCandidate.matchedApplications.length > 0;

  const secondHasApplications =
    Array.isArray(second.matchedApplications) &&
    second.matchedApplications.length > 0;

  if (bestHasApplications && !secondHasApplications) return true;

  const bestScore = bestCandidate._score || 0;
  const secondScore = second._score || 0;

  return bestScore - secondScore >= 20;
}

function getCommonOilData(candidates) {
  if (!Array.isArray(candidates) || candidates.length === 0) {
    return {
      capacity_l: null,
      spec: null,
      viscosity: null,
    };
  }

  const capacities = unique(candidates.map((item) => item.oil_capacity_l));
  const specs = unique(candidates.map((item) => item.oil_spec));
  const viscosities = unique(candidates.map((item) => item.oil_viscosity));

  return {
    capacity_l: capacities.length === 1 ? capacities[0] : null,
    spec: specs.length === 1 ? specs[0] : null,
    viscosity: viscosities.length === 1 ? viscosities[0] : null,
  };
}

function applyOilDataToUi(result) {
  const selected = result.enrichment?.selectedEngine;

  if (selected) {
    if (selected.oil_capacity_l != null && result.oilCapacity === "N/A") {
      result.oilCapacity = `${selected.oil_capacity_l} L`;
    }
    if (selected.oil_spec && result.oilSpec === "N/A") {
      result.oilSpec = selected.oil_spec;
    }
    if (selected.oil_viscosity && result.oilSae === "N/A") {
      result.oilSae = selected.oil_viscosity;
    }
    return;
  }

  const commonOil = getCommonOilData(result.enrichment?.engineCandidates || []);

  if (commonOil.capacity_l != null && result.oilCapacity === "N/A") {
    result.oilCapacity = `${commonOil.capacity_l} L`;
  }
  if (commonOil.spec && result.oilSpec === "N/A") {
    result.oilSpec = commonOil.spec;
  }
  if (commonOil.viscosity && result.oilSae === "N/A") {
    result.oilSae = commonOil.viscosity;
  }
}

function buildUiEngineLabel(result) {
  const selected = result.enrichment?.selectedEngine;

  if (selected) {
    const parts = [];
    const description = selected.notes || "";

    if (selected.displacement_l != null) {
      parts.push(`${selected.displacement_l.toFixed(1)}`);
    }

    if (description) {
      parts.push(cleanEngineDescription(description, selected.displacement_l, selected.kw));
    } else if (selected.fuel_type) {
      parts.push(normalizeFuelLabel(selected.fuel_type));
    }

    if (selected.kw != null) {
      parts.push(`${selected.kw} kW`);
    }

    return parts.join(" ").trim() || result.engine.description || "N/A";
  }

  return cleanEngineDescription(result.engine.description || "") || "N/A";
}

function cleanEngineDescription(description, displacement, kw) {
  let text = String(description || "").trim();
  if (!text) return "";

  text = text.replace(/\s+/g, " ");

  if (displacement != null) {
    const d = Number(displacement).toFixed(1).replace(".", "\\.");
    text = text.replace(new RegExp(`\\b${d}\\s*l\\b`, "gi"), "").trim();
    text = text.replace(new RegExp(`\\b${d}\\b`, "gi"), "").trim();
  }

  if (kw != null) {
    text = text.replace(new RegExp(`\\b${kw}\\s*kW\\b`, "gi"), "").trim();
    text = text.replace(new RegExp(`\\b${kw}\\b`, "gi"), "").trim();
  }

  text = text.replace(/\s{2,}/g, " ").trim();
  text = text.replace(/^\/+\s*/, "").trim();
  text = text.replace(/\s*\/+\s*/g, " / ").trim();

  return text;
}

function isFuelCompatible(candidateFuel, vinFuel) {
  if (candidateFuel === vinFuel) return true;

  const groups = {
    hybrid: ["hybrid", "phev", "petrol_hybrid"],
    phev: ["phev", "petrol_hybrid", "hybrid"],
    petrol_hybrid: ["petrol_hybrid", "hybrid", "phev"],
  };

  return groups[candidateFuel]?.includes(vinFuel) || groups[vinFuel]?.includes(candidateFuel) || false;
}

function resolveModelRuleset(modelCode, bodyCode, db, result) {
  const candidates = db?.resolver?.by_model_code?.[modelCode];

  if (!candidates || candidates.length === 0) {
    return null;
  }

  if (candidates.length === 1) {
    return candidates[0];
  }

  const disambiguation = db?.resolver?.disambiguation?.[modelCode];

  if (disambiguation?.position_4?.[bodyCode]) {
    result.warnings.push(`Model code ${modelCode} resolved by body code ${bodyCode}.`);
    downgradeConfidence(result, "medium");
    return disambiguation.position_4[bodyCode];
  }

  result.special_flags.ambiguous_model = true;
  result.possible_matches = candidates.map((item) => String(item));
  result.warnings.push(`Ambiguous model code ${modelCode}; unable to resolve uniquely.`);
  downgradeConfidence(result, "low");

  return null;
}

function resolveManufacturer(wmi) {
  const map = {
    TMB: "Skoda Auto",
    XWW: "Skoda Auto",
    XW8: "Skoda Auto",
  };

  return map[wmi] ?? "Unknown";
}

function resolveCountryHint(wmi) {
  const map = {
    TMB: "Czech Republic",
    XWW: "Kazakhstan",
    XW8: "Russia",
  };

  return map[wmi] ?? null;
}

function downgradeConfidence(result, target) {
  const rank = { exact: 4, high: 3, medium: 2, low: 1 };

  if (rank[target] < rank[result.confidence]) {
    result.confidence = target;
  }
}

function normalizeFuelLabel(fuelType) {
  const map = {
    petrol: "Petrol",
    diesel: "Diesel",
    cng: "CNG",
    phev: "PHEV",
    hybrid: "Hybrid",
    petrol_hybrid: "Hybrid",
    ev: "EV",
    electric: "EV",
  };

  return map[fuelType] ?? "N/A";
}

function inferGearbox(result) {
  const selectedMasterGearbox = result.enrichment?.masterGearbox;
  if (selectedMasterGearbox?.type === "manual") {
    return "Manual";
  }
  if (selectedMasterGearbox?.type === "DSG") {
    return "DSG";
  }
  if (selectedMasterGearbox?.type === "automatic") {
    return "Automatic";
  }
  if (selectedMasterGearbox?.type === "ev") {
    return "EV";
  }

  const exactTransmissionCode = result.enrichment?.exactVinMatch?.transmissionCode || null;
  const exactClassification = inferGearboxFromCode(exactTransmissionCode);
  if (exactClassification) {
    return exactClassification;
  }

  if (result.enrichment?.possibleGearboxCodes?.length === 1) {
    const soleCode = result.enrichment.possibleGearboxCodes[0];
    const soleClassification = inferGearboxFromCode(soleCode);
    if (soleClassification) {
      return soleClassification;
    }
  }

  const drivetrain = result.body?.drivetrain || "";
  const fuelType = result.engine?.fuel_type || "";
  const powerList = result.engine?.power_kw || [];
  const maxPower = powerList.length ? Math.max(...powerList) : null;

  if (fuelType === "ev" || fuelType === "electric") {
    return "EV";
  }

  if (fuelType === "phev" || fuelType === "hybrid" || fuelType === "petrol_hybrid") {
    return "DSG";
  }

  if (maxPower !== null && maxPower >= 140) {
    return "DSG";
  }

  if (drivetrain === "AWD") {
    return "DSG";
  }

  return "Manual";
}

function toEnrichmentModelKey(modelName) {
  const map = {
    Fabia: "FABIA",
    Scala: "SCALA",
    Kamiq: "KAMIQ",
    Karoq: "KAROQ",
    Kodiaq: "KODIAQ",
    Octavia: "OCTAVIA",
    Superb: "SUPERB",
    Enyaq: "ENYAQ",
  };

  return map[modelName] || null;
}

function isYearCompatible(modelYear, mounting) {
  if (!mounting || !mounting.start) return true;

  const startYear = parseInt(String(mounting.start).slice(0, 4), 10);
  const endYear = mounting.end ? parseInt(String(mounting.end).slice(0, 4), 10) : null;

  if (Number.isNaN(startYear)) return true;
  if (modelYear < startYear) return false;
  if (endYear !== null && !Number.isNaN(endYear) && modelYear > endYear) return false;

  return true;
}

function mapInferredGearboxToTechCandidates(inferredGearbox) {
  if (inferredGearbox === "Manual") {
    return ["5S", "6S"];
  }

  if (inferredGearbox === "DSG" || inferredGearbox === "Automatic") {
    return ["6A", "7A", "7C", "8A"];
  }

  if (inferredGearbox === "EV") {
    return ["1E"];
  }

  return [];
}

function unique(items) {
  return [...new Set((items || []).filter((item) => item !== null && item !== undefined && item !== ""))];
}

function refreshVinSummary(result) {
  result.vin_summary.manufacturer = result.wmi.manufacturer;
  result.vin_summary.country_hint = result.wmi.country_hint;
  result.vin_summary.model = result.model_info.name;
  result.vin_summary.generation = result.model_info.generation;
  result.vin_summary.body_style = result.body.style;
  result.vin_summary.steering = result.body.steering;
  result.vin_summary.drivetrain = result.body.drivetrain;
  result.vin_summary.fuel_type = normalizeFuelLabel(result.engine.fuel_type);
  result.vin_summary.power_kw = result.engine.power_kw_display || null;
  result.vin_summary.model_year = result.model_year.year;
  result.vin_summary.plant = result.plant.name;
}

export const decodeSkodaVin = decodeVin;