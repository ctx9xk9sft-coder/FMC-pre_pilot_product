
function normalize(value) {
  return String(value || "").trim().toLowerCase();
}

function includesAny(haystack, needles = []) {
  return needles.some((needle) => haystack.includes(String(needle).toLowerCase()));
}

export function resolvePricingProfile(vehicleContext = {}) {
  const model = normalize(vehicleContext.model);
  const engine = normalize(vehicleContext.engine);
  const gearboxCode = normalize(vehicleContext.gearboxCode);
  const drivetrain = normalize(vehicleContext.drivetrain);
  const fuelType = normalize(vehicleContext.fuelType);

  const isDiesel = fuelType.includes("diesel") || engine.includes("tdi");
  const isPetrol = fuelType.includes("petrol") || fuelType.includes("benzin") || engine.includes("tsi");
  const isAwd = drivetrain === "awd" || drivetrain.includes("4x4");

  const missingFields = [];
  if (!model) missingFields.push("model");
  if (!engine && !fuelType) missingFields.push("engine_or_fuel");

  let partsProfileKey = "generic_tsi_small";
  let brakeSpecKey = "generic_tsi_small";
  let tireSpecKey = "standard";
  let precision = "generic";
  let matchedBy = "generic_fallback";
  const warnings = [];

  if (model.includes("kodiaq") && isDiesel && isAwd) {
    partsProfileKey = "kodiaq_2_0_tdi_awd";
    brakeSpecKey = "kodiaq_2_0_tdi_awd";
    tireSpecKey = "suv";
    precision = "exact";
    matchedBy = "model_engine_drivetrain";
  } else if (model.includes("superb") && isDiesel) {
    partsProfileKey = "superb_2_0_tdi";
    brakeSpecKey = "superb_2_0_tdi";
    tireSpecKey = "large";
    precision = "exact";
    matchedBy = "model_engine";
  } else if (model.includes("octavia") && includesAny(engine, ["1.5", "110 kw"]) && isPetrol) {
    partsProfileKey = "octavia_1_5_tsi";
    brakeSpecKey = gearboxCode.includes("dq200") ? "octavia_1_5_tsi_dq200" : "octavia_1_5_tsi";
    tireSpecKey = "standard";
    precision = gearboxCode ? "exact" : "model_family";
    matchedBy = gearboxCode ? "model_engine_gearbox" : "model_engine";
  } else if (model.includes("octavia") && includesAny(engine, ["1.0", "tsi"]) && isPetrol) {
    partsProfileKey = "octavia_1_0_tsi";
    brakeSpecKey = "octavia_1_0_tsi";
    tireSpecKey = "standard";
    precision = "exact";
    matchedBy = "model_engine";
  } else if (model.includes("octavia") && isDiesel) {
    partsProfileKey = "octavia_2_0_tdi";
    brakeSpecKey = "octavia_2_0_tdi";
    tireSpecKey = "standard";
    precision = "exact";
    matchedBy = "model_fuel";
  } else if ((model.includes("scala") || model.includes("kamiq")) && includesAny(engine, ["1.0", "tsi"])) {
    partsProfileKey = "scala_kamiq_1_0_tsi";
    brakeSpecKey = "scala_kamiq_1_0_tsi";
    tireSpecKey = "small";
    precision = "exact";
    matchedBy = "model_engine";
  } else if (model.includes("fabia") && isPetrol) {
    partsProfileKey = "fabia_small_petrol";
    brakeSpecKey = "fabia_small_petrol";
    tireSpecKey = "small";
    precision = "exact";
    matchedBy = "model_fuel";
  } else if (isDiesel) {
    partsProfileKey = "generic_tdi_ea288";
    brakeSpecKey = "generic_tdi_ea288";
    tireSpecKey = "standard";
    precision = "generic";
    matchedBy = "fuel_fallback";
    warnings.push("Korišćen je generički TDI pricing profil.");
  } else if (isPetrol) {
    partsProfileKey = "generic_tsi_small";
    brakeSpecKey = "generic_tsi_small";
    tireSpecKey = "standard";
    precision = model ? "model_family" : "generic";
    matchedBy = model ? "engine_fallback" : "generic_fallback";
    warnings.push("Korišćen je generički TSI pricing profil.");
  } else {
    warnings.push("Nedovoljno podataka za precizan pricing profil. Korišćen je generički fallback.");
  }

  if (!gearboxCode && model.includes("octavia") && includesAny(engine, ["1.5", "110 kw"])) {
    warnings.push("Gearbox code nije potvrđen; Octavia 1.5 TSI koristi family-level pricing.");
  }

  return {
    partsProfileKey,
    brakeSpecKey,
    tireSpecKey,
    precision,
    matchedBy,
    warnings,
    missingFields,
    fallbackUsed: precision !== "exact",
  };
}

export default resolvePricingProfile;
