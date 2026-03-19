import engineCodesMaster from "../data/engine_codes_master.json" with { type: 'json' };
import gearboxCodesMaster from "../data/gearbox_codes_master.json" with { type: 'json' };
import { EXPLOITATION_PROFILES } from "../data/exploitationProfiles.js";
import { getLaborHours, getMaintenanceRule, getOilIntervalConfig, getServiceParts } from "./maintenance/ruleRegistry.js";

function toNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function roundToStep(value, step) {
  if (!step || step <= 0) return value;
  return Math.ceil(value / step) * step;
}

function makeEventId(type, km, month) {
  return `${type}_${km || 0}_${month || 0}`;
}

function cloneItems(items) {
  return Array.isArray(items) ? [...items] : [];
}

function sortEvents(events = []) {
  return [...events].sort((a, b) => {
    const monthA = a?.due?.month ?? 9999;
    const monthB = b?.due?.month ?? 9999;
    const kmA = a?.due?.km ?? 9999999;
    const kmB = b?.due?.km ?? 9999999;

    if (monthA !== monthB) return monthA - monthB;
    return kmA - kmB;
  });
}

function pushEvent(events, event) {
  events.push({
    id: makeEventId(event.type, event.due?.km, event.due?.month),
    type: event.type,
    category: event.category,
    title: event.title,
    due: {
      km: event.due?.km ?? null,
      month: event.due?.month ?? null,
    },
    trigger: {
      kind: event.trigger?.kind || "manual",
      km: event.trigger?.km ?? null,
      months: event.trigger?.months ?? null,
    },
    items: cloneItems(event.items),
    vehicleContext: { ...(event.vehicleContext || {}) },
    pricingContext: { ...(event.pricingContext || {}) },
    source: {
      rule: event.source?.rule || "manual",
      confidence: event.source?.confidence || "medium",
    },
    notes: cloneItems(event.notes),
    readiness: {
      level: event.readinessLevel || "confirmed",
    },
  });
}

function getAnnualKm(plannedKm, contractMonths) {
  if (!plannedKm || !contractMonths) return 0;
  return (plannedKm / contractMonths) * 12;
}

function normalizeUsageProfile(usageProfile) {
  if (!usageProfile) return "mixed";

  if (EXPLOITATION_PROFILES?.[usageProfile]) {
    return String(EXPLOITATION_PROFILES[usageProfile].usageProfile || usageProfile);
  }

  const value = String(usageProfile).trim().toLowerCase();
  if (!value) return "mixed";
  if (value.includes("grad") || value.includes("city")) return "city_heavy";
  if (value.includes("autoput") || value.includes("highway")) return "highway";
  return value;
}

function normalizeBuildInput(input = {}) {
  const legacyVehicle = input?.vehicle;
  const planning = input?.planning || {};
  const exploitation = input?.exploitation;

  const derivedUsageProfile =
    planning?.usageProfile ||
    exploitation?.usageProfile ||
    exploitation?.code ||
    exploitation?.key ||
    exploitation?.id ||
    exploitation?.label ||
    exploitation?.name ||
    input?.usageProfile ||
    "mixed";

  return {
    decoded: input?.decoded || legacyVehicle?.decoded || null,
    resolvedVehicle: input?.resolvedVehicle || legacyVehicle || null,
    validation: input?.validation || null,
    planning: {
      plannedKm: toNumber(planning?.plannedKm ?? input?.plannedKm),
      contractMonths: toNumber(planning?.contractMonths ?? input?.contractMonths),
      annualKm: toNumber(planning?.annualKm),
      serviceRegime: planning?.serviceRegime || input?.serviceRegime || "flex",
      usageProfile: normalizeUsageProfile(derivedUsageProfile),
      flexibleServiceIntervalKm: toNumber(
        planning?.flexibleServiceIntervalKm ?? input?.flexibleServiceIntervalKm,
        0
      ),
    },
  };
}

function inferFuelTypeFromTexts(...values) {
  const text = values
    .filter(Boolean)
    .map((value) => String(value).trim().toLowerCase())
    .join(" ");

  if (!text) return null;
  if (text.includes("diesel") || text.includes("dizel") || text.includes("tdi")) return "diesel";
  if (text.includes("petrol") || text.includes("benzin") || text.includes("tsi") || text.includes("mpi") || text.includes("tgi")) {
    return "petrol";
  }
  return null;
}

function getEngineMaster(engineCode) {
  if (!engineCode) return null;
  return engineCodesMaster?.[String(engineCode).trim().toUpperCase()] || null;
}

function getFuelTypeFromEngineMaster(engineCode) {
  const master = getEngineMaster(engineCode);
  const fuel = String(master?.fuel || "").trim().toLowerCase();
  return fuel === "diesel" || fuel === "petrol" ? fuel : null;
}

function getGearboxMaster(gearboxCode) {
  if (!gearboxCode) return null;
  return gearboxCodesMaster?.[String(gearboxCode).trim().toUpperCase()] || null;
}

function getGearboxFamilyFromMaster(gearboxCode) {
  const master = getGearboxMaster(gearboxCode);
  return master?.family || null;
}

function getVehicleContext(resolvedVehicle, decoded) {
  const engineCode =
    resolvedVehicle?.fields?.engine?.value ||
    resolvedVehicle?.vehicle?.engineCode ||
    decoded?.motorKod ||
    decoded?.engineCode ||
    null;

  const engineDisplay =
    resolvedVehicle?.fields?.engine?.displayValue ||
    resolvedVehicle?.vehicle?.engineDisplay ||
    decoded?.motor ||
    decoded?.engine ||
    null;

  const rawFuelType =
    resolvedVehicle?.vehicle?.fuelType ||
    decoded?.fuelType ||
    decoded?.gorivo ||
    null;

  return {
    brand: resolvedVehicle?.vehicle?.brand || decoded?.marka || "Skoda",
    model: resolvedVehicle?.vehicle?.model || decoded?.model || null,
    modelYear: resolvedVehicle?.vehicle?.modelYear || decoded?.modelYear || null,
    engine: resolvedVehicle?.vehicle?.engine || engineCode,
    engineCode,
    engineDisplay,
    engineConfidence: resolvedVehicle?.fields?.engine?.confidence || null,
    engineSource: resolvedVehicle?.fields?.engine?.source || null,
    gearbox:
      resolvedVehicle?.fields?.gearbox?.displayValue ||
      resolvedVehicle?.vehicle?.gearbox ||
      resolvedVehicle?.fields?.gearbox?.value ||
      decoded?.menjac ||
      null,
    gearboxCode:
      resolvedVehicle?.fields?.gearbox?.value ||
      decoded?.gearboxCode ||
      null,
    gearboxConfidence: resolvedVehicle?.fields?.gearbox?.confidence || null,
    gearboxSource: resolvedVehicle?.fields?.gearbox?.source || null,
    drivetrain:
      resolvedVehicle?.vehicle?.drivetrain ||
      resolvedVehicle?.fields?.drivetrain?.value ||
      decoded?.drivetrain ||
      null,
    drivetrainConfidence: resolvedVehicle?.fields?.drivetrain?.confidence || null,
    drivetrainSource: resolvedVehicle?.fields?.drivetrain?.source || null,
    fuelType: inferFuelTypeFromTexts(rawFuelType, engineDisplay, engineCode) || getFuelTypeFromEngineMaster(engineCode),
    fuelSource: rawFuelType ? "decoded" : getFuelTypeFromEngineMaster(engineCode) ? "engine_master" : null,
    manualOverrideUsed: Boolean(resolvedVehicle?.vehicle?.source?.manualOverrideUsed),
  };
}

function isDiesel(vehicleContext) {
  const fuelType = String(vehicleContext?.fuelType || "").trim().toLowerCase();
  const engineText = [vehicleContext?.engine, vehicleContext?.engineCode, vehicleContext?.engineDisplay]
    .filter(Boolean)
    .join(" ")
    .toUpperCase();

  return fuelType === "diesel" || fuelType === "dizel" || engineText.includes("TDI");
}

function isPetrol(vehicleContext) {
  const fuelType = String(vehicleContext?.fuelType || "").trim().toLowerCase();
  const engineText = [vehicleContext?.engine, vehicleContext?.engineCode, vehicleContext?.engineDisplay]
    .filter(Boolean)
    .join(" ")
    .toUpperCase();

  return fuelType === "petrol" || fuelType === "benzin" || engineText.includes("TSI") || engineText.includes("MPI") || engineText.includes("TGI");
}

function isDsg(vehicleContext) {
  const gearboxText = [vehicleContext?.gearbox, vehicleContext?.gearboxCode]
    .filter(Boolean)
    .join(" ")
    .toUpperCase();

  return gearboxText.includes("DSG") || gearboxText.includes("DQ");
}

function getDsgServiceProfile(vehicleContext) {
  const gearboxText = [vehicleContext?.gearboxCode, vehicleContext?.gearbox]
    .filter(Boolean)
    .join(" ")
    .toUpperCase();

  const masterFamily = getGearboxFamilyFromMaster(vehicleContext?.gearboxCode);
  const detectedFamily = masterFamily || (
    gearboxText.includes("DQ500")
      ? "DQ500"
      : gearboxText.includes("DQ381")
      ? "DQ381"
      : gearboxText.includes("DQ250")
      ? "DQ250"
      : gearboxText.includes("DQ400")
      ? "DQ400"
      : gearboxText.includes("DQ200")
      ? "DQ200"
      : gearboxText.includes("DSG")
      ? "DSG_UNKNOWN"
      : null
  );

  const isDsgVehicle = Boolean(masterFamily?.startsWith?.("DQ")) || isDsg(vehicleContext);
  if (!isDsgVehicle) {
    return { isDsg: false, serviceRequired: false, gearboxFamily: null, oilLiters: 0, filterPartId: null };
  }

  const family = detectedFamily;
  const serviceRequired = ["DQ250", "DQ381", "DQ400", "DQ500"].includes(family);
  const oilLiters = family === "DQ500" ? 7 : family === "DQ381" ? 5.5 : 6;
  const filterPartId = family === "DQ381" ? "dsg_filter_dq381" : ["DQ250", "DQ400", "DQ500"].includes(family) ? "dsg_filter_generic" : null;

  return {
    isDsg: true,
    serviceRequired,
    gearboxFamily: family,
    oilLiters,
    filterPartId,
  };
}

function isAwd(vehicleContext) {
  const drivetrain = String(vehicleContext?.drivetrain || "").trim().toUpperCase();
  return drivetrain === "AWD" || drivetrain === "4X4";
}


function confidenceRank(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (["exact", "confirmed", "high"].includes(normalized)) return 3;
  if (normalized === "medium") return 2;
  if (normalized === "low") return 1;
  return 0;
}

function buildRuleValidationContext(vehicleContext) {
  const gearboxMaster = getGearboxMaster(vehicleContext?.gearboxCode) || null;
  const engineMaster = getEngineMaster(vehicleContext?.engineCode) || null;
  const gearboxFamily = getGearboxFamilyFromMaster(vehicleContext?.gearboxCode);
  const dsgProfile = getDsgServiceProfile(vehicleContext);
  const drivetrainIsConfirmedAwd = isAwd(vehicleContext) && confidenceRank(vehicleContext?.drivetrainConfidence) >= 3;
  const gearboxIsConfirmedDsg = dsgProfile.isDsg && confidenceRank(vehicleContext?.gearboxConfidence) >= 3;
  const fuelType = vehicleContext?.fuelType || getFuelTypeFromEngineMaster(vehicleContext?.engineCode);
  const petrolConfirmed = String(fuelType || "").toLowerCase() === "petrol";

  const warnings = [];
  const assumptions = [];

  if (vehicleContext?.fuelSource === 'engine_master') {
    assumptions.push('Fuel type izveden iz engine master baze.');
  }
  if (vehicleContext?.manualOverrideUsed) {
    assumptions.push('Tehnički profil uključuje ručnu dopunu.');
  }
  if (dsgProfile.isDsg && !gearboxIsConfirmedDsg) {
    warnings.push('DSG servis blokiran dok gearbox nije potvrđen visokim poverenjem.');
  }
  if (isAwd(vehicleContext) && !drivetrainIsConfirmedAwd) {
    warnings.push('Haldex servis blokiran dok AWD nije potvrđen visokim poverenjem.');
  }
  if (!petrolConfirmed && isPetrol(vehicleContext)) {
    warnings.push('Spark plug pravilo nije aktivirano jer fuel type nije pouzdano potvrđen kao petrol.');
  }

  return {
    fuelType,
    engineMaster,
    gearboxMaster,
    gearboxFamily,
    assumptions,
    warnings,
    features: {
      sparkPlugsEligible: petrolConfirmed,
      dsgServiceEligible: gearboxIsConfirmedDsg && Boolean(gearboxMaster?.serviceRequired ?? dsgProfile.serviceRequired),
      haldexServiceEligible: drivetrainIsConfirmedAwd,
    },
  };
}

function estimateOilLiters(decoded) {
  const liters = Number(decoded?.oilCapacity);
  return Number.isFinite(liters) && liters > 0 ? liters : 5;
}

function mapServiceParts(eventType, options = {}) {
  return getServiceParts(eventType, options);
}


function buildOilServiceEvents({
  events,
  plannedKm,
  contractMonths,
  vehicleContext,
  decoded,
  serviceRegime,
  usageProfile,
  flexibleServiceIntervalKm,
}) {
  const annualKm = getAnnualKm(plannedKm, contractMonths);
  const oilInterval = getOilIntervalConfig({
    serviceRegime,
    usageProfile,
    annualKm,
    flexibleServiceIntervalKm,
  });
  const kmStep = oilInterval.km;
  const monthStep = oilInterval.months;

  const totalMonths = toNumber(contractMonths);
  const totalKm = toNumber(plannedKm);

  let index = 1;
  while (true) {
    const dueMonth = monthStep * index;
    const dueKm = roundToStep(kmStep * index, 1000);

    if (dueMonth > totalMonths && dueKm > totalKm) break;

    const items = mapServiceParts("oil_service", {
      includeDieselSecondService: index % 2 === 0 && isDiesel(vehicleContext),
    });

    pushEvent(events, {
      type: "oil_service",
      category: getMaintenanceRule("oil_service")?.category || "regular_service",
      title: index === 1 ? "Prvi mali servis" : `Mali servis ${index}`,
      due: {
        km: dueKm <= totalKm ? dueKm : null,
        month: dueMonth <= totalMonths ? dueMonth : null,
      },
      trigger: {
        kind: "km_or_time",
        km: kmStep,
        months: monthStep,
      },
      items,
      vehicleContext,
      pricingContext: {
        laborHours: getLaborHours("oil_service", 1.2),
        oilLiters: estimateOilLiters(decoded),
        partsGroup: "minor_service",
      },
      source: {
        rule: oilInterval?.sourceRule || "base_oil_service_rule",
        confidence: "medium",
      },
      notes: serviceRegime === "flex" ? ["Flex interval aproksimacija."] : [],
    });

    index += 1;
  }
}

function buildSparkPlugEvents({ events, plannedKm, contractMonths, vehicleContext, ruleValidation }) {
  const rule = getMaintenanceRule("spark_plugs");
  if (!ruleValidation?.features?.sparkPlugsEligible) return;

  const kmStep = toNumber(rule?.intervalKm, 60000);
  const monthStep = toNumber(rule?.intervalMonths, 48);
  const totalMonths = toNumber(contractMonths);
  const totalKm = toNumber(plannedKm);

  let index = 1;
  while (true) {
    const dueMonth = monthStep * index;
    const dueKm = kmStep * index;

    if (dueMonth > totalMonths && dueKm > totalKm) break;

    pushEvent(events, {
      type: "spark_plugs",
      category: rule?.category || "powertrain",
      title: rule?.title || "Zamena svećica",
      due: {
        km: dueKm <= totalKm ? dueKm : null,
        month: dueMonth <= totalMonths ? dueMonth : null,
      },
      trigger: {
        kind: "km_or_time",
        km: kmStep,
        months: monthStep,
      },
      items: mapServiceParts("spark_plugs"),
      vehicleContext,
      pricingContext: {
        laborHours: getLaborHours("spark_plugs", 0.8),
        partsGroup: "ignition",
      },
      source: {
        rule: rule?.sourceRule || "petrol_spark_plugs_60k_48m",
        confidence: "high",
      },
      notes: [],
      readinessLevel: "confirmed",
    });

    index += 1;
  }
}

function buildDsgEvents({ events, plannedKm, vehicleContext, ruleValidation }) {
  const rule = getMaintenanceRule("dsg_service");
  const dsgProfile = getDsgServiceProfile(vehicleContext);
  if (!dsgProfile.isDsg || !ruleValidation?.features?.dsgServiceEligible) return;

  const kmStep = toNumber(rule?.intervalKm, 120000);
  const oilLiters = toNumber(dsgProfile.oilLiters, toNumber(rule?.defaultOilLiters, 6));
  const totalKm = toNumber(plannedKm);

  for (let dueKm = kmStep; dueKm <= totalKm; dueKm += kmStep) {
    pushEvent(events, {
      type: "dsg_service",
      category: rule?.category || "powertrain",
      title: rule?.title || "Servis DSG menjača",
      due: { km: dueKm, month: null },
      trigger: {
        kind: "km_only",
        km: kmStep,
        months: null,
      },
      items: mapServiceParts("dsg_service"),
      vehicleContext,
      pricingContext: {
        laborHours: getLaborHours("dsg_service", 1.6),
        oilLiters,
        partsGroup: "dsg_service",
        gearboxFamily: dsgProfile.gearboxFamily,
        filterPartId: dsgProfile.filterPartId,
      },
      source: {
        rule: rule?.sourceRule || "dsg_service_120k",
        confidence: "high",
      },
      notes: [],
      readinessLevel: "confirmed",
    });
  }
}

function buildHaldexEvents({ events, plannedKm, contractMonths, vehicleContext, ruleValidation }) {
  const rule = getMaintenanceRule("haldex_service");
  if (!ruleValidation?.features?.haldexServiceEligible) return;

  const kmStep = toNumber(rule?.intervalKm, 60000);
  const monthStep = toNumber(rule?.intervalMonths, 24);
  const oilLiters = toNumber(rule?.defaultOilLiters, 1);
  const totalMonths = toNumber(contractMonths);
  const totalKm = toNumber(plannedKm);

  let index = 1;
  while (true) {
    const dueMonth = monthStep * index;
    const dueKm = kmStep * index;

    if (dueMonth > totalMonths && dueKm > totalKm) break;

    pushEvent(events, {
      type: "haldex_service",
      category: rule?.category || "drivetrain",
      title: rule?.title || "Servis Haldex pogona",
      due: {
        km: dueKm <= totalKm ? dueKm : null,
        month: dueMonth <= totalMonths ? dueMonth : null,
      },
      trigger: {
        kind: "km_or_time",
        km: kmStep,
        months: monthStep,
      },
      items: mapServiceParts("haldex_service"),
      vehicleContext,
      pricingContext: {
        laborHours: getLaborHours("haldex_service", 0.8),
        oilLiters,
        partsGroup: "haldex_service",
      },
      source: {
        rule: rule?.sourceRule || "haldex_60k_24m",
        confidence: "high",
      },
      notes: [],
      readinessLevel: "confirmed",
    });

    index += 1;
  }
}

function buildBrakeFluidEvents({ events, contractMonths, vehicleContext }) {
  const rule = getMaintenanceRule("brake_fluid");
  const monthStep = toNumber(rule?.intervalMonths, 24);
  const totalMonths = toNumber(contractMonths);

  for (let dueMonth = monthStep; dueMonth <= totalMonths; dueMonth += monthStep) {
    pushEvent(events, {
      type: "brake_fluid",
      category: rule?.category || "regular_service",
      title: rule?.title || "Zamena kočione tečnosti",
      due: { km: null, month: dueMonth },
      trigger: {
        kind: "time_only",
        km: null,
        months: monthStep,
      },
      items: mapServiceParts("brake_fluid"),
      vehicleContext,
      pricingContext: {
        laborHours: getLaborHours("brake_fluid", 0.7),
        partsGroup: "brake_fluid",
      },
      source: {
        rule: rule?.sourceRule || "brake_fluid_24m",
        confidence: "high",
      },
      notes: [],
      readinessLevel: "confirmed",
    });
  }
}

function buildBrakeEvents({ events, plannedKm, vehicleContext, usageProfile }) {
  const profile = EXPLOITATION_PROFILES?.[usageProfile] || null;
  const brakeFactor = toNumber(profile?.brakeFactor, 1);
  const totalKm = toNumber(plannedKm);

  const frontPadsInterval = Math.max(15000, Math.round(40000 / brakeFactor / 1000) * 1000);
  const rearPadsInterval = Math.max(20000, Math.round(60000 / brakeFactor / 1000) * 1000);
  const frontDiscsInterval = Math.max(30000, Math.round(80000 / brakeFactor / 1000) * 1000);
  const rearDiscsInterval = Math.max(40000, Math.round(120000 / brakeFactor / 1000) * 1000);

  for (let dueKm = frontPadsInterval; dueKm <= totalKm; dueKm += frontPadsInterval) {
    pushEvent(events, {
      type: "front_brake_pads",
      category: "brakes",
      title: "Prednje pločice",
      due: { km: dueKm, month: null },
      trigger: { kind: "km_only", km: frontPadsInterval, months: null },
      items: ["front_brake_pads"],
      vehicleContext,
      pricingContext: { laborHours: getLaborHours("front_brake_pads", 1), partsGroup: "front_brakes" },
      source: { rule: "brake_wear_profile_front_pads", confidence: "medium" },
      notes: ["Interval je aproksimacija na osnovu eksploatacije."],
      readinessLevel: "estimated",
    });
  }

  for (let dueKm = rearPadsInterval; dueKm <= totalKm; dueKm += rearPadsInterval) {
    pushEvent(events, {
      type: "rear_brake_pads",
      category: "brakes",
      title: "Zadnje pločice",
      due: { km: dueKm, month: null },
      trigger: { kind: "km_only", km: rearPadsInterval, months: null },
      items: ["rear_brake_pads"],
      vehicleContext,
      pricingContext: { laborHours: getLaborHours("rear_brake_pads", 1), partsGroup: "rear_brakes" },
      source: { rule: "brake_wear_profile_rear_pads", confidence: "medium" },
      notes: ["Interval je aproksimacija na osnovu eksploatacije."],
      readinessLevel: "estimated",
    });
  }

  for (let dueKm = frontDiscsInterval; dueKm <= totalKm; dueKm += frontDiscsInterval) {
    pushEvent(events, {
      type: "front_brake_discs",
      category: "brakes",
      title: "Prednji diskovi",
      due: { km: dueKm, month: null },
      trigger: { kind: "km_only", km: frontDiscsInterval, months: null },
      items: ["front_brake_discs"],
      vehicleContext,
      pricingContext: { laborHours: getLaborHours("front_brake_discs", 1.2), partsGroup: "front_brakes" },
      source: { rule: "brake_wear_profile_front_discs", confidence: "medium" },
      notes: ["Interval je aproksimacija na osnovu eksploatacije."],
      readinessLevel: "estimated",
    });
  }

  for (let dueKm = rearDiscsInterval; dueKm <= totalKm; dueKm += rearDiscsInterval) {
    pushEvent(events, {
      type: "rear_brake_discs",
      category: "brakes",
      title: "Zadnji diskovi",
      due: { km: dueKm, month: null },
      trigger: { kind: "km_only", km: rearDiscsInterval, months: null },
      items: ["rear_brake_discs"],
      vehicleContext,
      pricingContext: { laborHours: getLaborHours("rear_brake_discs", 1.2), partsGroup: "rear_brakes" },
      source: { rule: "brake_wear_profile_rear_discs", confidence: "medium" },
      notes: ["Interval je aproksimacija na osnovu eksploatacije."],
      readinessLevel: "estimated",
    });
  }
}

function buildTireEvents({ events, plannedKm, contractMonths, usageProfile, vehicleContext }) {
  const profile = EXPLOITATION_PROFILES?.[usageProfile] || null;
  const wearFactor = toNumber(profile?.tireFactor, 1);
  const totalKm = toNumber(plannedKm);
  const totalMonths = toNumber(contractMonths);
  const tireKmInterval = Math.max(25000, Math.round(50000 / wearFactor / 1000) * 1000);

  for (let dueKm = tireKmInterval; dueKm <= totalKm; dueKm += tireKmInterval) {
    pushEvent(events, {
      type: dueKm % (tireKmInterval * 2) === 0 ? "winter_tires" : "summer_tires",
      category: "tires",
      title: dueKm % (tireKmInterval * 2) === 0 ? "Set zimskih pneumatika" : "Set letnjih pneumatika",
      due: { km: dueKm, month: null },
      trigger: { kind: "km_only", km: tireKmInterval, months: null },
      items: ["tires_set"],
      vehicleContext,
      pricingContext: { laborHours: getLaborHours("tire_replacement", 1), partsGroup: "tires" },
      source: { rule: "tire_wear_profile", confidence: "medium" },
      notes: ["Zamena pneumatika je aproksimacija na osnovu eksploatacije."],
    });
  }

  for (let dueMonth = 6; dueMonth <= totalMonths; dueMonth += 6) {
    pushEvent(events, {
      type: "seasonal_tire_change",
      category: "tires",
      title: "Sezonska zamena pneumatika",
      due: { km: null, month: dueMonth },
      trigger: { kind: "time_only", km: null, months: 6 },
      items: ["seasonal_tire_change"],
      vehicleContext,
      pricingContext: { laborHours: getLaborHours("seasonal_tire_change", 0.6), partsGroup: "tires" },
      source: { rule: "seasonal_tire_change_6m", confidence: "high" },
      notes: [],
      readinessLevel: "confirmed",
    });
  }
}

function buildBlockedPlan({ vehicleContext, planning, validation, reason }) {
  return {
    vehicle: vehicleContext,
    planning,
    validation: validation || null,
    events: [],
    totals: {
      totalEvents: 0,
      totalServiceCost: 0,
      totalBrakeCost: 0,
      totalTireCost: 0,
      totalCost: 0,
    },
    meta: {
      engineVersion: "maintenance-engine-v3-rule-registry",
      pricingVersion: null,
      blocked: true,
      pricingStatus: "unavailable",
      reason: reason || "planning_blocked",
    },
  };
}

export function buildMaintenancePlan(input = {}) {
  const normalized = normalizeBuildInput(input);
  const { decoded, resolvedVehicle, validation } = normalized;
  const serviceRegime = normalized.planning.serviceRegime || "flex";
  const usageProfile = normalized.planning.usageProfile || "mixed";
  const plannedKm = toNumber(normalized.planning.plannedKm);
  const contractMonths = toNumber(normalized.planning.contractMonths);
  const annualKm = normalized.planning.annualKm || getAnnualKm(plannedKm, contractMonths);
  const flexibleServiceIntervalKm = toNumber(normalized.planning.flexibleServiceIntervalKm, 0);

  const vehicleContext = getVehicleContext(resolvedVehicle, decoded);
  const ruleValidation = buildRuleValidationContext(vehicleContext);
  const planning = {
    plannedKm,
    contractMonths,
    annualKm,
    serviceRegime,
    usageProfile,
    flexibleServiceIntervalKm,
  };

  if (!decoded?.supported) {
    return buildBlockedPlan({
      vehicleContext,
      planning,
      validation,
      reason: decoded?.reason || "vin_not_supported",
    });
  }

  if (plannedKm <= 0 || contractMonths <= 0) {
    return buildBlockedPlan({
      vehicleContext,
      planning,
      validation,
      reason: "planning_inputs_missing",
    });
  }

  const events = [];

  buildOilServiceEvents({
    events,
    plannedKm,
    contractMonths,
    vehicleContext,
    decoded,
    serviceRegime,
    usageProfile,
    flexibleServiceIntervalKm,
  });
  buildSparkPlugEvents({ events, plannedKm, contractMonths, vehicleContext, ruleValidation });
  buildDsgEvents({ events, plannedKm, vehicleContext, ruleValidation });
  buildHaldexEvents({ events, plannedKm, contractMonths, vehicleContext, ruleValidation });
  buildBrakeEvents({ events, plannedKm, vehicleContext, usageProfile });
  buildTireEvents({ events, plannedKm, contractMonths, usageProfile, vehicleContext });
  buildBrakeFluidEvents({ events, contractMonths, vehicleContext });

  const sortedEvents = sortEvents(events);
  const confirmedEvents = sortedEvents.filter((event) => event?.readiness?.level === "confirmed");
  const estimatedEvents = sortedEvents.filter((event) => event?.readiness?.level === "estimated");

  return {
    vehicle: vehicleContext,
    planning,
    validation: validation || null,
    events: sortedEvents,
    totals: {
      totalEvents: sortedEvents.length,
      totalServiceCost: 0,
      totalBrakeCost: 0,
      totalTireCost: 0,
      totalCost: 0,
    },
    meta: {
      engineVersion: "maintenance-engine-v4-safety-guarded",
      pricingVersion: null,
      blocked: false,
      pricingStatus: "unpriced",
      planReadiness: confirmedEvents.length > 0 ? "quote_ready" : "estimate_ready",
      confirmedEventsCount: confirmedEvents.length,
      estimatedEventsCount: estimatedEvents.length,
      assumptions: ruleValidation.assumptions,
      ruleValidationWarnings: ruleValidation.warnings,
      ruleValidation: {
        sparkPlugsEligible: ruleValidation.features.sparkPlugsEligible,
        dsgServiceEligible: ruleValidation.features.dsgServiceEligible,
        haldexServiceEligible: ruleValidation.features.haldexServiceEligible,
        derivedFuelType: ruleValidation.fuelType || null,
        gearboxFamily: ruleValidation.gearboxFamily || null,
      },
    },
  };
}

export default buildMaintenancePlan;
