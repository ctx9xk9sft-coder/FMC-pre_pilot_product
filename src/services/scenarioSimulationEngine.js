import { buildMaintenancePlan } from "./buildMaintenancePlan.js";
import { priceMaintenancePlan } from "./pricingEngine.js";
import { calculateMaintenanceValidation } from "./tcoCalculator.js";
import { EXPLOITATION_PROFILES } from "../data/exploitationProfiles.js";

function toNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function buildScenarioLabel(km) {
  return `${Math.round(toNumber(km, 0) / 1000)}k km`;
}

function resolveExploitationProfile(exploitationType, exploitation) {
  if (exploitation && typeof exploitation === "object") {
    return exploitation;
  }

  if (exploitationType && EXPLOITATION_PROFILES[exploitationType]) {
    return EXPLOITATION_PROFILES[exploitationType];
  }

  return EXPLOITATION_PROFILES.fleet_standard;
}

function resolveUsageProfileKey({
  usageProfileKey,
  exploitationType,
  exploitationProfile,
}) {
  return (
    usageProfileKey ||
    exploitationType ||
    exploitationProfile?.usageProfile ||
    exploitationProfile?.code ||
    "fleet_standard"
  );
}

export function buildSingleScenarioSimulation({
  km,
  contractMonths,
  decoded,
  resolvedVehicle,
  exploitation,
  exploitationType,
  usageProfileKey,
  flexInterval,
  hourlyRate,
  oilPricePerLiter,
  tireCategory,
  laborDiscount,
  partsDiscount,
  oilDiscount,
  scenarioLabel,
  serviceRegime = "flex",
}) {
  const normalizedKm = toNumber(km, 0);
  const normalizedMonths = toNumber(contractMonths, 0);
  const annualKm =
    normalizedMonths > 0 ? Math.round((normalizedKm / normalizedMonths) * 12) : 0;

  const exploitationProfile = resolveExploitationProfile(
    exploitationType,
    exploitation
  );

  const normalizedUsageProfile = resolveUsageProfileKey({
    usageProfileKey,
    exploitationType,
    exploitationProfile,
  });

  const effectiveFlexInterval = Number(flexInterval) > 0 ? Number(flexInterval) : 0;

  const validation = calculateMaintenanceValidation({
    decoded,
    exploitation: exploitationProfile,
    plannedKm: normalizedKm,
    contractMonths: normalizedMonths,
    serviceRegime,
  });

  const maintenancePlan = buildMaintenancePlan({
    decoded,
    resolvedVehicle,
    validation,
    planning: {
      plannedKm: normalizedKm,
      contractMonths: normalizedMonths,
      annualKm,
      serviceRegime,
      usageProfile: normalizedUsageProfile,
      hourlyRate,
      flexibleServiceIntervalKm: effectiveFlexInterval,
    },
  });

  const pricedPlan = priceMaintenancePlan({
    maintenancePlan,
    decoded,
    userPricing: {
      laborRate: hourlyRate,
      oilPricePerLiter,
      tireCategory,
      laborDiscount,
      partsDiscount,
      oilDiscount,
    },
  });

  const totalCost = pricedPlan?.totals?.totalCost || 0;
  const totalEvents = pricedPlan?.totals?.totalEvents || 0;
  const serviceCost = pricedPlan?.totals?.totalServiceCost || 0;
  const brakeCost = pricedPlan?.totals?.totalBrakeCost || 0;
  const tireCost = pricedPlan?.totals?.totalTireCost || 0;

  return {
    label: scenarioLabel || buildScenarioLabel(normalizedKm),
    km: normalizedKm,
    months: normalizedMonths,
    totalCost,
    totalServiceCost: serviceCost,
    totalBrakeCost: brakeCost,
    totalTireCost: tireCost,
    serviceCost,
    brakeCost,
    tireCost,
    costPerKm: normalizedKm > 0 ? totalCost / normalizedKm : 0,
    costPerMonth: normalizedMonths > 0 ? totalCost / normalizedMonths : 0,
    eventCount: totalEvents,
    maintenancePlan,
    pricedPlan,
    validation,
    assumptions: {
      usageProfile: normalizedUsageProfile,
      exploitationType:
        exploitationType || exploitationProfile?.code || "fleet_standard",
      flexIntervalKm: effectiveFlexInterval,
      serviceRegime,
      annualKm,
    },
    meta: {
      pricingStatus: pricedPlan?.meta?.pricingStatus || "unknown",
      blocked: Boolean(pricedPlan?.meta?.blocked),
      engineVersion: pricedPlan?.meta?.engineVersion || null,
      pricingVersion: pricedPlan?.meta?.pricingVersion || null,
    },
  };
}

export default buildSingleScenarioSimulation;