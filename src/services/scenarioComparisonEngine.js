import { EXPLOITATION_PROFILES } from "../data/exploitationProfiles.js";
import { buildSingleScenarioSimulation } from "./scenarioSimulationEngine.js";

function normalizeScenarioKmList(scenarioKmList = []) {
  const unique = new Set();

  for (const value of scenarioKmList) {
    const km = Number(value);
    if (Number.isFinite(km) && km > 0) {
      unique.add(km);
    }
  }

  return [...unique].sort((a, b) => a - b);
}

function resolveExploitationType(exploitationType, exploitation) {
  if (exploitationType && EXPLOITATION_PROFILES?.[exploitationType]) {
    return exploitationType;
  }

  const normalizedLabel = String(exploitation?.label || "")
    .trim()
    .toLowerCase();

  return (
    Object.keys(EXPLOITATION_PROFILES).find(
      (key) =>
        String(EXPLOITATION_PROFILES[key]?.label || "")
          .trim()
          .toLowerCase() === normalizedLabel
    ) || "fleet_standard"
  );
}

function toPositiveNumber(value) {
  const numericValue = Number(value);
  return Number.isFinite(numericValue) && numericValue > 0 ? numericValue : null;
}

function resolveDirectFlexInterval(flexInterval) {
  return toPositiveNumber(flexInterval);
}

function resolveVariantFlexInterval(flexInterval, fallback) {
  const direct = toPositiveNumber(flexInterval);
  if (direct) return direct;

  const fallbackValue = toPositiveNumber(fallback);
  if (fallbackValue) return fallbackValue;

  return 25000;
}

export function buildScenarioComparisonData({
  scenarioKmList,
  contractMonths,
  decoded,
  resolvedVehicle,
  exploitation,
  exploitationType,
  flexInterval,
  hourlyRate,
  oilPricePerLiter,
  tireCategory,
  laborDiscount,
  partsDiscount,
  oilDiscount,
}) {
  const normalizedKmList = normalizeScenarioKmList(scenarioKmList);
  const resolvedExploitationType = resolveExploitationType(
    exploitationType,
    exploitation
  );

  // Važno:
  // standard scenario comparison mora da koristi isti ulaz kao glavni flow,
  // bez dodatnog "pametnog" fallback-a na recommendedFlex.
  const directFlexInterval = resolveDirectFlexInterval(flexInterval);

  return normalizedKmList.map((km) =>
    buildSingleScenarioSimulation({
      km,
      contractMonths,
      decoded,
      resolvedVehicle,
      exploitation,
      exploitationType: resolvedExploitationType,
      usageProfileKey: resolvedExploitationType,
      flexInterval: directFlexInterval,
      hourlyRate,
      oilPricePerLiter,
      tireCategory,
      laborDiscount,
      partsDiscount,
      oilDiscount,
    })
  );
}

export function buildScenarioVariantRows({
  plannedKm,
  contractMonths,
  decoded,
  resolvedVehicle,
  exploitation,
  exploitationType,
  flexInterval,
  hourlyRate,
  oilPricePerLiter,
  tireCategory,
  laborDiscount,
  partsDiscount,
  oilDiscount,
}) {
  const resolvedExploitationType = resolveExploitationType(
    exploitationType,
    exploitation
  );

  // Za variant rows je i dalje OK da imamo fallback logiku,
  // jer su to namerno "what-if" scenariji.
  const expectedFlex = resolveVariantFlexInterval(
    flexInterval,
    EXPLOITATION_PROFILES?.[resolvedExploitationType]?.recommendedFlex
  );

  const variants = [
    {
      label: "Optimistični",
      usageProfileKey: "highway",
      flexInterval: Math.max(expectedFlex, 30000),
    },
    {
      label: "Očekivani",
      usageProfileKey: resolvedExploitationType,
      flexInterval: expectedFlex,
    },
    {
      label: "Konzervativni",
      usageProfileKey:
        resolvedExploitationType === "rentacar" ? "rentacar" : "fleet_city",
      flexInterval: Math.min(expectedFlex, 20000),
    },
  ];

  return variants.map((variant) => {
    const usageProfile = EXPLOITATION_PROFILES?.[variant.usageProfileKey] || null;

    const simulation = buildSingleScenarioSimulation({
      km: plannedKm,
      contractMonths,
      decoded,
      resolvedVehicle,
      exploitation,
      exploitationType: resolvedExploitationType,
      usageProfileKey: variant.usageProfileKey,
      flexInterval: variant.flexInterval,
      hourlyRate,
      oilPricePerLiter,
      tireCategory,
      laborDiscount,
      partsDiscount,
      oilDiscount,
      scenarioLabel: variant.label,
    });

    return {
      ...simulation,
      usageProfileKey: variant.usageProfileKey,
      usageLabel: usageProfile?.label || variant.usageProfileKey,
      flexInterval: variant.flexInterval,
    };
  });
}

export default buildScenarioComparisonData;