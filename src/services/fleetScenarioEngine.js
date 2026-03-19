import { buildSingleScenarioSimulation } from "./scenarioSimulationEngine.js";

function toNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

export function buildFleetScenarioSummary({ vehicles = [], sharedConfig = {} } = {}) {
  const simulations = vehicles.map((vehicle, index) => {
    const scenario = buildSingleScenarioSimulation({
      ...sharedConfig,
      ...vehicle,
      km: vehicle.km ?? vehicle.plannedKm,
    });

    return {
      fleetIndex: index,
      vin: vehicle?.decoded?.vin || vehicle?.vin || null,
      label: vehicle?.label || scenario?.label || `Vehicle ${index + 1}`,
      ...scenario,
    };
  });

  const totals = simulations.reduce(
    (acc, item) => {
      acc.totalCost += toNumber(item.totalCost);
      acc.totalServiceCost += toNumber(item.totalServiceCost);
      acc.totalBrakeCost += toNumber(item.totalBrakeCost);
      acc.totalTireCost += toNumber(item.totalTireCost);
      acc.totalEvents += toNumber(item.eventCount);
      return acc;
    },
    {
      totalCost: 0,
      totalServiceCost: 0,
      totalBrakeCost: 0,
      totalTireCost: 0,
      totalEvents: 0,
    }
  );

  return {
    simulations,
    totals,
    meta: {
      vehicleCount: simulations.length,
      averageCostPerVehicle: simulations.length ? totals.totalCost / simulations.length : 0,
      engineVersion: "fleet-scenario-engine-v1",
    },
  };
}

export default buildFleetScenarioSummary;
