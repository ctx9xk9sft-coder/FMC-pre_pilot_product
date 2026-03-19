import { useMemo } from "react";
import { buildSingleScenarioSimulation } from "../services/scenarioSimulationEngine.js";

export function useMaintenanceScenario({
  decoded,
  resolvedVehicle,
  plannedKm,
  contractMonths,
  exploitation,
  labourRateRsd,
  engineOilPricePerLitreRsd,
  tyreClass,
  flexibleServiceIntervalKm,
}) {
  return useMemo(() => {
    const resolverMissingConfirmations = resolvedVehicle?.missingConfirmations || [];

    const hasPlanningInputs =
      Boolean(decoded?.supported) &&
      Number(plannedKm) > 0 &&
      Number(contractMonths) > 0 &&
      Boolean(exploitation);

    const hasResolvedTechnicalMinimum =
      Boolean(resolvedVehicle?.readyForProvisionalMaintenance) &&
      resolverMissingConfirmations.length === 0 &&
      Boolean(resolvedVehicle?.fields?.engine?.value) &&
      Boolean(
        resolvedVehicle?.fields?.gearbox?.value ||
          resolvedVehicle?.fields?.gearbox?.displayValue
      ) &&
      Boolean(resolvedVehicle?.fields?.drivetrain?.value);

    const canGenerateMaintenancePlan = hasPlanningInputs && hasResolvedTechnicalMinimum;

    const maintenanceGateMessage = !decoded?.supported
      ? decoded?.reason || "VIN nije podržan."
      : !hasPlanningInputs
      ? "Plan održavanja i cena su blokirani dok ne uneseš validnu kilometražu, trajanje ugovora i tip eksploatacije."
      : !canGenerateMaintenancePlan
      ? resolverMissingConfirmations.length > 0
        ? "Plan održavanja i cena se ne generišu dok ne dopuniš obavezna polja u levom panelu."
        : "Plan održavanja i cena su privremeno blokirani dok vozilo ne bude dovoljno potvrđeno."
      : null;

    if (!canGenerateMaintenancePlan) {
      return {
        canGenerateMaintenancePlan,
        maintenanceGateMessage,
        maintenancePlan: null,
        pricedPlan: null,
        totalCost: 0,
        serviceCost: 0,
        brakeCost: 0,
        tyreCost: 0,
        costPerKm: 0,
        costPerMonth: 0,
        totalEvents: 0,
      };
    }

    const simulation = buildSingleScenarioSimulation({
      km: plannedKm,
      contractMonths,
      decoded,
      resolvedVehicle,
      exploitation,
      exploitationType: exploitation?.code || exploitation?.usageProfile || "fleet_standard",
      usageProfileKey: exploitation?.usageProfile || exploitation?.code || "fleet_standard",
      flexInterval: flexibleServiceIntervalKm,
      hourlyRate: labourRateRsd,
      oilPricePerLiter: engineOilPricePerLitreRsd,
      tireCategory: tyreClass,
      serviceRegime: "flex",
    });

    return {
      canGenerateMaintenancePlan,
      maintenanceGateMessage,
      maintenancePlan: simulation?.maintenancePlan || null,
      pricedPlan: simulation?.pricedPlan || null,
      totalCost: simulation?.totalCost ?? 0,
      serviceCost: simulation?.totalServiceCost ?? 0,
      brakeCost: simulation?.totalBrakeCost ?? 0,
      tyreCost: simulation?.totalTireCost ?? 0,
      costPerKm: simulation?.costPerKm ?? 0,
      costPerMonth: simulation?.costPerMonth ?? 0,
      totalEvents: simulation?.eventCount ?? 0,
    };
  }, [
    decoded,
    resolvedVehicle,
    plannedKm,
    contractMonths,
    exploitation,
    labourRateRsd,
    engineOilPricePerLitreRsd,
    tyreClass,
    flexibleServiceIntervalKm,
  ]);
}
