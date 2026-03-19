import { useMemo, useState } from "react";
import { decodeSkodaVin } from "../services/vinDecoder.js";
import { VEHICLE_PROFILES } from "../data/vehicleProfiles.js";
import { SAMPLE_VINS } from "../data/sampleVins.js";
import { EXPLOITATION_PROFILES } from "../data/exploitationProfiles.js";
import { calculateMaintenanceValidation } from "../services/tcoCalculator.js";
import { resolveVehicleForMaintenance } from "../services/vehicleResolver.js";
import { buildMaintenancePlan } from "../services/buildMaintenancePlan.js";
import { priceMaintenancePlan } from "../services/pricingEngine.js";
import { getVehiclePresentation } from "../data/vehiclePresentation.js";
import { formatRsd, formatNum } from "../utils/formatters.js";
import engineBusinessGroups from "../data/engine_business_groups.json" with { type: 'json' };
import gearboxBusinessGroups from "../data/gearbox_business_groups.json" with { type: 'json' };
import {
  buildScenarioComparisonData,
  buildScenarioVariantRows,
} from "../services/scenarioComparisonEngine.js";
import { computeVehicleConfidence } from "../services/confidence/computeVehicleConfidence.js";
import { computePricingConfidence } from "../services/confidence/computePricingConfidence.js";
import { computeQuoteReadiness } from "../services/confidence/computeQuoteReadiness.js";


function getStatusLabel(status) {
  if (status === "ready_for_planning") return "READY FOR PLANNING";
  if (status === "needs_manual_input") return "NEEDS MANUAL INPUT";
  if (status === "partial_inferred") return "PARTIAL INFERRED";
  if (status === "invalid") return "INVALID";
  return status || "-";
}

function getFieldLabel(field) {
  const map = {
    engine: "Motor",
    gearbox: "Menjač",
    drivetrain: "Pogon",
    model: "Model",
    modelYear: "Godište",
    serviceRegime: "Servisni režim",
    usageProfile: "Tip eksploatacije",
    plannedKm: "Planirana kilometraža",
    contractMonths: "Trajanje ugovora",
  };

  return map[field] || field;
}

function getResolverSourceLabel(source) {
  const map = {
    manual: "Ručna potvrda",
    decoded: "Dekoder",
    decoded_label: "Dekoder label",
    exactVin: "Exact VIN",
    candidate: "Kandidat",
    ambiguous: "Nejasno",
    missing: "Nedostaje",
    vin_pattern_rule: "VIN pattern rule",
    inferred_type_only: "Inferred type only",
    inference: "VIN inference",
  };

  return map[source] || source || "-";
}

function getStatusBadgeStyle(status) {
  if (status === "ready_for_planning") {
    return {
      background: "#dcfce7",
      color: "#166534",
      border: "1px solid #86efac",
    };
  }

  if (status === "needs_manual_input" || status === "partial_inferred") {
    return {
      background: "#fef3c7",
      color: "#92400e",
      border: "1px solid #fcd34d",
    };
  }

  if (status === "invalid") {
    return {
      background: "#fee2e2",
      color: "#991b1b",
      border: "1px solid #fca5a5",
    };
  }

  return {
    background: "#e2e8f0",
    color: "#334155",
    border: "1px solid #cbd5e1",
  };
}

function getPlanStatusUi(canBuildProvisionalPlan, overallStatus) {
  if (canBuildProvisionalPlan) {
    return {
      label: "DOZVOLJEN",
      style: getStatusBadgeStyle(overallStatus),
    };
  }

  return {
    label: "BLOKIRAN",
    style: getStatusBadgeStyle("invalid"),
  };
}

function resolveBusinessEngineLabel(engineCode) {
  if (!engineCode) return "-";

  for (const group of Object.values(engineBusinessGroups)) {
    if (group.engineCodes?.includes(engineCode)) {
      return group.label;
    }
  }

  return engineCode;
}

function resolveBusinessGearboxLabel(gearboxCode) {
  if (!gearboxCode) return "-";

  for (const group of Object.values(gearboxBusinessGroups)) {
    if (group.codes?.includes(gearboxCode)) {
      return group.label;
    }
  }

  const normalized = String(gearboxCode).toUpperCase();
  if (normalized === "DSG") return "Automatik";
  if (normalized === "MANUAL" || normalized === "MANUAL6" || normalized === "MANUAL 6") {
    return "Manual";
  }

  return gearboxCode;
}

export function useFleetCalculatorController() {
  const [sessionUser] = useState({
    displayName: "Admin demo",
    company: "Ćirinac",
  });

  const [viewMode, setViewMode] = useState("business");
  const [selectedProfileId, setSelectedProfileId] = useState("octavia_diesel_dsg");
  const [vin, setVin] = useState(SAMPLE_VINS.octavia_diesel_dsg);
  const [plannedKm, setPlannedKm] = useState(200000);
  const [contractMonths, setContractMonths] = useState(48);
  const [exploitationType, setExploitationType] = useState("fleet_standard");
  const [hourlyRate, setHourlyRate] = useState(5500);
  const [oilPricePerLiter, setOilPricePerLiter] = useState(1800);
  const [tireCategory, setTireCategory] = useState("standard");
  const [flexInterval, setFlexInterval] = useState(25000);
  const [laborDiscount, setLaborDiscount] = useState(0);
  const [partsDiscount, setPartsDiscount] = useState(0);
  const [oilDiscount, setOilDiscount] = useState(0);
  const [engineOverride, setEngineOverride] = useState("");
  const [gearboxOverride, setGearboxOverride] = useState("");
  const [drivetrainOverride, setDrivetrainOverride] = useState("");

  const decoded = useMemo(() => decodeSkodaVin(vin), [vin]);
  const exploitation = EXPLOITATION_PROFILES[exploitationType];
  const annualKm = contractMonths ? (plannedKm / contractMonths) * 12 : 0;

  const validation = useMemo(
    () =>
      calculateMaintenanceValidation({
        decoded,
        exploitation,
        plannedKm,
        contractMonths,
        serviceRegime: "flex",
      }),
    [decoded, exploitation, plannedKm, contractMonths]
  );

  const resolvedVehicle = useMemo(
    () =>
      resolveVehicleForMaintenance({
        decoded,
        validation,
        manualOverrides: {
          engine: engineOverride,
          gearbox: gearboxOverride,
          drivetrain: drivetrainOverride,
        },
      }),
    [decoded, validation, engineOverride, gearboxOverride, drivetrainOverride]
  );

  const resolverMissingConfirmations =
    resolvedVehicle?.missingConfirmations || [];

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

  const canGenerateMaintenancePlan =
    hasPlanningInputs && hasResolvedTechnicalMinimum;

  const maintenancePlanBase = useMemo(() => {
    if (!canGenerateMaintenancePlan) return null;

    return buildMaintenancePlan({
      decoded,
      resolvedVehicle,
      validation,
      planning: {
        plannedKm,
        contractMonths,
        annualKm,
        serviceRegime: "flex",
        usageProfile: exploitationType,
        hourlyRate,
        flexibleServiceIntervalKm: flexInterval,
      },
    });
  }, [
    canGenerateMaintenancePlan,
    decoded,
    resolvedVehicle,
    validation,
    plannedKm,
    contractMonths,
    annualKm,
    exploitationType,
    hourlyRate,
  ]);

  const maintenancePlan = useMemo(() => {
    if (!canGenerateMaintenancePlan || !maintenancePlanBase) return null;

    return priceMaintenancePlan({
      maintenancePlan: maintenancePlanBase,
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
  }, [
    canGenerateMaintenancePlan,
    maintenancePlanBase,
    decoded,
    hourlyRate,
    oilPricePerLiter,
    tireCategory,
    laborDiscount,
    partsDiscount,
    oilDiscount,
  ]);

  const scenarioRows = useMemo(() => {
    if (!canGenerateMaintenancePlan) return [];

    return buildScenarioVariantRows({
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
    });
  }, [
    canGenerateMaintenancePlan,
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
  ]);

  const scenarioComparisonData = useMemo(() => {
    if (!canGenerateMaintenancePlan) return [];

    return buildScenarioComparisonData({
      scenarioKmList: [120000, 150000, 200000],
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
    });
  }, [
    canGenerateMaintenancePlan,
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
  ]);

  const planTotalCost = maintenancePlan?.totals?.totalCost || 0;
  const planTotalService = maintenancePlan?.totals?.totalServiceCost || 0;
  const planTotalBrakes = maintenancePlan?.totals?.totalBrakeCost || 0;
  const planTotalTires = maintenancePlan?.totals?.totalTireCost || 0;
  const planTotalEvents = maintenancePlan?.totals?.totalEvents || 0;
  const planCostPerKm = plannedKm ? planTotalCost / plannedKm : 0;
  const planCostPerMonth = contractMonths ? planTotalCost / contractMonths : 0;

  const serviceEvents =
    maintenancePlan?.events?.filter(
      (event) => event.category !== "brakes" && event.category !== "tires"
    ) || [];
  const brakeEvents =
    maintenancePlan?.events?.filter((event) => event.category === "brakes") || [];
  const tireEvents =
    maintenancePlan?.events?.filter((event) => event.category === "tires") || [];

  const gate = validation || null;
  const vehicleGate = gate?.vehicleGate || null;
  const planningGate = gate?.planningGate || null;
  const missingFields = gate?.missingFields || [];
  const warnings = gate?.warnings || [];

  const overallStatus = !decoded?.supported
    ? "invalid"
    : canGenerateMaintenancePlan
    ? "ready_for_planning"
    : resolverMissingConfirmations.length > 0
    ? "needs_manual_input"
    : gate?.status || resolvedVehicle?.status || "needs_manual_input";

  const canBuildProvisionalPlan = canGenerateMaintenancePlan;

  const finalEngine =
    resolvedVehicle?.fields?.engine?.value || decoded.motorKod || decoded.motor || "-";
  const finalGearbox =
    resolvedVehicle?.fields?.gearbox?.displayValue ||
    resolvedVehicle?.fields?.gearbox?.value ||
    decoded.menjac ||
    "-";
  const finalDrivetrain =
    resolvedVehicle?.fields?.drivetrain?.value || decoded.drivetrain || "-";

  const maintenanceGateMessage = !decoded?.supported
    ? decoded?.reason || "VIN nije podržan."
    : !hasPlanningInputs
    ? "Plan održavanja i cena su blokirani dok ne uneseš validnu kilometražu, trajanje ugovora i tip eksploatacije."
    : !canGenerateMaintenancePlan
    ? resolverMissingConfirmations.length > 0
      ? "Plan održavanja i cena se ne generišu dok ne dopuniš obavezna polja u levom panelu."
      : "Plan održavanja i cena su privremeno blokirani dok vozilo ne bude dovoljno potvrđeno."
    : null;

  const vehicleLabel = `${decoded.marka || "Škoda"} ${decoded.model || "-"}`;
  const exploitationLabel = exploitation?.label || "-";
  const vehiclePresentation = getVehiclePresentation(decoded);
  const showExpertSections = viewMode === "expert";

  const engineLabel =
    viewMode === "business"
      ? resolveBusinessEngineLabel(finalEngine)
      : finalEngine || "-";

  const gearboxLabel =
    viewMode === "business"
      ? resolveBusinessGearboxLabel(finalGearbox)
      : finalGearbox || "-";

  const drivetrainLabel = finalDrivetrain || "-";

  const businessStatusLabel =
    overallStatus === "ready_for_planning"
      ? "Potvrđeno"
      : overallStatus === "needs_manual_input"
      ? "Potrebna potvrda"
      : overallStatus === "invalid"
      ? "Blokirano"
      : "U obradi";

  const totalsDisplay = {
    totalCost: formatRsd(planTotalCost),
    totalService: formatRsd(planTotalService),
    totalBrakes: formatRsd(planTotalBrakes),
    totalTires: formatRsd(planTotalTires),
    costPerKm: formatRsd(planCostPerKm),
    costPerMonth: formatRsd(planCostPerMonth),
    eventCount: String(planTotalEvents),
  };

  const resolverSourceLabels = {
    engine: getResolverSourceLabel(resolvedVehicle?.fields?.engine?.source),
    gearbox: getResolverSourceLabel(resolvedVehicle?.fields?.gearbox?.source),
    drivetrain: getResolverSourceLabel(resolvedVehicle?.fields?.drivetrain?.source),
  };

  const resolverStatusUi = {
    label: getStatusLabel(resolvedVehicle?.status),
    style: getStatusBadgeStyle(resolvedVehicle?.status),
  };

  const vehicleGateUi = {
    label: getStatusLabel(vehicleGate?.status),
    style: getStatusBadgeStyle(vehicleGate?.status),
  };

  const planningGateUi = {
    label: getStatusLabel(planningGate?.status),
    style: getStatusBadgeStyle(planningGate?.status),
  };

  const overallStatusUi = {
    label: getStatusLabel(overallStatus),
    style: getStatusBadgeStyle(overallStatus),
  };

  const planStatusUi = getPlanStatusUi(canBuildProvisionalPlan, overallStatus);

  const vehicleConfidence = useMemo(
    () => computeVehicleConfidence({ resolvedVehicle, decoded }),
    [resolvedVehicle, decoded]
  );

  const pricingConfidence = useMemo(
    () =>
      computePricingConfidence({
        pricingMeta: maintenancePlan?.pricingMeta || null,
      }),
    [maintenancePlan]
  );

  const quoteReadiness = useMemo(
    () =>
      computeQuoteReadiness({
        overallStatus,
        vehicleConfidence,
        pricingConfidence,
        maintenancePlan,
        resolverMissingConfirmations,
      }),
    [
      overallStatus,
      vehicleConfidence,
      pricingConfidence,
      maintenancePlan,
      resolverMissingConfirmations,
    ]
  );

  const quoteReadinessUi = {
    label: quoteReadiness.label,
    style:
      quoteReadiness.status === "READY_FOR_QUOTE"
        ? getStatusBadgeStyle("ready_for_planning")
        : quoteReadiness.status === "ESTIMATE_ONLY"
        ? getStatusBadgeStyle("needs_manual_input")
        : getStatusBadgeStyle("invalid"),
  };

  const explainabilityNotes = [
    ...(maintenancePlan?.meta?.assumptions || []),
    ...(maintenancePlan?.meta?.ruleValidationWarnings || []),
    ...(quoteReadiness?.warnings || []),
  ].filter(Boolean);


  const loadProfile = (profileId) => {
    setSelectedProfileId(profileId);
    setVin(SAMPLE_VINS[profileId]);
    setHourlyRate(VEHICLE_PROFILES[profileId]?.hourlyRate || 5500);
    setEngineOverride("");
    setGearboxOverride("");
    setDrivetrainOverride("");
  };

  return {
    sessionUser,
    viewMode,
    setViewMode,
    selectedProfileId,
    loadProfile,
    vin,
    setVin,
    plannedKm,
    setPlannedKm,
    contractMonths,
    setContractMonths,
    exploitationType,
    setExploitationType,
    hourlyRate,
    setHourlyRate,
    oilPricePerLiter,
    setOilPricePerLiter,
    tireCategory,
    setTireCategory,
    flexInterval,
    setFlexInterval,
    laborDiscount,
    setLaborDiscount,
    partsDiscount,
    setPartsDiscount,
    oilDiscount,
    setOilDiscount,
    engineOverride,
    setEngineOverride,
    gearboxOverride,
    setGearboxOverride,
    drivetrainOverride,
    setDrivetrainOverride,
    decoded,
    exploitation,
    annualKm,
    validation,
    resolvedVehicle,
    resolverMissingConfirmations,
    canGenerateMaintenancePlan,
    maintenancePlan,
    scenarioRows,
    scenarioComparisonData,
    gate,
    vehicleGate,
    planningGate,
    missingFields,
    warnings,
    overallStatus,
    canBuildProvisionalPlan,
    finalEngine,
    finalGearbox,
    finalDrivetrain,
    maintenanceGateMessage,
    vehicleLabel,
    exploitationLabel,
    vehiclePresentation,
    showExpertSections,
    engineLabel,
    gearboxLabel,
    drivetrainLabel,
    businessStatusLabel,
    totalsDisplay,
    resolverSourceLabels,
    resolverStatusUi,
    vehicleGateUi,
    planningGateUi,
    overallStatusUi,
    planStatusUi,
    vehicleConfidence,
    pricingConfidence,
    quoteReadiness,
    quoteReadinessUi,
    explainabilityNotes,
    getFieldLabel,
    planTotalCost,
    planTotalService,
    planTotalBrakes,
    planTotalTires,
    planTotalEvents,
    planCostPerKm,
    planCostPerMonth,
    serviceEvents,
    brakeEvents,
    tireEvents,
    EXPLOITATION_PROFILES,
  };
}