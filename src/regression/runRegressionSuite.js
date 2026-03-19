import { GOLDEN_CASES, CROSS_CASE_ASSERTIONS } from "./goldenCases.js";
import { decodeSkodaVin } from "../services/vinDecoder.js";
import { calculateMaintenanceValidation } from "../services/tcoCalculator.js";
import { EXPLOITATION_PROFILES } from "../data/exploitationProfiles.js";
import { resolveVehicleForMaintenance } from "../services/vehicleResolver.js";
import { buildScenarioComparisonData } from "../services/scenarioComparisonEngine.js";
import { buildSingleScenarioSimulation } from "../services/scenarioSimulationEngine.js";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function stableStringify(value) {
  return JSON.stringify(value, Object.keys(value).sort());
}

function sortStrings(values = []) {
  return [...values].map(String).sort();
}

function pickScenarioSummary(rows = []) {
  return rows.map((row) => ({
    km: Number(row?.km || 0),
    totalCost: Number(row?.totalCost || 0),
    eventCount: Number(row?.eventCount || 0),
    costPerKm: Number((row?.costPerKm || 0).toFixed(4)),
    costPerMonth: Number((row?.costPerMonth || 0).toFixed(4)),
  }));
}

function pickResultSummary(result) {
  return {
    supported: Boolean(result?.decoded?.supported),
    decodedModel: result?.decoded?.model || null,
    status: result?.resolvedVehicle?.status || result?.validation?.status || "invalid",
    internalStatus:
      result?.resolvedVehicle?.internalStatus ||
      result?.resolvedVehicle?.resolutionSummary?.internalStatus ||
      result?.validation?.overallEnhancedStatus ||
      null,
    operationalReadiness:
      result?.resolvedVehicle?.operationalReadiness ||
      result?.resolvedVehicle?.resolutionSummary?.operationalReadiness ||
      null,
    resolutionStatus: result?.resolvedVehicle?.resolutionStatus || null,
    missingConfirmations: sortStrings(result?.resolvedVehicle?.missingConfirmations || []),
    engine: result?.resolvedVehicle?.fields?.engine?.value || null,
    gearboxDisplay:
      result?.resolvedVehicle?.fields?.gearbox?.displayValue ||
      result?.resolvedVehicle?.fields?.gearbox?.value ||
      null,
    drivetrain: result?.resolvedVehicle?.fields?.drivetrain?.value || null,
    canPlan: Boolean(result?.canPlan),
    totals: result?.pricedPlan
      ? {
          totalCost: Number(result.pricedPlan?.totals?.totalCost || 0),
          totalEvents: Number(result.pricedPlan?.totals?.totalEvents || 0),
          totalServiceCost: Number(result.pricedPlan?.totals?.totalServiceCost || 0),
          totalBrakeCost: Number(result.pricedPlan?.totals?.totalBrakeCost || 0),
          totalTireCost: Number(result.pricedPlan?.totals?.totalTireCost || 0),
          costPerKm: Number(
            ((result.pricedPlan?.totals?.totalCost || 0) / (result.input.plannedKm || 1)).toFixed(4)
          ),
        }
      : null,
    scenarioSummary: pickScenarioSummary(result?.scenarioRows || []),
    maintenanceMeta: result?.maintenancePlan
      ? {
          planReadiness: result?.maintenancePlan?.meta?.planReadiness || null,
          confirmedEventsCount: Number(result?.maintenancePlan?.meta?.confirmedEventsCount || 0),
          estimatedEventsCount: Number(result?.maintenancePlan?.meta?.estimatedEventsCount || 0),
          pricingStatus: result?.pricedPlan?.meta?.pricingStatus || null,
        }
      : null,
  };
}

function runSingleCase(test) {
  const decoded = test.mockDecoded || decodeSkodaVin(test.vin);
  const exploitation =
    EXPLOITATION_PROFILES[test.exploitationType] ||
    EXPLOITATION_PROFILES.fleet_standard;

  const validation = calculateMaintenanceValidation({
    decoded,
    exploitation,
    plannedKm: test.plannedKm,
    contractMonths: test.contractMonths,
    serviceRegime: "flex",
  });

  const resolvedVehicle = resolveVehicleForMaintenance({
    decoded,
    validation,
    manualOverrides: test.manualOverrides || {},
  });

  const canPlan =
    Boolean(decoded?.supported) &&
    resolvedVehicle?.status === "ready_for_planning";

  let maintenancePlan = null;
  let pricedPlan = null;
  let scenarioRows = [];

  if (canPlan) {
    const mainSimulation = buildSingleScenarioSimulation({
      km: test.plannedKm,
      contractMonths: test.contractMonths,
      decoded,
      resolvedVehicle,
      exploitation,
      exploitationType: test.exploitationType,
      usageProfileKey: test.exploitationType,
      hourlyRate: test.pricing?.laborRate || 5500,
      oilPricePerLiter: test.pricing?.oilPricePerLiter || 1800,
      tireCategory: test.pricing?.tireCategory || "standard",
      laborDiscount: test.pricing?.laborDiscount || 0,
      partsDiscount: test.pricing?.partsDiscount || 0,
      oilDiscount: test.pricing?.oilDiscount || 0,
      serviceRegime: "flex",
    });

    maintenancePlan = mainSimulation?.maintenancePlan || null;
    pricedPlan = mainSimulation?.pricedPlan || null;

    scenarioRows = buildScenarioComparisonData({
      scenarioKmList: test.scenarioKmList || [120000, 150000, 200000],
      contractMonths: test.contractMonths,
      decoded,
      resolvedVehicle,
      exploitation,
      exploitationType: test.exploitationType,
      hourlyRate: test.pricing?.laborRate || 5500,
      oilPricePerLiter: test.pricing?.oilPricePerLiter || 1800,
      tireCategory: test.pricing?.tireCategory || "standard",
      laborDiscount: test.pricing?.laborDiscount || 0,
      partsDiscount: test.pricing?.partsDiscount || 0,
      oilDiscount: test.pricing?.oilDiscount || 0,
    });
  }

  return {
    input: test,
    decoded,
    validation,
    resolvedVehicle,
    canPlan,
    maintenancePlan,
    pricedPlan,
    scenarioRows,
  };
}

function assertScenarioMonotonicity(rows = []) {
  for (let i = 1; i < rows.length; i += 1) {
    const previous = rows[i - 1];
    const current = rows[i];

    assert(current.km >= previous.km, "Scenario KM list must be sorted ascending.");
    assert(
      current.totalCost >= previous.totalCost,
      `Scenario total cost must be monotonic. ${current.km}km is cheaper than ${previous.km}km.`
    );
    assert(
      current.eventCount >= previous.eventCount,
      `Scenario event count must be monotonic. ${current.km}km has fewer events than ${previous.km}km.`
    );
  }
}

function assertPlannedKmMatchesScenario(result) {
  if (!result?.canPlan) return;

  const scenarioRows = result?.scenarioRows || [];
  const plannedKm = Number(result?.input?.plannedKm || 0);
  const matchingRow = scenarioRows.find((row) => Number(row?.km || 0) === plannedKm);

  if (!matchingRow) return;

  assert(
    Number(result?.pricedPlan?.totals?.totalCost || 0) === Number(matchingRow?.totalCost || 0),
    `Main plan total cost (${result?.pricedPlan?.totals?.totalCost}) must equal matching scenario total (${matchingRow?.totalCost}).`
  );
  assert(
    Number(result?.pricedPlan?.totals?.totalEvents || 0) === Number(matchingRow?.eventCount || 0),
    `Main plan total events (${result?.pricedPlan?.totals?.totalEvents}) must equal matching scenario events (${matchingRow?.eventCount}).`
  );
}

function assertExpectedCase(result, expected = {}) {
  const summary = pickResultSummary(result);

  if (typeof expected.supported === "boolean") {
    assert(summary.supported === expected.supported, `Expected supported=${expected.supported}, got ${summary.supported}`);
  }

  if (expected.status) {
    assert(summary.status === expected.status, `Expected status=${expected.status}, got ${summary.status}`);
  }

  if (expected.resolutionStatus) {
    assert(
      summary.resolutionStatus === expected.resolutionStatus,
      `Expected resolutionStatus=${expected.resolutionStatus}, got ${summary.resolutionStatus}`
    );
  }
  if (expected.internalStatus) {
    assert(summary.internalStatus === expected.internalStatus, `Expected internalStatus=${expected.internalStatus}, got ${summary.internalStatus}`);
  }

  if (expected.operationalReadiness) {
    assert(
      summary.operationalReadiness === expected.operationalReadiness,
      `Expected operationalReadiness=${expected.operationalReadiness}, got ${summary.operationalReadiness}`
    );
  }

  if (Array.isArray(expected.missingConfirmations)) {
    const actual = sortStrings(summary.missingConfirmations);
    const wanted = sortStrings(expected.missingConfirmations);
    assert(
      stableStringify(actual) === stableStringify(wanted),
      `Expected missingConfirmations=${wanted.join(", ")}, got ${actual.join(", ") || "-"}`
    );
  }

  if (typeof expected.canPlan === "boolean") {
    assert(summary.canPlan === expected.canPlan, `Expected canPlan=${expected.canPlan}, got ${summary.canPlan}`);
  }

  if (expected.engine) {
    assert(summary.engine === expected.engine, `Expected engine=${expected.engine}, got ${summary.engine}`);
  }

  if (expected.gearboxDisplay) {
    assert(
      summary.gearboxDisplay === expected.gearboxDisplay,
      `Expected gearboxDisplay=${expected.gearboxDisplay}, got ${summary.gearboxDisplay}`
    );
  }

  if (expected.drivetrain) {
    assert(summary.drivetrain === expected.drivetrain, `Expected drivetrain=${expected.drivetrain}, got ${summary.drivetrain}`);
  }

  if (summary.canPlan) {
    assert(result?.maintenancePlan, "Maintenance plan should exist for a plannable case.");
    assert(result?.pricedPlan, "Priced plan should exist for a plannable case.");
    assert(Array.isArray(result?.maintenancePlan?.events), "Maintenance plan events must be an array.");
    assert(Array.isArray(result?.pricedPlan?.events), "Priced plan events must be an array.");
    assert(result?.pricedPlan?.totals?.totalCost > 0, "Total cost must be > 0 for a plannable case.");
    assert(["priced", "partial"].includes(result?.pricedPlan?.meta?.pricingStatus), "Pricing engine should mark plan as priced or partial.");

    if (typeof expected.totalCost === "number") {
      assert(
        Number(result?.pricedPlan?.totals?.totalCost || 0) === expected.totalCost,
        `Expected totalCost=${expected.totalCost}, got ${result?.pricedPlan?.totals?.totalCost}`
      );
    }

    if (typeof expected.totalEvents === "number") {
      assert(
        Number(result?.pricedPlan?.totals?.totalEvents || 0) === expected.totalEvents,
        `Expected totalEvents=${expected.totalEvents}, got ${result?.pricedPlan?.totals?.totalEvents}`
      );
    }

    if (typeof expected.totalServiceCost === "number") {
      assert(
        Number(result?.pricedPlan?.totals?.totalServiceCost || 0) === expected.totalServiceCost,
        `Expected totalServiceCost=${expected.totalServiceCost}, got ${result?.pricedPlan?.totals?.totalServiceCost}`
      );
    }

    if (typeof expected.totalBrakeCost === "number") {
      assert(
        Number(result?.pricedPlan?.totals?.totalBrakeCost || 0) === expected.totalBrakeCost,
        `Expected totalBrakeCost=${expected.totalBrakeCost}, got ${result?.pricedPlan?.totals?.totalBrakeCost}`
      );
    }

    if (typeof expected.totalTireCost === "number") {
      assert(
        Number(result?.pricedPlan?.totals?.totalTireCost || 0) === expected.totalTireCost,
        `Expected totalTireCost=${expected.totalTireCost}, got ${result?.pricedPlan?.totals?.totalTireCost}`
      );
    }

    if (typeof expected.minimumTotalCost === "number") {
      assert(
        Number(result?.pricedPlan?.totals?.totalCost || 0) >= expected.minimumTotalCost,
        `Expected totalCost >= ${expected.minimumTotalCost}, got ${result?.pricedPlan?.totals?.totalCost}`
      );
    }

    if (typeof expected.minimumEvents === "number") {
      assert(
        Number(result?.pricedPlan?.totals?.totalEvents || 0) >= expected.minimumEvents,
        `Expected totalEvents >= ${expected.minimumEvents}, got ${result?.pricedPlan?.totals?.totalEvents}`
      );
    }
    if (expected.planReadiness) {
      assert(
        result?.maintenancePlan?.meta?.planReadiness === expected.planReadiness,
        `Expected planReadiness=${expected.planReadiness}, got ${result?.maintenancePlan?.meta?.planReadiness}`
      );
    }

    if (typeof expected.minimumConfirmedEvents === "number") {
      assert(
        Number(result?.maintenancePlan?.meta?.confirmedEventsCount || 0) >= expected.minimumConfirmedEvents,
        `Expected confirmedEventsCount >= ${expected.minimumConfirmedEvents}, got ${result?.maintenancePlan?.meta?.confirmedEventsCount}`
      );
    }

    if (typeof expected.minimumEstimatedEvents === "number") {
      assert(
        Number(result?.maintenancePlan?.meta?.estimatedEventsCount || 0) >= expected.minimumEstimatedEvents,
        `Expected estimatedEventsCount >= ${expected.minimumEstimatedEvents}, got ${result?.maintenancePlan?.meta?.estimatedEventsCount}`
      );
    }

    if (Array.isArray(expected.requiredEventTypes) && expected.requiredEventTypes.length > 0) {
      const eventTypes = (result?.maintenancePlan?.events || []).map((event) => event?.type).filter(Boolean);
      expected.requiredEventTypes.forEach((requiredType) => {
        assert(eventTypes.includes(requiredType), `Required event type ${requiredType} was not generated.`);
      });
    }

    if (Array.isArray(expected.forbiddenEventTypes) && expected.forbiddenEventTypes.length > 0) {
      const eventTypes = (result?.maintenancePlan?.events || []).map((event) => event?.type).filter(Boolean);
      expected.forbiddenEventTypes.forEach((forbiddenType) => {
        assert(!eventTypes.includes(forbiddenType), `Forbidden event type ${forbiddenType} was generated.`);
      });
    }

    if (typeof expected.minimumCostPerKm === "number") {
      const costPerKm = Number(result?.pricedPlan?.totals?.totalCost || 0) / Number(result?.input?.plannedKm || 1);
      assert(
        costPerKm >= expected.minimumCostPerKm,
        `Expected costPerKm >= ${expected.minimumCostPerKm}, got ${costPerKm.toFixed(4)}`
      );
    }

    assertScenarioMonotonicity(result?.scenarioRows || []);
    assertPlannedKmMatchesScenario(result);

    if (Array.isArray(expected.scenarioRows)) {
      const actual = pickScenarioSummary(result?.scenarioRows || []).map((row) => ({
        km: row.km,
        totalCost: row.totalCost,
        eventCount: row.eventCount,
      }));

      assert(
        stableStringify(actual) === stableStringify(expected.scenarioRows),
        `Scenario rows mismatch. Expected ${JSON.stringify(expected.scenarioRows)}, got ${JSON.stringify(actual)}`
      );
    }
  } else {
    assert(!result?.maintenancePlan, "Blocked case should not build maintenance plan.");
    assert(!result?.pricedPlan, "Blocked case should not build priced plan.");
  }
}

function runDeterminismCheck(test) {
  const first = runSingleCase(test);
  const second = runSingleCase(test);

  const firstSummary = pickResultSummary(first);
  const secondSummary = pickResultSummary(second);

  assert(
    stableStringify(firstSummary) === stableStringify(secondSummary),
    `Determinism check failed.\nFirst: ${JSON.stringify(firstSummary, null, 2)}\nSecond: ${JSON.stringify(secondSummary, null, 2)}`
  );

  return first;
}

function assertCrossCase(resultsById) {
  for (const rule of CROSS_CASE_ASSERTIONS) {
    const left = resultsById.get(rule.leftCaseId);
    const right = resultsById.get(rule.rightCaseId);

    assert(left, `Cross-case assertion missing left case: ${rule.leftCaseId}`);
    assert(right, `Cross-case assertion missing right case: ${rule.rightCaseId}`);

    const leftCostPerKm = Number(left?.pricedPlan?.totals?.totalCost || 0) / Number(left?.input?.plannedKm || 1);
    const rightCostPerKm = Number(right?.pricedPlan?.totals?.totalCost || 0) / Number(right?.input?.plannedKm || 1);
    const leftEvents = Number(left?.pricedPlan?.totals?.totalEvents || 0);
    const rightEvents = Number(right?.pricedPlan?.totals?.totalEvents || 0);

    if (rule.assertion === "costPerKmGreater") {
      assert(leftCostPerKm > rightCostPerKm, `${rule.message} Left=${leftCostPerKm.toFixed(4)}, Right=${rightCostPerKm.toFixed(4)}`);
    } else if (rule.assertion === "eventCountGreaterOrEqual") {
      assert(leftEvents >= rightEvents, `${rule.message} Left=${leftEvents}, Right=${rightEvents}`);
    } else {
      throw new Error(`Unsupported cross-case assertion: ${rule.assertion}`);
    }
  }
}

export function runRegressionSuite() {
  let pass = 0;
  let fail = 0;
  const resultsById = new Map();

  console.log("Running hardened regression suite (phase 5)...\n");

  for (const test of GOLDEN_CASES) {
    try {
      const result = runDeterminismCheck(test);
      assertExpectedCase(result, test.expected || {});
      resultsById.set(test.id, result);
      pass += 1;
      console.log(`✅ PASS: ${test.name}`);
    } catch (error) {
      fail += 1;

      let debugSummary = null;
      try {
        const debugResult = runSingleCase(test);
        debugSummary = pickResultSummary(debugResult);
      } catch (debugError) {
        debugSummary = { debugError: debugError.message };
      }

      console.log(`❌ FAIL: ${test.name} -> ${error.message}`);
      console.log("   ACTUAL:", JSON.stringify(debugSummary, null, 2));
    }
  }

  try {
    assertCrossCase(resultsById);
    console.log("✅ PASS: cross-case assertions");
  } catch (error) {
    fail += 1;
    console.log(`❌ FAIL: cross-case assertions -> ${error.message}`);
  }

  console.log("\nRESULT:");
  console.log(`PASS: ${pass}`);
  console.log(`FAIL: ${fail}`);

  if (fail > 0) {
    process.exitCode = 1;
  }

  return { pass, fail };
}

export default runRegressionSuite;