import fs from 'fs';
import path from 'path';

import vinTrainingDataset from '../src/data/vin_training_dataset.json' with { type: 'json' };
import runtimeTestVins from '../src/data/runtime_test_vins.json' with { type: 'json' };
import { decodeSkodaVin } from '../src/services/vinDecoder.js';
import { calculateMaintenanceValidation } from '../src/services/tcoCalculator.js';
import { resolveVehicleForMaintenance } from '../src/services/vehicleResolver.js';
import { buildSingleScenarioSimulation } from '../src/services/scenarioSimulationEngine.js';
import { computePricingConfidence } from '../src/services/confidence/computePricingConfidence.js';
import { EXPLOITATION_PROFILES } from '../src/data/exploitationProfiles.js';

const DEFAULT_PLANNED_KM = 150000;
const DEFAULT_CONTRACT_MONTHS = 48;
const DEFAULT_EXPLOITATION_TYPE = 'fleet_standard';
const DEFAULT_LABOR_RATE = 5500;
const DEFAULT_OIL_PRICE_PER_LITER = 1800;
const DEFAULT_TIRE_CATEGORY = 'standard';
const OUTPUT_PATH = path.resolve('reports/runtime_coverage_audit.json');

function clean(value) {
  const text = String(value ?? '').trim();
  return text || null;
}

function numberOrNull(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function incrementCounter(map, key, amount = 1) {
  const resolvedKey = clean(key) || 'unknown';
  map.set(resolvedKey, (map.get(resolvedKey) || 0) + amount);
}

function incrementNestedCounter(rootMap, bucket, key, amount = 1) {
  const resolvedBucket = clean(bucket) || 'unknown';
  if (!rootMap.has(resolvedBucket)) {
    rootMap.set(resolvedBucket, new Map());
  }
  incrementCounter(rootMap.get(resolvedBucket), key, amount);
}

function mapToSortedEntries(map, valueKey = 'count', limit = null) {
  const entries = [...map.entries()]
    .map(([key, value]) => ({ key, [valueKey]: value }))
    .sort((a, b) => {
      if (b[valueKey] !== a[valueKey]) return b[valueKey] - a[valueKey];
      return String(a.key).localeCompare(String(b.key));
    });

  return Number.isFinite(limit) ? entries.slice(0, limit) : entries;
}

function nestedMapToPlainObject(rootMap) {
  return Object.fromEntries(
    [...rootMap.entries()].map(([bucket, nested]) => [bucket, Object.fromEntries(mapToSortedEntries(nested).map((entry) => [entry.key, entry.count]))])
  );
}

function topBucketsForStatus(clusterMap, status, limit = 10) {
  return mapToSortedEntries(clusterMap.get(status) || new Map(), 'count', limit);
}

function deriveBodyCode(vin, decoded) {
  return (
    clean(decoded?.decoderMeta?.parsedCore?.segments?.fullBodyCode) ||
    clean(decoded?.enrichment?.exactVinMatch?.bodyCode) ||
    (clean(vin) && String(vin).length >= 6 ? String(vin).slice(3, 6).toUpperCase() : null)
  );
}

function derivePlatformCode(vin, decoded) {
  return (
    clean(decoded?.decoderMeta?.parsedCore?.segments?.platformCode) ||
    clean(decoded?.enrichment?.exactVinMatch?.modelCode) ||
    (clean(vin) && String(vin).length >= 8 ? String(vin).slice(6, 8).toUpperCase() : null)
  );
}

function deriveGeneration(decoded) {
  return (
    clean(decoded?.vin_summary?.generation) ||
    clean(decoded?.model_info?.generation) ||
    null
  );
}

function deriveModel(decoded, resolvedVehicle) {
  return (
    clean(decoded?.vin_summary?.model) ||
    clean(decoded?.model_info?.name) ||
    clean(resolvedVehicle?.fields?.model?.value) ||
    clean(decoded?.model) ||
    null
  );
}

function deriveEngineFamily(decoded) {
  return (
    clean(decoded?.enrichment?.selectedEngine?.family) ||
    clean(decoded?.enrichment?.masterEngine?.family) ||
    clean(decoded?.engine?.family) ||
    clean(decoded?.enrichment?.exactVinMatch?.engineUnitCode) ||
    null
  );
}

function deriveGearboxFamily(decoded, resolvedVehicle) {
  return (
    clean(resolvedVehicle?.fields?.gearbox?.semantic?.family) ||
    clean(decoded?.enrichment?.selectedGearbox?.family) ||
    clean(decoded?.enrichment?.masterGearbox?.family) ||
    null
  );
}

function buildClusterKey(dims) {
  return [
    dims.model,
    dims.generation,
    dims.bodyCode,
    dims.platformCode,
    dims.modelYear,
    dims.engineFamily,
    dims.gearboxFamily,
  ]
    .map((value) => clean(value) || 'unknown')
    .join(' | ');
}

function collectBreakdowns(records, field) {
  const bucketMap = new Map();

  for (const record of records) {
    incrementCounter(bucketMap, record[field]);
  }

  return mapToSortedEntries(bucketMap);
}

function collectStatusByDimension(records, field) {
  const root = new Map();

  for (const record of records) {
    incrementNestedCounter(root, record[field], record.internalStatus);
  }

  return nestedMapToPlainObject(root);
}

function createPricingAccumulator() {
  return {
    auditedPricedVinCount: 0,
    missingPriceEventCount: 0,
    genericEventCount: 0,
    familyEventCount: 0,
    fallbackLineCount: 0,
    missingLineCount: 0,
    pricedCountByConfidence: new Map(),
    weakPricingClusters: new Map(),
  };
}

function updatePricingAccumulator(accumulator, record) {
  if (!record.pricingObserved) {
    return;
  }

  accumulator.auditedPricedVinCount += 1;
  accumulator.missingPriceEventCount += Number(record.pricingMeta?.missingPriceEventCount || 0);
  accumulator.genericEventCount += Number(record.pricingMeta?.genericEventCount || 0);
  accumulator.familyEventCount += Number(record.pricingMeta?.familyEventCount || 0);
  accumulator.fallbackLineCount += Number(record.pricingMeta?.fallbackLineCount || 0);
  accumulator.missingLineCount += Number(record.pricingMeta?.missingLineCount || 0);
  incrementCounter(accumulator.pricedCountByConfidence, record.pricingConfidenceLevel);

  if (['low', 'medium'].includes(record.pricingConfidenceLevel)) {
    incrementCounter(accumulator.weakPricingClusters, record.clusterKey);
  }
}

function gatherRecords(datasetRows) {
  const records = [];
  const exploitation = EXPLOITATION_PROFILES[DEFAULT_EXPLOITATION_TYPE] || EXPLOITATION_PROFILES.fleet_standard;

for (const item of datasetRows) {
  const vin = clean(typeof item === 'string' ? item : item?.vin); 

    const decoded = decodeSkodaVin(vin);
    const validation = calculateMaintenanceValidation({
      decoded,
      exploitation,
      plannedKm: DEFAULT_PLANNED_KM,
      contractMonths: DEFAULT_CONTRACT_MONTHS,
      serviceRegime: 'flex',
    });
    const resolvedVehicle = resolveVehicleForMaintenance({ vin, decoded, validation });

    const canRunPricingFlow = Boolean(decoded?.supported) && resolvedVehicle?.status === 'ready_for_planning';
    const scenario = canRunPricingFlow
      ? buildSingleScenarioSimulation({
          km: DEFAULT_PLANNED_KM,
          contractMonths: DEFAULT_CONTRACT_MONTHS,
          decoded,
          resolvedVehicle,
          exploitation,
          exploitationType: DEFAULT_EXPLOITATION_TYPE,
          usageProfileKey: DEFAULT_EXPLOITATION_TYPE,
          hourlyRate: DEFAULT_LABOR_RATE,
          oilPricePerLiter: DEFAULT_OIL_PRICE_PER_LITER,
          tireCategory: DEFAULT_TIRE_CATEGORY,
          serviceRegime: 'flex',
        })
      : null;

    const pricingMeta = scenario?.pricedPlan?.pricingMeta || null;
    const pricingConfidence = pricingMeta ? computePricingConfidence({ pricingMeta }) : null;
    const gearboxSemantic = resolvedVehicle?.fields?.gearbox?.semantic || null;

    const record = {
      vin,
      model: deriveModel(decoded, resolvedVehicle),
      generation: deriveGeneration(decoded),
      bodyCode: deriveBodyCode(vin, decoded),
      platformCode: derivePlatformCode(vin, decoded),
      modelYear: numberOrNull(decoded?.modelYear) ?? numberOrNull(decoded?.vin_summary?.model_year) ?? numberOrNull(row?.modelYear),
      engineFamily: deriveEngineFamily(decoded),
      gearboxFamily: deriveGearboxFamily(decoded, resolvedVehicle),
      canonicalStatus: clean(resolvedVehicle?.status) || 'invalid',
      internalStatus: clean(resolvedVehicle?.internalStatus) || 'invalid',
      operationalReadiness: clean(resolvedVehicle?.operationalReadiness) || 'blocked',
      resolutionStatus: clean(resolvedVehicle?.resolutionStatus) || 'unresolved',
      validationStatus: clean(validation?.status) || 'unknown',
      missingConfirmations: resolvedVehicle?.missingConfirmations || [],
      warnings: resolvedVehicle?.warnings || [],
      inferredEngine: Boolean(resolvedVehicle?.inferredEngine),
      inferredGearbox: Boolean(resolvedVehicle?.inferredGearbox),
      gearboxResolved: Boolean(resolvedVehicle?.fields?.gearbox?.resolved),
      gearboxClosureMissing: !Boolean(
        gearboxSemantic &&
          (gearboxSemantic.familyClosed || gearboxSemantic.transmissionTypeClosed) &&
          !gearboxSemantic.hasConflict
      ),
      gearboxClosure: gearboxSemantic
        ? {
            family: gearboxSemantic.family || null,
            transmissionType: gearboxSemantic.transmissionType || null,
            maintenanceGroup: gearboxSemantic.maintenanceGroup || null,
            familyClosed: Boolean(gearboxSemantic.familyClosed),
            transmissionTypeClosed: Boolean(gearboxSemantic.transmissionTypeClosed),
            hasConflict: Boolean(gearboxSemantic.hasConflict),
          }
        : null,
      pricingObserved: Boolean(pricingMeta),
      pricingStatus: clean(scenario?.pricedPlan?.meta?.pricingStatus) || null,
      pricingMeta,
      pricingConfidenceLevel: clean(pricingConfidence?.level) || null,
      pricingConfidenceMetrics: pricingConfidence?.metrics || null,
      totalPlannedEvents: numberOrNull(scenario?.pricedPlan?.totals?.totalEvents),
      totalPlannedCost: numberOrNull(scenario?.pricedPlan?.totals?.totalCost),
      priceRange: pricingMeta?.pricingRange || null,
    };

    record.clusterKey = buildClusterKey(record);
    records.push(record);
  }

  return records;
}

function buildReport(records) {
  const totals = {
    totalVinsAudited: records.length,
    internalStatus: Object.fromEntries(mapToSortedEntries(records.reduce((map, record) => {
      incrementCounter(map, record.internalStatus);
      return map;
    }, new Map())).map((entry) => [entry.key, entry.count])),
    canonicalStatus: Object.fromEntries(mapToSortedEntries(records.reduce((map, record) => {
      incrementCounter(map, record.canonicalStatus);
      return map;
    }, new Map())).map((entry) => [entry.key, entry.count])),
    operationalReadiness: Object.fromEntries(mapToSortedEntries(records.reduce((map, record) => {
      incrementCounter(map, record.operationalReadiness);
      return map;
    }, new Map())).map((entry) => [entry.key, entry.count])),
    resolutionStatus: Object.fromEntries(mapToSortedEntries(records.reduce((map, record) => {
      incrementCounter(map, record.resolutionStatus);
      return map;
    }, new Map())).map((entry) => [entry.key, entry.count])),
  };

  const pricingAccumulator = createPricingAccumulator();
  const statusClusters = new Map();
  const readinessClusters = new Map();
  const gearboxClosureClusters = new Map();

  for (const record of records) {
    incrementNestedCounter(statusClusters, record.internalStatus, record.clusterKey);
    incrementNestedCounter(readinessClusters, record.operationalReadiness, record.clusterKey);
    if (record.gearboxClosureMissing) {
      incrementCounter(gearboxClosureClusters, record.clusterKey);
    }
    updatePricingAccumulator(pricingAccumulator, record);
  }

  return {
    generatedAt: new Date().toISOString(),
    runtimeModulesUsed: [
      'src/services/vinDecoder.js#decodeSkodaVin',
      'src/services/tcoCalculator.js#calculateMaintenanceValidation',
      'src/services/vehicleResolver.js#resolveVehicleForMaintenance',
      'src/services/scenarioSimulationEngine.js#buildSingleScenarioSimulation',
      'src/services/confidence/computePricingConfidence.js#computePricingConfidence',
    ],
    auditInput: {
      source: 'src/data/vin_training_dataset.json',
      semantics: 'VIN list only; all statuses below come from runtime decode/resolution/planning execution.',
      defaultScenario: {
        exploitationType: DEFAULT_EXPLOITATION_TYPE,
        plannedKm: DEFAULT_PLANNED_KM,
        contractMonths: DEFAULT_CONTRACT_MONTHS,
        laborRate: DEFAULT_LABOR_RATE,
        oilPricePerLiter: DEFAULT_OIL_PRICE_PER_LITER,
        tireCategory: DEFAULT_TIRE_CATEGORY,
      },
    },
    totals,
    breakdowns: {
      byModel: collectStatusByDimension(records, 'model'),
      byGeneration: collectStatusByDimension(records, 'generation'),
      byBodyCode: collectStatusByDimension(records, 'bodyCode'),
      byPlatformCode: collectStatusByDimension(records, 'platformCode'),
      byModelYear: collectStatusByDimension(records, 'modelYear'),
      byEngineFamily: collectStatusByDimension(records, 'engineFamily'),
      byGearboxFamily: collectStatusByDimension(records, 'gearboxFamily'),
    },
    topClusters: {
      partialInferred: topBucketsForStatus(statusClusters, 'partial_inferred'),
      needsManualInput: topBucketsForStatus(statusClusters, 'needs_manual_input'),
      blocked: topBucketsForStatus(readinessClusters, 'blocked'),
      gearboxUnresolved: mapToSortedEntries(gearboxClosureClusters, 'count', 10),
      weakPricingConfidence: mapToSortedEntries(pricingAccumulator.weakPricingClusters, 'count', 10),
    },
    pricingObservability: pricingAccumulator.auditedPricedVinCount > 0
      ? {
          included: true,
          auditedPricedVinCount: pricingAccumulator.auditedPricedVinCount,
          missingPriceEventCount: pricingAccumulator.missingPriceEventCount,
          genericEventCount: pricingAccumulator.genericEventCount,
          familyEventCount: pricingAccumulator.familyEventCount,
          fallbackLineCount: pricingAccumulator.fallbackLineCount,
          missingLineCount: pricingAccumulator.missingLineCount,
          pricingConfidenceBuckets: Object.fromEntries(
            mapToSortedEntries(pricingAccumulator.pricedCountByConfidence).map((entry) => [entry.key, entry.count])
          ),
        }
      : {
          included: false,
          reason: 'No VIN reached the runtime planning + pricing path.',
        },
    sampleRecords: records.slice(0, 10),
  };
}

function printCountMap(title, counts) {
  console.log(`\n${title}`);
  const entries = Object.entries(counts || {});
  if (entries.length === 0) {
    console.log('  (none)');
    return;
  }
  for (const [key, value] of entries) {
    console.log(`  ${key}: ${value}`);
  }
}

function printDimensionSection(title, breakdown) {
  console.log(`\n${title}`);
  const entries = Object.entries(breakdown || {});
  if (entries.length === 0) {
    console.log('  (none)');
    return;
  }

  for (const [bucket, statuses] of entries) {
    const compact = Object.entries(statuses)
      .map(([status, count]) => `${status}=${count}`)
      .join(', ');
    console.log(`  ${bucket}: ${compact}`);
  }
}

function printTopClusterSection(title, clusters) {
  console.log(`\n${title}`);
  if (!clusters || clusters.length === 0) {
    console.log('  (none)');
    return;
  }

  clusters.forEach((cluster, index) => {
    console.log(`  ${index + 1}. ${cluster.key} -> ${cluster.count}`);
  });
}

function printConsoleReport(report) {
  console.log('=== Runtime Coverage Audit ===');
  console.log(`Generated at: ${report.generatedAt}`);
  console.log(`Audit input source: ${report.auditInput.source}`);
  console.log(`Runtime scenario: ${report.auditInput.defaultScenario.exploitationType}, ${report.auditInput.defaultScenario.plannedKm} km, ${report.auditInput.defaultScenario.contractMonths} months`);
  console.log(`Runtime modules used: ${report.runtimeModulesUsed.join(', ')}`);
  console.log(`Audit semantics: ${report.auditInput.semantics}`);

  console.log('\nOverall outcome summary');
  console.log(`  total VINs audited: ${report.totals.totalVinsAudited}`);

  printCountMap('Status breakdown (internal status)', report.totals.internalStatus);
  printCountMap('Status breakdown (canonical status)', report.totals.canonicalStatus);
  printCountMap('Operational readiness breakdown', report.totals.operationalReadiness);
  printCountMap('Resolution status breakdown', report.totals.resolutionStatus);

  printDimensionSection('Breakdown by model', report.breakdowns.byModel);
  printDimensionSection('Breakdown by generation', report.breakdowns.byGeneration);
  printDimensionSection('Breakdown by body code', report.breakdowns.byBodyCode);
  printDimensionSection('Breakdown by platform', report.breakdowns.byPlatformCode);
  printDimensionSection('Breakdown by model year', report.breakdowns.byModelYear);
  printDimensionSection('Breakdown by engine family', report.breakdowns.byEngineFamily);
  printDimensionSection('Breakdown by gearbox family', report.breakdowns.byGearboxFamily);

  printTopClusterSection('Top partial_inferred clusters', report.topClusters.partialInferred);
  printTopClusterSection('Top needs_manual_input clusters', report.topClusters.needsManualInput);
  printTopClusterSection('Top blocked clusters', report.topClusters.blocked);
  printTopClusterSection('Top gearbox-unresolved clusters', report.topClusters.gearboxUnresolved);
  printTopClusterSection('Top weak pricing-confidence clusters', report.topClusters.weakPricingConfidence);

  console.log('\nPricing observability summary');
  if (!report.pricingObservability.included) {
    console.log(`  not included: ${report.pricingObservability.reason}`);
    return;
  }

  console.log(`  priced VINs: ${report.pricingObservability.auditedPricedVinCount}`);
  console.log(`  missingPriceEventCount: ${report.pricingObservability.missingPriceEventCount}`);
  console.log(`  genericEventCount: ${report.pricingObservability.genericEventCount}`);
  console.log(`  familyEventCount: ${report.pricingObservability.familyEventCount}`);
  console.log(`  fallbackLineCount: ${report.pricingObservability.fallbackLineCount}`);
  console.log(`  missingLineCount: ${report.pricingObservability.missingLineCount}`);
  console.log(`  pricingConfidenceBuckets: ${Object.entries(report.pricingObservability.pricingConfidenceBuckets).map(([key, value]) => `${key}=${value}`).join(', ') || '(none)'}`);
}

function main() {
  const vins = Array.isArray(runtimeTestVins) ? runtimeTestVins : [];
  const records = gatherRecords(vins);
  const report = buildReport(records);

  fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  fs.writeFileSync(OUTPUT_PATH, `${JSON.stringify(report, null, 2)}\n`);

  printConsoleReport(report);
  console.log(`\nJSON artifact written to ${path.relative(process.cwd(), OUTPUT_PATH)}`);
}

main();