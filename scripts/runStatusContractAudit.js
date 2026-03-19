import vinTrainingDataset from "../src/data/vin_training_dataset.json" with { type: "json" };
import vinPatternRules from "../src/data/vin_pattern_rules.json" with { type: "json" };

function clean(value) {
  const text = String(value ?? "").trim();
  return text || null;
}

function bodyCode(vin) {
  const cleanVin = String(vin || "").trim().toUpperCase();
  return cleanVin.length >= 6 ? cleanVin.slice(3, 6) : null;
}

function platformCode(vin) {
  const cleanVin = String(vin || "").trim().toUpperCase();
  return cleanVin.length >= 8 ? cleanVin.slice(6, 8) : null;
}

function keyOf(parts) {
  return parts.map((part) => clean(part) ?? "_").join("|");
}

function inc(map, key, amount = 1) {
  map.set(key, (map.get(key) || 0) + amount);
}

function summarizeRows(rows) {
  const segmentMap = new Map();
  const engineMap = new Map();
  const gearboxMap = new Map();
  const modelMap = new Map();
  const platformMap = new Map();
  const yearlyMap = new Map();

  for (const row of rows) {
    const vin = clean(row?.vin);
    const body = bodyCode(vin);
    const platform = platformCode(vin);
    const model = clean(row?.model);
    const year = row?.modelYear != null ? Number(row.modelYear) : null;
    const engine = clean(row?.engineCode);
    const gearbox = clean(row?.transmissionCode);
    const drivetrain = clean(row?.drivetrain);

    const key = keyOf([model, body, platform, year]);
    const segment = segmentMap.get(key) || {
      key,
      model,
      body,
      platform,
      year,
      sampleCount: 0,
      engineCounts: new Map(),
      gearboxCounts: new Map(),
      drivetrainCounts: new Map(),
      rows: [],
    };

    segment.sampleCount += 1;
    if (engine) inc(segment.engineCounts, engine);
    if (gearbox) inc(segment.gearboxCounts, gearbox);
    if (drivetrain) inc(segment.drivetrainCounts, drivetrain);
    segment.rows.push({ vin, engine, gearbox, drivetrain });
    segmentMap.set(key, segment);

    if (model) inc(modelMap, model);
    if (platform) inc(platformMap, platform);
    if (year != null && Number.isFinite(year)) inc(yearlyMap, String(year));
    if (engine) inc(engineMap, engine);
    if (gearbox) inc(gearboxMap, gearbox);
  }

  const segments = [...segmentMap.values()].map((segment) => {
    const engineVariants = segment.engineCounts.size;
    const gearboxVariants = segment.gearboxCounts.size;
    const drivetrainVariants = segment.drivetrainCounts.size;
    const exactFriendly =
      segment.sampleCount >= 2 &&
      engineVariants <= 1 &&
      gearboxVariants <= 1;
    const inferenceSafe =
      segment.sampleCount >= 3 &&
      engineVariants <= 1 &&
      gearboxVariants <= 1 &&
      drivetrainVariants <= 1;
    const ambiguous =
      engineVariants > 1 ||
      gearboxVariants > 1 ||
      drivetrainVariants > 1;

    return {
      ...segment,
      engineVariants,
      gearboxVariants,
      drivetrainVariants,
      exactFriendly,
      inferenceSafe,
      ambiguous,
    };
  });

  const totals = {
    totalRows: rows.length,
    totalSegments: segments.length,
    exactFriendlySegments: segments.filter((item) => item.exactFriendly).length,
    inferenceSafeSegments: segments.filter((item) => item.inferenceSafe).length,
    ambiguousSegments: segments.filter((item) => item.ambiguous).length,
    singleSampleSegments: segments.filter((item) => item.sampleCount === 1).length,
  };

  return {
    totals,
    segments,
    modelCounts: [...modelMap.entries()].sort((a, b) => b[1] - a[1]),
    platformCounts: [...platformMap.entries()].sort((a, b) => b[1] - a[1]),
    yearlyCounts: [...yearlyMap.entries()].sort((a, b) => Number(a[0]) - Number(b[0])),
    engineCounts: [...engineMap.entries()].sort((a, b) => b[1] - a[1]),
    gearboxCounts: [...gearboxMap.entries()].sort((a, b) => b[1] - a[1]),
  };
}

function summarizePatternRules(rules) {
  const bodyPlatform = new Map();
  const modelHits = new Map();
  const entries = Array.isArray(rules)
    ? rules
    : Object.entries(rules || {}).map(([key, value]) => ({ key, ...(value || {}) }));

  for (const rule of entries) {
    const ruleKey = clean(rule?.key);
    const derivedBody = ruleKey && ruleKey.includes("|") ? ruleKey.split("|")[0] : null;
    const derivedPlatform = ruleKey && ruleKey.includes("|") ? ruleKey.split("|")[1] : null;
    const body = clean(rule?.bodyCode || rule?.fullBodyCode || rule?.body || derivedBody);
    const platform = clean(rule?.platformCode || rule?.platform || derivedPlatform);
    const model = clean(rule?.model);
    const key = keyOf([body, platform]);
    inc(bodyPlatform, key);
    if (model) inc(modelHits, model);
  }

  return {
    totalRules: entries.length,
    bodyPlatformCoverage: [...bodyPlatform.entries()].sort((a, b) => b[1] - a[1]),
    modelCoverage: [...modelHits.entries()].sort((a, b) => b[1] - a[1]),
  };
}

function toPlainTopSegments(segments, filterFn, limit = 15) {
  return segments
    .filter(filterFn)
    .sort((a, b) => {
      if (b.sampleCount !== a.sampleCount) return b.sampleCount - a.sampleCount;
      return a.key.localeCompare(b.key);
    })
    .slice(0, limit)
    .map((item) => ({
      key: item.key,
      model: item.model,
      body: item.body,
      platform: item.platform,
      year: item.year,
      sampleCount: item.sampleCount,
      engineVariants: item.engineVariants,
      gearboxVariants: item.gearboxVariants,
      drivetrainVariants: item.drivetrainVariants,
    }));
}

const dataset = Array.isArray(vinTrainingDataset) ? vinTrainingDataset : [];
const ruleSummary = summarizePatternRules(vinPatternRules);
const audit = summarizeRows(dataset);

const output = {
  generatedAt: new Date().toISOString(),
  dataset: audit.totals,
  patternRules: {
    totalRules: ruleSummary.totalRules,
    coveredBodyPlatforms: ruleSummary.bodyPlatformCoverage.length,
    topBodyPlatforms: ruleSummary.bodyPlatformCoverage
      .slice(0, 15)
      .map(([key, count]) => ({ key, count })),
    topModels: ruleSummary.modelCoverage
      .slice(0, 10)
      .map(([model, count]) => ({ model, count })),
  },
  leadingModels: audit.modelCounts
    .slice(0, 12)
    .map(([model, count]) => ({ model, count })),
  leadingPlatforms: audit.platformCounts
    .slice(0, 12)
    .map(([platform, count]) => ({ platform, count })),
  topAmbiguousSegments: toPlainTopSegments(audit.segments, (item) => item.ambiguous),
  topSingleSampleSegments: toPlainTopSegments(audit.segments, (item) => item.sampleCount === 1),
  topInferenceSafeSegments: toPlainTopSegments(audit.segments, (item) => item.inferenceSafe),
};

console.log(JSON.stringify(output, null, 2));