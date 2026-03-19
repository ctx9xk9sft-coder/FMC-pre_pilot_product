import vinTrainingDataset from "../data/vin_training_dataset.json" with { type: 'json' };

function unique(values = []) {
  return [...new Set((values || []).filter(Boolean))];
}

function normalizeModel(model) {
  const value = String(model || "").trim();
  return value || null;
}

function getFullBodyCode(vin) {
  const cleanVin = String(vin || "").trim().toUpperCase();
  return cleanVin.length >= 6 ? cleanVin.slice(3, 6) : null;
}

function getPlatformCode(vin) {
  const cleanVin = String(vin || "").trim().toUpperCase();
  return cleanVin.length >= 8 ? cleanVin.slice(6, 8) : null;
}

function buildKey(parts = []) {
  return parts.map((part) => String(part || "").trim()).join("|");
}

function makeEmptyBucket() {
  return {
    sampleCount: 0,
    engineCounts: {},
    gearboxCounts: {},
    models: {},
    drivetrains: {},
  };
}

function incrementCounter(target, key) {
  if (!key) return;
  target[key] = (target[key] || 0) + 1;
}

function analyzeCounter(counter = {}, minSamples = 2) {
  const entries = Object.entries(counter).sort((a, b) => {
    if (b[1] !== a[1]) return b[1] - a[1];
    return String(a[0]).localeCompare(String(b[0]));
  });

  const sampleCount = entries.reduce((sum, [, count]) => sum + Number(count || 0), 0);

  if (entries.length === 0 || sampleCount < minSamples) {
    return {
      selected: null,
      confidence: entries.length === 0 ? "none" : "low",
      ratio: 0,
      sampleCount,
      ambiguity: entries.length === 0 ? "empty" : "insufficient_samples",
      candidates: entries.map(([value, count]) => ({ value, key: value, count })),
      supportCount: 0,
      supportRatio: 0,
      dominanceScore: 0,
      conflictCount: Math.max(0, entries.length - 1),
    };
  }

  const [[topValue, topCount], second = [null, 0]] = entries;
  const ratio = sampleCount > 0 ? topCount / sampleCount : 0;
  const secondCount = Number(second?.[1] || 0);
  const dominanceScore = topCount > 0 ? topCount / Math.max(topCount + secondCount, topCount) : 0;
  const conflictCount = Math.max(0, entries.filter(([, count]) => Number(count || 0) > 0).length - 1);

  const highDominance = sampleCount >= 3 && ratio >= 0.6 && dominanceScore >= 0.6 && conflictCount <= 1;
  const mediumDominance = sampleCount >= 2 && ratio >= 0.5 && dominanceScore >= 0.55;

  return {
    selected: highDominance || mediumDominance ? topValue : null,
    confidence: highDominance ? "high" : mediumDominance ? "medium" : "low",
    ratio: Number(ratio.toFixed(4)),
    sampleCount,
    ambiguity: entries.length > 1 ? "multi_candidate" : "single_candidate",
    candidates: entries.map(([value, count]) => ({ value, key: value, count })),
    supportCount: Number(topCount || 0),
    supportRatio: Number(ratio.toFixed(4)),
    dominanceScore: Number(dominanceScore.toFixed(4)),
    conflictCount,
  };
}

function registerBucket(map, key, row) {
  if (!key) return;
  const bucket = map.get(key) || makeEmptyBucket();
  bucket.sampleCount += 1;
  incrementCounter(bucket.engineCounts, row?.engineCode);
  incrementCounter(bucket.gearboxCounts, row?.transmissionCode);
  incrementCounter(bucket.models, normalizeModel(row?.model));
  incrementCounter(bucket.drivetrains, row?.drivetrain);
  map.set(key, bucket);
}

function buildDatasetStatistics(rows = []) {
  const map = new Map();

  for (const row of rows) {
    const vin = String(row?.vin || "").trim().toUpperCase();
    if (vin.length !== 17) continue;

    const fullBodyCode = getFullBodyCode(vin);
    const platformCode = getPlatformCode(vin);
    const modelYear = row?.modelYear != null ? Number(row.modelYear) : null;
    const model = normalizeModel(row?.model);

    if (!fullBodyCode || !platformCode || !model) continue;

    registerBucket(map, buildKey([fullBodyCode, platformCode, modelYear, model]), row);
    registerBucket(map, buildKey([fullBodyCode, platformCode, model]), row);
    registerBucket(map, buildKey([platformCode, modelYear, model]), row);
  }

  return map;
}

const DATASET_STATS = buildDatasetStatistics(Array.isArray(vinTrainingDataset) ? vinTrainingDataset : []);

function pickBucket(decoded = {}) {
  const vin = decoded?.vin || "";
  const fullBodyCode = getFullBodyCode(vin);
  const platformCode = getPlatformCode(vin);
  const modelYear = decoded?.modelYear != null ? Number(decoded.modelYear) : null;
  const model = normalizeModel(decoded?.model ? String(decoded.model).replace(/\s+(I|II|III|IV|V)$/u, "") : null);

  const strategies = [
    { key: buildKey([fullBodyCode, platformCode, modelYear, model]), level: "body_platform_year_model" },
    { key: buildKey([fullBodyCode, platformCode, model]), level: "body_platform_model" },
    { key: buildKey([platformCode, modelYear, model]), level: "platform_year_model" },
  ];

  for (const strategy of strategies) {
    const bucket = DATASET_STATS.get(strategy.key);
    if (bucket && bucket.sampleCount >= 2) {
      return { ...strategy, bucket };
    }
  }

  return null;
}

export function inferVinConfiguration(decoded = {}) {
  const bucketMatch = pickBucket(decoded);
  if (!bucketMatch) {
    return {
      matched: false,
      source: "dataset_pattern_statistics",
      engine: null,
      gearbox: null,
      statsLevel: null,
      sampleCount: 0,
    };
  }

  const engine = analyzeCounter(bucketMatch.bucket.engineCounts);
  const gearbox = analyzeCounter(bucketMatch.bucket.gearboxCounts);

  return {
    matched: Boolean(engine.selected || gearbox.selected),
    source: "dataset_pattern_statistics",
    statsLevel: bucketMatch.level,
    sampleCount: bucketMatch.bucket.sampleCount,
    key: bucketMatch.key,
    engine,
    gearbox,
    models: bucketMatch.bucket.models,
    drivetrains: bucketMatch.bucket.drivetrains,
  };
}

export function getInferenceCandidateList(inference = {}, field = "engine") {
  const slice = field === "gearbox" ? inference?.gearbox : inference?.engine;
  return unique((slice?.candidates || []).map((item) => item?.value || item?.key).filter(Boolean));
}

export default inferVinConfiguration;
