import fs from "fs";

const DATASET_PATH = "./src/data/vin_training_dataset.json";
const OUTPUT_PATH = "./src/data/vin_pattern_rules.json";
const ENGINE_MASTER_PATH = "./src/data/engine_codes_master.json";

function loadJson(path, fallback = {}) {
  if (!fs.existsSync(path)) return fallback;
  return JSON.parse(fs.readFileSync(path, "utf8"));
}

function loadDataset() {
  const dataset = loadJson(DATASET_PATH, []);
  if (!Array.isArray(dataset)) throw new Error(`Dataset not found or invalid: ${DATASET_PATH}`);
  return dataset;
}

function isValidRow(row) {
  return Boolean(row && row.vin && row.model && row.engineCode && row.transmissionCode && row.modelYear && row.drivetrain);
}

function getBodyCode(vin) {
  if (!vin || vin.length < 6) return null;
  return vin.slice(3, 6).toUpperCase();
}

function getPlatformCode(vin) {
  if (!vin || vin.length < 8) return null;
  return vin.slice(6, 8).toUpperCase();
}

function toSortedArray(set) {
  return [...set].filter(Boolean).sort();
}

function run() {
  const dataset = loadDataset();
  const engineMaster = loadJson(ENGINE_MASTER_PATH, {});
  const validRows = dataset.filter(isValidRow);
  const patterns = {};

  for (const row of validRows) {
    const bodyCode = getBodyCode(row.vin);
    const platformCode = getPlatformCode(row.vin);
    if (!bodyCode || !platformCode) continue;

    const key = `${bodyCode}|${platformCode}`;
    if (!patterns[key]) {
      patterns[key] = {
        bodyCode,
        platformCode,
        models: new Set(),
        engineCodes: new Set(),
        gearboxCodes: new Set(),
        modelYears: new Set(),
        drivetrains: new Set(),
        fuelTypes: new Set(),
        engineFamilies: new Set(),
      };
    }

    const bucket = patterns[key];
    bucket.models.add(row.model);
    bucket.engineCodes.add(row.engineCode);
    bucket.gearboxCodes.add(row.transmissionCode);
    bucket.modelYears.add(Number(row.modelYear));
    bucket.drivetrains.add(row.drivetrain);
    if (row.fuel) bucket.fuelTypes.add(row.fuel);
    const family = engineMaster?.[row.engineCode]?.family || null;
    if (family) bucket.engineFamilies.add(family);
  }

  const finalRules = {};
  for (const [key, value] of Object.entries(patterns)) {
    finalRules[key] = {
      bodyCode: value.bodyCode,
      platformCode: value.platformCode,
      models: toSortedArray(value.models),
      modelYears: [...value.modelYears].sort((a, b) => a - b),
      engineCodes: toSortedArray(value.engineCodes),
      gearboxCodes: toSortedArray(value.gearboxCodes),
      drivetrains: toSortedArray(value.drivetrains),
      fuelTypes: toSortedArray(value.fuelTypes),
      engineFamilies: toSortedArray(value.engineFamilies),
    };
  }

  fs.writeFileSync(OUTPUT_PATH, JSON.stringify(finalRules, null, 2), "utf8");
  console.log(`VIN pattern rules generated: ${OUTPUT_PATH}`);
  console.log(`Total dataset rows: ${dataset.length}`);
  console.log(`Valid rows used: ${validRows.length}`);
  console.log(`Pattern groups generated: ${Object.keys(finalRules).length}`);
}

run();
