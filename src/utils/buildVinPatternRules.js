import fs from "fs";
import path from "path";

const vinTrainingDatasetPath = path.resolve("src/data/vin_training_dataset.json");
const vinTrainingDataset = JSON.parse(
  fs.readFileSync(vinTrainingDatasetPath, "utf-8")
);

function unique(items) {
  return [...new Set((items || []).filter((x) => x !== null && x !== undefined && x !== ""))];
}

function getBodyCodeFromVin(vin) {
  if (!vin || vin.length < 6) return null;
  return vin.slice(3, 6);
}

function buildVinPatternRules(dataset) {
  const rules = {};

  for (const item of dataset) {
    const vin = item?.vin || "";
    const bodyCode = getBodyCodeFromVin(vin);
    const platformCode = item?.modelCode || (vin && vin.length >= 8 ? vin.slice(6, 8) : null);

    if (!bodyCode || !platformCode) continue;

    const key = `${bodyCode}|${platformCode}`;

    if (!rules[key]) {
      rules[key] = {
        bodyCode,
        platformCode,
        models: [],
        modelYears: [],
        engineCodes: [],
        gearboxCodes: [],
        drivetrains: [],
        fuelTypes: [],
        engineFamilies: [],
      };
    }

    rules[key].models.push(item?.model || null);
    rules[key].modelYears.push(item?.modelYear || null);
    rules[key].engineCodes.push(item?.engineCode || null);
    rules[key].gearboxCodes.push(item?.transmissionCode || null);
    rules[key].drivetrains.push(item?.drivetrain || null);
    rules[key].fuelTypes.push(item?.fuel || item?.fuelType || null);
    rules[key].engineFamilies.push(item?.engineFamily || null);
  }

  for (const key of Object.keys(rules)) {
    rules[key].models = unique(rules[key].models).sort();
    rules[key].modelYears = unique(rules[key].modelYears).sort((a, b) => a - b);
    rules[key].engineCodes = unique(rules[key].engineCodes).sort();
    rules[key].gearboxCodes = unique(rules[key].gearboxCodes).sort();
    rules[key].drivetrains = unique(rules[key].drivetrains).sort();
    rules[key].fuelTypes = unique(rules[key].fuelTypes).sort();
    rules[key].engineFamilies = unique(rules[key].engineFamilies).sort();
  }

  return rules;
}

const rules = buildVinPatternRules(vinTrainingDataset);

const outputPath = path.resolve("src/data/vin_pattern_rules.json");
fs.writeFileSync(outputPath, JSON.stringify(rules, null, 2), "utf-8");

console.log(`vin_pattern_rules.json generated: ${outputPath}`);
console.log(`Total pattern rules: ${Object.keys(rules).length}`);