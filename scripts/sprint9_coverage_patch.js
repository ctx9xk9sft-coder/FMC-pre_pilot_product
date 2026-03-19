// scripts/sprint9_coverage_patch.js
import fs from "fs";
import path from "path";

const root = process.cwd();
const dataDir = path.join(root, "src", "data");

const engineMasterPath = path.join(dataDir, "engine_codes_master.json");
const engineGroupsPath = path.join(dataDir, "engine_business_groups.json");
const gearboxMasterPath = path.join(dataDir, "gearbox_codes_master.json");

const engineMaster = JSON.parse(fs.readFileSync(engineMasterPath, "utf8"));
const engineGroups = JSON.parse(fs.readFileSync(engineGroupsPath, "utf8"));
const gearboxMaster = JSON.parse(fs.readFileSync(gearboxMasterPath, "utf8"));

function upsertEngineFromGroup(code) {
  for (const group of Object.values(engineGroups)) {
    const codes = Array.isArray(group?.engineCodes) ? group.engineCodes : [];
    if (!codes.includes(code)) continue;

    if (!engineMaster[code]) {
      engineMaster[code] = {
        engineUnit: null,
        family: group.label || null,
        displacementL: group.displacementL ?? null,
        fuel: group.fuel ?? null,
        powerKw: group.powerKw ?? null,
        cylinders: group.displacementL === 1.0 ? 3 : 4,
        description: group.label || code,
        sparkPlugsRequired: group.fuel === "petrol",
      };
    }
    return true;
  }
  return false;
}

// Missing engine master entries seen in your current VIN set
[
  "DLAC",
  "DKRF",
  "DKJA",
  "DXUA",
  "DSTB",
  "DTTA",
  "DTTC",
  "DSTA",
  "DBGC",
  "DFGA",
  "DSUD",
  "DFFA",
  "CZDA",
  "CZEA",
].forEach(upsertEngineFromGroup);

// Safe gearbox drivetrain stubs.
// Intention: unlock FWD/AWD consensus without inventing DSG semantics.
// Do NOT mark unknown codes as DSG/serviceable unless confirmed by PDF.
const FWD_STUBS = [
  // Superb III NP
  "USA", "TJZ", "TKD", "TKN", "TWE", "TYB", "TZH", "UEU", "ULV", "VCH",

  // Octavia IV NX
  "USX", "TVR", "TWH", "UET", "UEV", "UFH", "UFJ", "UFK", "UHX", "ULR", "ULW", "UPH", "USE", "USF",

  // Fabia IV / PJ
  "UHC", "UKC", "USM", "UVN", "VGR",

  // Kamiq / Scala NW
  "SBV", "SHA", "TCV", "TKQ", "TKV", "TLN", "TUN", "TYP", "UFC", "UHY", "UJA", "UJB", "UQU", "UYS",
];

const AWD_STUBS = [
  // Kodiaq I NS
  "TFQ", "TFR", "TFU", "TJL", "TNY", "TUL", "TUR", "URP", "URR", "URX", "URZ", "UZU", "UZW",
  "VDA", "VDL", "VHB", "VHE", "VHH", "WBP", "WBQ", "WBR", "WBS", "WBT", "WBW", "WDD", "WFN",
  "WFT", "WFW", "WGA", "WGH", "WGJ", "WGK", "WGQ", "WJB", "WLR",

  // Kodiaq II PS
  "WJN", "WJT", "WJW", "WKH", "WKJ", "WKK", "WPX",

  // Karoq I NU
  "VGF",
];

function ensureStub(code, drivetrain) {
  if (!gearboxMaster[code]) {
    gearboxMaster[code] = {
      family: null,
      type: null,
      drivetrain,
      description: `coverage stub ${drivetrain}`,
      serviceRequired: false,
      clutchType: null,
    };
    return;
  }

  if (!gearboxMaster[code].drivetrain) {
    gearboxMaster[code].drivetrain = drivetrain;
  }
}

FWD_STUBS.forEach((code) => ensureStub(code, "FWD"));
AWD_STUBS.forEach((code) => ensureStub(code, "AWD"));

fs.writeFileSync(engineMasterPath, JSON.stringify(engineMaster, null, 2) + "\n");
fs.writeFileSync(gearboxMasterPath, JSON.stringify(gearboxMaster, null, 2) + "\n");

console.log("Coverage patch applied:");
console.log("- engine_codes_master.json updated");
console.log("- gearbox_codes_master.json updated");