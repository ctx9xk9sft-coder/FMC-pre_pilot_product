import fs from "fs";
import path from "path";
import * as pdfjsLib from "pdfjs-dist/legacy/build/pdf.mjs";

const INPUT_ROOT = "./data_sources";
const OUTPUT_DATASET = "./src/data/vin_training_dataset.json";
const OUTPUT_REPORT = "./src/data/pdf_import_report.json";

const MODEL_NAMES = [
  "Fabia",
  "Scala",
  "Kamiq",
  "Octavia",
  "Superb",
  "Kodiaq",
  "Karoq",
  "Enyaq",
];

const MODEL_ABBR_MAP = {
  Fabia: "FAB",
  Scala: "SCA",
  Kamiq: "KAM",
  Octavia: "OCT",
  Superb: "SUP",
  Kodiaq: "KOD",
  Karoq: "KAR",
  Enyaq: "ENY",
};

function loadJson(filePath, fallback) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return fallback;
  }
}

function normalizeText(value = "") {
  return String(value)
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function walkPdfFiles(dir) {
  if (!fs.existsSync(dir)) return [];

  const entries = fs.readdirSync(dir, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);

    if (entry.isDirectory()) {
      files.push(...walkPdfFiles(fullPath));
      continue;
    }

    if (entry.isFile() && entry.name.toLowerCase().endsWith(".pdf")) {
      files.push(fullPath);
    }
  }

  return files.sort((a, b) => a.localeCompare(b));
}

async function extractPageLines(filePath) {
  const data = new Uint8Array(fs.readFileSync(filePath));
  const pdf = await pdfjsLib.getDocument({ data }).promise;

  const pages = [];

  for (let pageNo = 1; pageNo <= pdf.numPages; pageNo++) {
    const page = await pdf.getPage(pageNo);
    const content = await page.getTextContent();

    const buckets = new Map();

    for (const item of content.items) {
      const str = normalizeText(item.str || "");
      if (!str) continue;

      const x = item.transform[4];
      const y = Math.round(item.transform[5] * 10) / 10;
      const key = String(y);

      if (!buckets.has(key)) {
        buckets.set(key, []);
      }

      buckets.get(key).push({ x, y, str });
    }

    const lines = [...buckets.values()]
      .map((items) => {
        const sorted = items.sort((a, b) => a.x - b.x);
        return {
          y: sorted[0].y,
          text: normalizeText(sorted.map((x) => x.str).join(" ")),
        };
      })
      .filter((x) => x.text)
      .sort((a, b) => b.y - a.y);

    pages.push({
      pageNo,
      lines,
      fullText: normalizeText(lines.map((x) => x.text).join(" ")),
    });
  }

  return pages;
}

function uniqueStrings(values) {
  const out = [];
  const seen = new Set();

  for (const value of values) {
    const key = normalizeText(value);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(key);
  }

  return out;
}

function findVin(text) {
  const match = text.match(/\b[A-HJ-NPR-Z0-9]{17}\b/);
  return match ? match[0] : null;
}

function findModelInText(text) {
  for (const model of MODEL_NAMES) {
    const rx = new RegExp(`\\b${model}\\b`, "i");
    if (rx.test(text)) return model;
  }

  return null;
}

function isModelAbbr(value) {
  return /^(FAB|SCA|KAM|OCT|SUP|KOD|KAR|ENY)$/i.test(value);
}

function findHeaderWindow(firstPageLines, vin) {
  const texts = firstPageLines.map((x) => x.text);
  const vinIndex = texts.findIndex((line) => line.includes(vin));

  if (vinIndex === -1) {
    return {
      start: 0,
      end: Math.min(texts.length - 1, 25),
      texts,
    };
  }

  return {
    start: vinIndex,
    end: Math.min(texts.length - 1, vinIndex + 25),
    texts,
  };
}

function findModelYearFromHeader(headerLines, fullText) {
  for (const line of headerLines) {
    const match = line.match(/Model Year\s+(20\d{2})/i);
    if (match) return Number(match[1]);
  }

  const fallback = fullText.match(/\b20\d{2}\b/);
  return fallback ? Number(fallback[0]) : null;
}

function findEngineCodeFromHeader(headerLines) {
  for (const line of headerLines) {
    const match = line.match(/Engine Code\s+([A-Z0-9]{4})/i);
    if (match) return match[1].toUpperCase();
  }

  return null;
}

function findTransmissionCodeFromHeader(headerLines) {
  for (const line of headerLines) {
    const match = line.match(/Transmission Code\s+([A-Z0-9]{3})/i);
    if (match) return match[1].toUpperCase();
  }

  return null;
}

function parseHeaderBlock(firstPage) {
  const vin = findVin(firstPage.fullText);

  if (!vin) {
    return {
      vin: null,
      salesType: null,
      modelYear: null,
      modelAbbr: null,
      model: null,
      engineCode: null,
      transmissionCode: null,
      headerSlice: [],
    };
  }

  const { start, end, texts } = findHeaderWindow(firstPage.lines, vin);
  const rawSlice = texts.slice(start, end + 1);
  const headerSlice = uniqueStrings(rawSlice);

  const salesTypeLine = headerSlice.find((x) => /Sales Type/i.test(x)) || null;
  let salesType = null;

  if (salesTypeLine) {
    const match = salesTypeLine.match(/Sales Type\s+([A-Z0-9]{6,8})/i);
    if (match) {
      salesType = match[1].toUpperCase();
    }
  }

  const modelYear = findModelYearFromHeader(headerSlice, firstPage.fullText);

  let model = null;
  for (const text of headerSlice) {
    const found = findModelInText(text);
    if (found) {
      model = found;
      break;
    }
  }

  let modelAbbr = headerSlice.find((x) => isModelAbbr(x)) || null;
  if (!modelAbbr && model) {
    modelAbbr = MODEL_ABBR_MAP[model] || null;
  }

  const engineCode = findEngineCodeFromHeader(headerSlice);
  const transmissionCode = findTransmissionCodeFromHeader(headerSlice);

  const modelCode = vin && vin.length >= 8 ? vin.slice(6, 8).toUpperCase() : null;

  return {
    vin,
    salesType,
    modelCode,
    modelYear,
    modelAbbr,
    model,
    engineCode,
    transmissionCode,
    headerSlice,
  };
}

function findProductionDate(fullText) {
  const match = fullText.match(/\b\d{2}\.\d{2}\.\d{4}\b/);
  return match ? match[0] : null;
}

function findServiceRegime(fullText) {
  if (/\bQG1\b/i.test(fullText) || /longlife service regime/i.test(fullText)) {
    return "LongLife";
  }

  if (/fixed service regime/i.test(fullText)) {
    return "Fixed";
  }

  return null;
}

function findServiceIndicator(fullText) {
  const match = fullText.match(/\bQI\d\b/i);
  return match ? match[0].toUpperCase() : null;
}

function findDrivetrain(fullText) {
  if (
    /\b1X1\b/i.test(fullText) ||
    /four-wheel drive/i.test(fullText) ||
    /\ball-wheel drive\b/i.test(fullText) ||
    /\b4x4\b/i.test(fullText)
  ) {
    return "AWD";
  }

  if (/\b1X0\b/i.test(fullText) || /front-wheel drive/i.test(fullText)) {
    return "FWD";
  }

  if (/rear-wheel drive/i.test(fullText)) {
    return "RWD";
  }

  return null;
}

function findEngineUnitCode(fullText) {
  const match = fullText.match(/\bT(?!DI\b)[A-Z0-9]{2}\b/);
  return match ? match[0].toUpperCase() : null;
}

function findGearboxPrCode(fullText) {
  const match = fullText.match(/\bG[0-9A-Z]{2}\b/);
  return match ? match[0].toUpperCase() : null;
}

function buildRecordFromPages(pages, sourceFile) {
  const firstPage = pages[0];
  const allText = normalizeText(pages.map((x) => x.fullText).join(" "));

  const header = parseHeaderBlock(firstPage);

  const record = {
    sourceFile,
    vin: header.vin,
    salesType: header.salesType,
    modelCode: header.modelCode || (header.vin && header.vin.length >= 8 ? header.vin.slice(6, 8).toUpperCase() : null),
    modelYear: header.modelYear,
    modelAbbr: header.modelAbbr,
    model: header.model,
    engineCode: header.engineCode,
    transmissionCode: header.transmissionCode,
    drivetrain: findDrivetrain(allText),
    serviceRegime: findServiceRegime(allText),
    serviceIndicator: findServiceIndicator(allText),
    productionDate: findProductionDate(allText),
    engineUnitCode: findEngineUnitCode(allText),
    gearboxPrCode: findGearboxPrCode(allText),
  };

  const required = [
    "vin",
    "model",
    "modelYear",
    "engineCode",
    "transmissionCode",
    "drivetrain",
  ];

  const missing = required.filter((field) => {
    const value = record[field];
    return value === null || value === undefined || value === "";
  });

  return {
    ok: missing.length === 0,
    record,
    missing,
    debug: {
      headerSlice: header.headerSlice,
      firstPagePreview: firstPage.lines.slice(0, 40).map((x) => x.text),
    },
  };
}

function sortDataset(rows) {
  return [...rows].sort((a, b) => a.vin.localeCompare(b.vin));
}

function createBackupIfNeeded(filePath) {
  if (!fs.existsSync(filePath)) return null;

  const now = new Date();
  const stamp = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, "0")}${String(now.getDate()).padStart(2, "0")}-${String(now.getHours()).padStart(2, "0")}${String(now.getMinutes()).padStart(2, "0")}${String(now.getSeconds()).padStart(2, "0")}`;
  const backupPath = filePath.replace(/\.json$/i, `.backup-${stamp}.json`);

  fs.copyFileSync(filePath, backupPath);
  return backupPath;
}

async function run() {
  const pdfFiles = walkPdfFiles(INPUT_ROOT);
  const existingDataset = loadJson(OUTPUT_DATASET, []);

  if (pdfFiles.length === 0) {
    console.log(`No PDF files found under ${INPUT_ROOT}`);
    return;
  }

  const datasetMap = new Map();

  for (const row of existingDataset) {
    if (row?.vin) {
      datasetMap.set(row.vin, row);
    }
  }

  const report = {
    scannedAt: new Date().toISOString(),
    inputRoot: INPUT_ROOT,
    totalPdfFiles: pdfFiles.length,
    created: [],
    updated: [],
    failed: [],
  };

  let createdCount = 0;
  let updatedCount = 0;

  for (const filePath of pdfFiles) {
    const sourceFile = path.basename(filePath);

    try {
      const pages = await extractPageLines(filePath);
      const parsed = buildRecordFromPages(pages, sourceFile);

      if (!parsed.ok) {
        report.failed.push({
          sourceFile,
          missing: parsed.missing,
          parsedRecord: parsed.record,
          debug: parsed.debug,
        });
        console.log(`FAILED  ${sourceFile} -> missing: ${parsed.missing.join(", ")}`);
        continue;
      }

      const existed = datasetMap.has(parsed.record.vin);
      datasetMap.set(parsed.record.vin, parsed.record);

      if (existed) {
        updatedCount += 1;
        report.updated.push({
          sourceFile,
          vin: parsed.record.vin,
        });
        console.log(`UPDATED ${sourceFile} -> ${parsed.record.vin}`);
      } else {
        createdCount += 1;
        report.created.push({
          sourceFile,
          vin: parsed.record.vin,
        });
        console.log(`CREATED ${sourceFile} -> ${parsed.record.vin}`);
      }
    } catch (error) {
      report.failed.push({
        sourceFile,
        error: error.message,
      });
      console.log(`ERROR   ${sourceFile} -> ${error.message}`);
    }
  }

  const finalDataset = sortDataset([...datasetMap.values()]);
  const backupPath = createBackupIfNeeded(OUTPUT_DATASET);

  fs.writeFileSync(OUTPUT_DATASET, JSON.stringify(finalDataset, null, 2), "utf8");

  report.summary = {
    beforeRows: existingDataset.length,
    afterRows: finalDataset.length,
    createdCount,
    updatedCount,
    failedCount: report.failed.length,
    backupPath,
  };

  fs.writeFileSync(OUTPUT_REPORT, JSON.stringify(report, null, 2), "utf8");

  console.log("");
  console.log("Import finished.");
  console.log(`PDF files scanned: ${pdfFiles.length}`);
  console.log(`Created rows: ${createdCount}`);
  console.log(`Updated rows: ${updatedCount}`);
  console.log(`Failed files: ${report.failed.length}`);
  console.log(`Dataset saved to: ${OUTPUT_DATASET}`);
  console.log(`Report saved to: ${OUTPUT_REPORT}`);
  if (backupPath) {
    console.log(`Backup created: ${backupPath}`);
  }
}

run();