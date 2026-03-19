import { ENGINE_FAMILY_MAP, MODEL_YEAR_MAP, PLANT_MAP } from "./decoderMappings.js";

function cleanString(value) {
  return typeof value === "string" ? value.trim().toUpperCase() : "";
}

function isValidVinCharacterSet(vin) {
  return !/[^A-HJ-NPR-Z0-9]/.test(vin);
}

export function parseVinCore(vin) {
  const cleanVin = cleanString(vin);

  const segments = {
    vin: cleanVin,
    wmi: cleanVin.slice(0, 3) || null,
    bodyCode: cleanVin.length >= 4 ? cleanVin[3] : null,
    engineCode: cleanVin.length >= 5 ? cleanVin[4] : null,
    restraintCode: cleanVin.length >= 6 ? cleanVin[5] : null,
    modelCode: cleanVin.length >= 8 ? cleanVin.slice(6, 8) : null,
    yearCode: cleanVin.length >= 10 ? cleanVin[9] : null,
    plantCode: cleanVin.length >= 11 ? cleanVin[10] : null,
    serialNumber: cleanVin.length >= 17 ? cleanVin.slice(11, 17) : null,
    fullBodyCode: cleanVin.length >= 6 ? cleanVin.slice(3, 6) : null,
    platformCode: cleanVin.length >= 8 ? cleanVin.slice(6, 8) : null,
  };

  const validationErrors = [];
  if (!cleanVin) validationErrors.push("VIN is empty.");
  if (cleanVin.length !== 17) validationErrors.push("VIN must be exactly 17 characters long.");
  if (cleanVin && !isValidVinCharacterSet(cleanVin)) validationErrors.push("VIN contains invalid characters.");

  return {
    cleanVin,
    isStructurallyValid: validationErrors.length === 0,
    validationErrors,
    segments,
    derived: {
      engineFamily: ENGINE_FAMILY_MAP[segments.engineCode] || null,
      modelYear: MODEL_YEAR_MAP[segments.yearCode] || null,
      plant: PLANT_MAP[segments.plantCode] || null,
    },
  };
}

export default parseVinCore;
