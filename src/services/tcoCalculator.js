import { validateVinForMaintenance } from "../validation/maintenanceGate.js";

function normalizeUsageProfile(exploitation) {
  if (!exploitation) return null;

  if (typeof exploitation === "string") {
    const value = exploitation.trim().toLowerCase();

    if (!value) return null;
    if (value.includes("grad") || value.includes("city")) return "city_heavy";
    if (value.includes("autoput") || value.includes("highway")) return "highway";

    return "mixed";
  }

  const raw =
    exploitation.code ||
    exploitation.key ||
    exploitation.id ||
    exploitation.label ||
    exploitation.name ||
    "";

  const value = String(raw).trim().toLowerCase();

  if (!value) return null;
  if (value.includes("grad") || value.includes("city")) return "city_heavy";
  if (value.includes("autoput") || value.includes("highway")) return "highway";

  return "mixed";
}

export function calculateMaintenanceValidation({
  decoded,
  exploitation,
  plannedKm,
  contractMonths,
  serviceRegime,
}) {
  return validateVinForMaintenance(decoded, {
    serviceRegime: serviceRegime || null,
    usageProfile: normalizeUsageProfile(exploitation),
    plannedKm,
    contractMonths,
  });
}
