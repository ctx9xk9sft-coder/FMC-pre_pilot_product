import PRICING_RULES from "../../data/pricing_rules.json" with { type: 'json' };
import { PARTS_CATALOG } from "../../data/partsCatalog.js";
import { VEHICLE_PARTS_MAP } from "../../data/vehiclePartsMap.js";
import BRAKE_SPECS from "../../data/brake_specs.json" with { type: 'json' };
import { buildLine, toNumber } from "./shared.js";
import { applyFleetContractModifiers, mergedUserPricing } from "./contracts.js";
import { resolvePricingProfile } from "./resolvePricingProfile.js";

function inferMissingPartFallback(partId) {
  const normalized = String(partId || "").toLowerCase();
  const defaults = PRICING_RULES?.missingPricing || {};

  if (normalized.includes("brake")) return toNumber(defaults.defaultBrakeUnitPrice, 22000);
  if (normalized.includes("filter")) return toNumber(defaults.defaultFilterUnitPrice, 3500);
  return toNumber(defaults.defaultPartUnitPrice, 6500);
}

export function detectVehiclePartsKey(vehicleContext = {}) {
  return resolvePricingProfile(vehicleContext).partsProfileKey;
}

export function getVehiclePartsMap(vehicleContext = {}) {
  const profile = resolvePricingProfile(vehicleContext);
  const base =
    VEHICLE_PARTS_MAP[profile.partsProfileKey] || VEHICLE_PARTS_MAP.generic_tsi_small || {};
  const brakeSpec =
    BRAKE_SPECS?.[profile.brakeSpecKey] || BRAKE_SPECS?.[profile.partsProfileKey] || null;

  const map = !brakeSpec
    ? { ...base }
    : {
        ...base,
        frontPads: brakeSpec.frontPadsPartId || base.frontPads,
        rearPads: brakeSpec.rearPadsPartId || base.rearPads,
        frontDiscs: brakeSpec.frontDiscsPartId || base.frontDiscs,
        rearDiscs: brakeSpec.rearDiscsPartId || base.rearDiscs,
      };

  return {
    ...map,
    _profile: profile,
  };
}

export function priceMappedPart(partId, qty = 1, userPricing = {}, options = {}) {
  const part = partId ? PARTS_CATALOG[partId] || null : null;
  const pricing = mergedUserPricing(userPricing);
  const precision = options.precision || "exact";

  if (!part) {
    const fallbackUnitPrice = applyFleetContractModifiers(inferMissingPartFallback(partId), {
      discountPercent: pricing.partsDiscount,
    });
    return {
      ...buildLine(
        partId || "unknown_part",
        partId ? `Procena za ${partId}` : "Procena za nepoznatu stavku",
        qty,
        "pcs",
        fallbackUnitPrice,
        "parts"
      ),
      pricingStatus: "incomplete",
      precision: "missing",
      fallbackUsed: true,
      warning: `Nedostaje katalog za deo ${partId || "unknown_part"}; korišćena je fallback procena.`,
    };
  }

  const unitPrice = applyFleetContractModifiers(toNumber(part.basePrice, 0), {
    discountPercent: pricing.partsDiscount,
  });

  return {
    ...buildLine(part.id, part.name, qty, part.unit, unitPrice, "parts"),
    pricingStatus: "priced",
    precision,
    fallbackUsed: false,
    warning: null,
  };
}

export default {
  detectVehiclePartsKey,
  getVehiclePartsMap,
  priceMappedPart,
};
