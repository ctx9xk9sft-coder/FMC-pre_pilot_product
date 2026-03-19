import { PARTS_CATALOG } from "../../data/partsCatalog.js";
import PRICING_RULES from "../../data/pricing_rules.json" with { type: 'json' };
import { buildLine, toNumber } from "./shared.js";
import { applyFleetContractModifiers, mergedUserPricing } from "./contracts.js";

export function getOilUnitPrice(userPricing = {}) {
  const pricing = mergedUserPricing(userPricing);
  return toNumber(pricing.oilPricePerLiter, 1800);
}

export function priceOil(decoded, qty, userPricing = {}) {
  const pricing = mergedUserPricing(userPricing);
  const unitPrice = applyFleetContractModifiers(getOilUnitPrice(pricing), {
    discountPercent: pricing.oilDiscount,
  });
  return buildLine("engine_oil", "Motorno ulje", qty, "liter", unitPrice, "oil");
}

function priceCatalogOilLine({ id, name, qty, fallbackPrice, userPricing = {} }) {
  const pricing = mergedUserPricing(userPricing);
  const part = PARTS_CATALOG[id];
  const unitPrice = applyFleetContractModifiers(toNumber(part?.basePrice, fallbackPrice), {
    discountPercent: pricing.partsDiscount,
  });
  return buildLine(id, name, qty, "liter", unitPrice, "parts");
}

export function priceGearboxOil(qty, userPricing = {}) {
  return priceCatalogOilLine({
    id: "gearbox_oil",
    name: "Ulje menjača",
    qty,
    fallbackPrice: PRICING_RULES?.consumables?.gearboxOilUnitPrice || 3500,
    userPricing,
  });
}

export function priceHaldexOil(qty, userPricing = {}) {
  return priceCatalogOilLine({
    id: "haldex_oil",
    name: "Ulje Haldex pogona",
    qty,
    fallbackPrice: PRICING_RULES?.consumables?.haldexOilUnitPrice || 4200,
    userPricing,
  });
}

export function priceBrakeFluid(qty, userPricing = {}) {
  return priceCatalogOilLine({
    id: "brake_fluid",
    name: "Kočiona tečnost",
    qty,
    fallbackPrice: PRICING_RULES?.consumables?.brakeFluidUnitPrice || 1800,
    userPricing,
  });
}

export default {
  getOilUnitPrice,
  priceOil,
  priceGearboxOil,
  priceHaldexOil,
  priceBrakeFluid,
};
