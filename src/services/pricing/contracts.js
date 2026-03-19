import PRICING_RULES from "../../data/pricing_rules.json" with { type: 'json' };
import { toNumber } from "./shared.js";

export function mergedUserPricing(userPricing = {}) {
  return {
    ...PRICING_RULES.defaults,
    ...userPricing,
  };
}

export function getDiscountFactor(discountPercent) {
  return 1 - toNumber(discountPercent, 0) / 100;
}

export function applyFleetContractModifiers(unitPrice, { discountPercent = 0, multiplier = 1 } = {}) {
  return toNumber(unitPrice, 0) * getDiscountFactor(discountPercent) * toNumber(multiplier, 1);
}

export default {
  mergedUserPricing,
  getDiscountFactor,
  applyFleetContractModifiers,
};
