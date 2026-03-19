import { buildLine, toNumber } from "./shared.js";
import { applyFleetContractModifiers, mergedUserPricing } from "./contracts.js";

export function getLaborRate(userPricing = {}) {
  const pricing = mergedUserPricing(userPricing);
  return toNumber(pricing.laborRate, 5500);
}

export function priceLabor(hours, userPricing = {}) {
  const pricing = mergedUserPricing(userPricing);
  const unitPrice = applyFleetContractModifiers(getLaborRate(pricing), {
    discountPercent: pricing.laborDiscount,
  });
  return buildLine("labor", "Rad", hours, "h", unitPrice, "labor");
}

export default {
  getLaborRate,
  priceLabor,
};
