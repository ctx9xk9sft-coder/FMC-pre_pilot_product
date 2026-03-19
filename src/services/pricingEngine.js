import PRICING_RULES from "../data/pricing_rules.json" with { type: 'json' };
import { TIRE_CATALOG } from "../data/tireCatalog.js";
import { mergedUserPricing } from "./pricing/contracts.js";
import { getLaborRate, priceLabor } from "./pricing/laborPricing.js";
import {
  getOilUnitPrice,
  priceOil,
  priceGearboxOil,
  priceHaldexOil,
  priceBrakeFluid,
} from "./pricing/oilPricing.js";
import { getVehiclePartsMap, priceMappedPart } from "./pricing/partsPricing.js";
import { buildLine, roundMoney, toNumber } from "./pricing/shared.js";

function normalizePricingInput(input = {}) {
  const basePlan = input?.maintenancePlan || input?.plan || null;
  const legacyPricing = {
    laborRate: input?.labourRateRsd,
    oilPricePerLiter: input?.engineOilPricePerLitreRsd,
    tireCategory: input?.tyreClass,
  };

  return {
    maintenancePlan: basePlan,
    decoded: input?.decoded || null,
    userPricing: mergedUserPricing({
      ...legacyPricing,
      ...(input?.userPricing || {}),
    }),
  };
}

function getRangeMultiplier(event = {}) {
  const precision = String(event?.pricingPrecision || '').toLowerCase();
  const hasMissing = Number(event?.pricingBreakdown?.filter((line) => line?.pricingStatus === 'incomplete').length || 0) > 0;
  if (hasMissing || precision === 'missing') return { min: 0.8, max: 1.25 };
  if (precision === 'generic') return { min: 0.9, max: 1.15 };
  if (precision === 'family') return { min: 0.93, max: 1.1 };
  return { min: 0.97, max: 1.05 };
}

function summarizePriceRange(pricedEvents = []) {
  return pricedEvents.reduce(
    (acc, event) => {
      const amount = toNumber(event?.estimatedCost, 0);
      const mult = getRangeMultiplier(event);
      acc.min += amount * mult.min;
      acc.max += amount * mult.max;
      return acc;
    },
    { min: 0, max: 0 }
  );
}

function getTirePricingConfig(userPricing = {}, pricingProfile = {}) {
  const pricing = mergedUserPricing(userPricing);
  const requested = String(pricing.tireCategory || pricingProfile.tireSpecKey || "standard").toLowerCase();
  return TIRE_CATALOG[requested] || TIRE_CATALOG.standard;
}

function priceTireSet(tireType, userPricing = {}, pricingProfile = {}) {
  const config = getTirePricingConfig(userPricing, pricingProfile);
  const unitPrice =
    tireType === "winter"
      ? toNumber(config.winterPrice, PRICING_RULES?.missingPricing?.defaultTireUnitPrice || 11500)
      : toNumber(config.summerPrice, PRICING_RULES?.missingPricing?.defaultTireUnitPrice || 11500);
  return {
    ...buildLine(
      tireType === "winter" ? "winter_tires_set" : "summer_tires_set",
      tireType === "winter" ? "Set zimskih pneumatika" : "Set letnjih pneumatika",
      4,
      "pcs",
      unitPrice,
      "tires"
    ),
    pricingStatus: "priced",
    precision: pricingProfile.precision || "generic",
    fallbackUsed: false,
    warning: null,
  };
}

function priceTireChangeService(userPricing = {}, pricingProfile = {}) {
  const config = getTirePricingConfig(userPricing, pricingProfile);
  return {
    ...buildLine(
      "tire_change_service",
      "Montaža i balansiranje",
      1,
      "service",
      toNumber(config.changeService, 2400),
      "tires"
    ),
    pricingStatus: "priced",
    precision: pricingProfile.precision || "generic",
    fallbackUsed: false,
    warning: null,
  };
}

function summarizeLines(lines = []) {
  return {
    hasMissing: lines.some((line) => line?.pricingStatus === "incomplete"),
    hasGeneric: lines.some((line) => ["generic", "model_family"].includes(line?.precision)),
    fallbackLineCount: lines.filter((line) => line?.fallbackUsed).length,
    missingLineCount: lines.filter((line) => line?.pricingStatus === "incomplete").length,
    warnings: lines.map((line) => line?.warning).filter(Boolean),
  };
}

function buildBreakdownForEvent(event, decoded, vehicleContext, userPricing = {}) {
  const lines = [];
  const vehicleMap = getVehiclePartsMap(vehicleContext);
  const pricingProfile = vehicleMap?._profile || {};
  const laborHours = toNumber(event?.pricingContext?.laborHours, 0);
  const partsPrecision = pricingProfile.precision || "generic";

  switch (event.type) {
    case "oil_service": {
      const oilQty = toNumber(event?.pricingContext?.oilLiters, toNumber(decoded?.oilCapacity, 5));
      lines.push(priceOil(decoded, oilQty, userPricing));
      if (vehicleMap.oilFilter) lines.push(priceMappedPart(vehicleMap.oilFilter, 1, userPricing, { precision: partsPrecision }));
      if (vehicleMap.airFilter) lines.push(priceMappedPart(vehicleMap.airFilter, 1, userPricing, { precision: partsPrecision }));
      if (vehicleMap.cabinFilter) lines.push(priceMappedPart(vehicleMap.cabinFilter, 1, userPricing, { precision: partsPrecision }));
      const hasDrainPlug = (event.items || []).includes("drain_plug");
      if (hasDrainPlug) lines.push(priceMappedPart("drain_plug", 1, userPricing, { precision: "exact" }));
      const hasFuelFilter = (event.items || []).includes("fuel_filter");
      if (hasFuelFilter && vehicleMap.fuelFilter) lines.push(priceMappedPart(vehicleMap.fuelFilter, 1, userPricing, { precision: partsPrecision }));
      lines.push(priceLabor(laborHours, userPricing));
      break;
    }
    case "spark_plugs": {
      if (vehicleMap.sparkPlugs) lines.push(priceMappedPart(vehicleMap.sparkPlugs, 1, userPricing, { precision: partsPrecision }));
      lines.push(priceLabor(laborHours, userPricing));
      break;
    }
    case "dsg_service": {
      const oilQty = toNumber(event?.pricingContext?.oilLiters, 6);
      lines.push(priceGearboxOil(oilQty, userPricing));
      const dsgFilterId = vehicleMap.dsgFilter || "dsg_filter_generic";
      lines.push(priceMappedPart(dsgFilterId, 1, userPricing, { precision: vehicleMap.dsgFilter ? partsPrecision : "generic" }));
      lines.push(priceLabor(laborHours, userPricing));
      break;
    }
    case "haldex_service": {
      const oilQty = toNumber(event?.pricingContext?.oilLiters, 1);
      lines.push(priceHaldexOil(oilQty, userPricing));
      lines.push(priceLabor(laborHours, userPricing));
      break;
    }
    case "brake_fluid": {
      lines.push(priceBrakeFluid(1, userPricing));
      lines.push(priceLabor(laborHours, userPricing));
      break;
    }
    case "front_brake_pads": {
      if (vehicleMap.frontPads) lines.push(priceMappedPart(vehicleMap.frontPads, 1, userPricing, { precision: partsPrecision }));
      lines.push(priceLabor(laborHours, userPricing));
      break;
    }
    case "rear_brake_pads": {
      if (vehicleMap.rearPads) lines.push(priceMappedPart(vehicleMap.rearPads, 1, userPricing, { precision: partsPrecision }));
      lines.push(priceLabor(laborHours, userPricing));
      break;
    }
    case "front_brake_discs": {
      if (vehicleMap.frontDiscs) lines.push(priceMappedPart(vehicleMap.frontDiscs, 1, userPricing, { precision: partsPrecision }));
      lines.push(priceLabor(laborHours, userPricing));
      break;
    }
    case "rear_brake_discs": {
      if (vehicleMap.rearDiscs) lines.push(priceMappedPart(vehicleMap.rearDiscs, 1, userPricing, { precision: partsPrecision }));
      lines.push(priceLabor(laborHours, userPricing));
      break;
    }
    case "summer_tires": {
      lines.push(priceTireSet("summer", userPricing, pricingProfile));
      lines.push(priceLabor(laborHours, userPricing));
      break;
    }
    case "winter_tires": {
      lines.push(priceTireSet("winter", userPricing, pricingProfile));
      lines.push(priceLabor(laborHours, userPricing));
      break;
    }
    case "seasonal_tire_change": {
      lines.push(priceTireChangeService(userPricing, pricingProfile));
      lines.push(priceLabor(laborHours, userPricing));
      break;
    }
    default: {
      lines.push(priceLabor(laborHours, userPricing));
      break;
    }
  }

  const lineSummary = summarizeLines(lines);
  const eventPricingStatus = lineSummary.hasMissing ? "partial" : "priced";
  const eventPrecision = lineSummary.hasMissing
    ? "missing"
    : pricingProfile.precision || (lineSummary.hasGeneric ? "generic" : "exact");

  return {
    breakdown: lines,
    pricingProfile,
    eventPricingMeta: {
      status: eventPricingStatus,
      precision: eventPrecision,
      warnings: [...(pricingProfile.warnings || []), ...lineSummary.warnings],
      fallbackLineCount: lineSummary.fallbackLineCount,
      missingLineCount: lineSummary.missingLineCount,
    },
  };
}

function sumLines(lines = []) {
  return roundMoney(lines.reduce((sum, line) => sum + toNumber(line?.total, 0), 0));
}

function summarizePlanPricing(pricedEvents = []) {
  const exactEventCount = pricedEvents.filter((event) => event?.pricingMeta?.precision === "exact").length;
  const familyEventCount = pricedEvents.filter((event) => event?.pricingMeta?.precision === "model_family").length;
  const genericEventCount = pricedEvents.filter((event) => event?.pricingMeta?.precision === "generic").length;
  const missingPriceEventCount = pricedEvents.filter((event) => event?.pricingMeta?.status === "partial").length;
  const fallbackLineCount = pricedEvents.reduce((sum, event) => sum + toNumber(event?.pricingMeta?.fallbackLineCount, 0), 0);
  const missingLineCount = pricedEvents.reduce((sum, event) => sum + toNumber(event?.pricingMeta?.missingLineCount, 0), 0);
  const totalEvents = pricedEvents.length;
  const coveredEvents = totalEvents - missingPriceEventCount;
  const pricingCoveragePercent = totalEvents > 0 ? roundMoney((coveredEvents / totalEvents) * 100) : 100;

  let pricingConfidence = "high";
  if (missingPriceEventCount > 0 || pricingCoveragePercent < 95) {
    pricingConfidence = "low";
  } else if (genericEventCount > 0 || familyEventCount > 0 || fallbackLineCount > 0) {
    pricingConfidence = "medium";
  }

  const warnings = [];
  pricedEvents.forEach((event) => {
    (event?.pricingMeta?.warnings || []).forEach((warning) => warnings.push(warning));
  });

  return {
    pricingCoveragePercent,
    exactEventCount,
    familyEventCount,
    genericEventCount,
    missingPriceEventCount,
    fallbackLineCount,
    missingLineCount,
    pricingConfidence,
    warnings: [...new Set(warnings)],
  };
}

export function priceMaintenancePlan(input = {}) {
  const normalized = normalizePricingInput(input);
  const { maintenancePlan, decoded, userPricing } = normalized;

  if (!maintenancePlan) return null;

  const pricedEvents = (maintenancePlan.events || []).map((event) => {
    const { breakdown, pricingProfile, eventPricingMeta } = buildBreakdownForEvent(
      event,
      decoded,
      maintenancePlan.vehicle || {},
      userPricing
    );
    return {
      ...event,
      pricingBreakdown: breakdown,
      estimatedCost: sumLines(breakdown),
      pricingMeta: {
        ...eventPricingMeta,
        profileKey: pricingProfile.partsProfileKey || null,
        matchedBy: pricingProfile.matchedBy || null,
      },
    };
  });

  const totalBrakeCost = sumLines(pricedEvents.filter((event) => event.category === "brakes").map((event) => ({ total: event.estimatedCost })));
  const totalTireCost = sumLines(pricedEvents.filter((event) => event.category === "tires").map((event) => ({ total: event.estimatedCost })));
  const totalServiceCost = sumLines(
    pricedEvents.filter((event) => event.category !== "brakes" && event.category !== "tires").map((event) => ({ total: event.estimatedCost }))
  );

  const totalCost = roundMoney(totalServiceCost + totalBrakeCost + totalTireCost);
  const totalEvents = pricedEvents.length;
  const coverage = summarizePlanPricing(pricedEvents);
  const priceRange = summarizePriceRange(pricedEvents);

  return {
    ...maintenancePlan,
    events: pricedEvents,
    totals: {
      ...(maintenancePlan.totals || {}),
      totalServiceCost,
      totalBrakeCost,
      totalTireCost,
      totalCost,
      totalEvents,
      service: totalServiceCost,
      brakes: totalBrakeCost,
      tyres: totalTireCost,
      total: totalCost,
      events: totalEvents,
      totalCostMin: roundMoney(priceRange.min),
      totalCostExpected: totalCost,
      totalCostMax: roundMoney(priceRange.max),
      pricingRange: {
        min: roundMoney(priceRange.min),
        expected: totalCost,
        max: roundMoney(priceRange.max),
      },
    },
    meta: {
      ...(maintenancePlan.meta || {}),
      pricingVersion: "pricing-engine-v4-precision-hardened",
      pricingStatus: coverage.missingPriceEventCount > 0 ? "partial" : "priced",
    },
    pricingMeta: {
      laborRate: getLaborRate(userPricing),
      oilPricePerLiter: getOilUnitPrice(userPricing),
      tireCategory: String(userPricing.tireCategory || "standard").toLowerCase(),
      discounts: {
        laborDiscount: toNumber(userPricing.laborDiscount, 0),
        partsDiscount: toNumber(userPricing.partsDiscount, 0),
        oilDiscount: toNumber(userPricing.oilDiscount, 0),
      },
      contractMode: userPricing.contractMode || "standard",
      ...coverage,
      pricingRange: {
        min: roundMoney(priceRange.min),
        expected: totalCost,
        max: roundMoney(priceRange.max),
      },
    },
  };
}

export default priceMaintenancePlan;
