import MAINTENANCE_RULES from "../../data/maintenance_rules.json" with { type: 'json' };
import SERVICE_PARTS from "../../data/service_parts.json" with { type: 'json' };
import LABOR_OPERATIONS from "../../data/labor_operations.json" with { type: 'json' };

function toNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

export function getMaintenanceRule(type) {
  return MAINTENANCE_RULES?.[type] || null;
}

export function getLaborHours(type, fallback = 1) {
  return toNumber(LABOR_OPERATIONS?.[type]?.hours, fallback);
}

export function getServiceParts(eventType, options = {}) {
  const config = SERVICE_PARTS?.[eventType];
  const base = Array.isArray(config?.default)
    ? config.default.map((item) => item.partType)
    : [];

  if (eventType === "oil_service" && options.includeDieselSecondService) {
    const extra = Array.isArray(config?.dieselEverySecondServiceAdds)
      ? config.dieselEverySecondServiceAdds.map((item) => item.partType)
      : [];
    return [...base, ...extra];
  }

  return base;
}

export function getOilIntervalConfig({ serviceRegime, usageProfile, annualKm, flexibleServiceIntervalKm = 0 }) {
  const oilRule = getMaintenanceRule("oil_service");
  const regimeConfig = oilRule?.serviceRegimes?.[serviceRegime];
  const usageConfig = regimeConfig?.usageAdjustments?.[usageProfile];

  let km = 30000;
  let months = 24;

  if (serviceRegime === "flex" && flexibleServiceIntervalKm > 0) {
    km = flexibleServiceIntervalKm;
  } else if (usageConfig?.km) {
    km = usageConfig.km;
  } else if (regimeConfig?.baseKm) {
    km = regimeConfig.baseKm;
  } else if (serviceRegime === "fixed_15000") {
    km = 15000;
  } else if (serviceRegime === "fixed_10000") {
    km = 10000;
  } else {
    if (usageProfile === "city_heavy") km = 20000;
    else if (usageProfile === "mixed") km = 25000;
    if (annualKm > 45000) km = Math.min(km, 25000);
  }

  if (usageConfig?.months) {
    months = usageConfig.months;
  } else if (regimeConfig?.baseMonths) {
    months = regimeConfig.baseMonths;
  } else if (serviceRegime === "fixed_15000" || serviceRegime === "fixed_10000" || usageProfile === "city_heavy") {
    months = 12;
  }

  return { km, months, sourceRule: oilRule?.sourceRule || "base_oil_service_rule" };
}

export default {
  getMaintenanceRule,
  getLaborHours,
  getServiceParts,
  getOilIntervalConfig,
};
