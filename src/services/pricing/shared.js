export function toNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

export function roundMoney(value) {
  return Math.round((toNumber(value) + Number.EPSILON) * 100) / 100;
}

export function buildLine(id, name, qty, unit, unitPrice, category) {
  const safeQty = toNumber(qty, 0);
  const safePrice = toNumber(unitPrice, 0);

  return {
    id,
    name,
    qty: safeQty,
    unit,
    unitPrice: roundMoney(safePrice),
    total: roundMoney(safeQty * safePrice),
    category,
  };
}
