import React from "react";

export default function BusinessDashboard({
  totalCost,
  serviceCost,
  brakeCost,
  tireCost,
  costPerKm,
  costPerMonth,
  eventCount
}) {
  const formatRsd = (v) =>
    new Intl.NumberFormat("sr-RS").format(Math.round(v || 0)) + " RSD";

  const formatNum = (v) =>
    new Intl.NumberFormat("sr-RS").format(Math.round(v || 0));

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "repeat(7, 1fr)",
        gap: "12px",
        marginTop: "20px",
        marginBottom: "20px"
      }}
    >
      <Stat label="Ukupno održavanje" value={formatRsd(totalCost)} />
      <Stat label="Servisi" value={formatRsd(serviceCost)} />
      <Stat label="Kočnice" value={formatRsd(brakeCost)} />
      <Stat label="Gume" value={formatRsd(tireCost)} />
      <Stat label="Trošak po km" value={formatRsd(costPerKm)} />
      <Stat label="Trošak po mesecu" value={formatRsd(costPerMonth)} />
      <Stat label="Ukupni događaji" value={formatNum(eventCount)} />
    </div>
  );
}

function Stat({ label, value }) {
  return (
    <div
      style={{
        background: "#f8fafc",
        padding: "14px",
        borderRadius: "10px",
        border: "1px solid #e2e8f0"
      }}
    >
      <div style={{ fontSize: "12px", color: "#64748b" }}>{label}</div>
      <div style={{ fontSize: "18px", fontWeight: "600" }}>{value}</div>
    </div>
  );
}