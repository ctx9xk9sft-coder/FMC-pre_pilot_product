import React from "react";

export default function FleetKpiSection({
  totalCost,
  contractMonths,
  plannedKm,
  serviceEvents,
  brakeEvents,
  tireEvents
}) {
  const reservePerKm = plannedKm > 0 ? totalCost / plannedKm : 0;
  const reservePerMonth = contractMonths > 0 ? totalCost / contractMonths : 0;

  const serviceVisits = Array.isArray(serviceEvents) ? serviceEvents.length : 0;
  const brakeCycles = Array.isArray(brakeEvents) ? brakeEvents.length : 0;
  const tireCycles = Array.isArray(tireEvents) ? tireEvents.length : 0;

  const formatRsd = (v) =>
    new Intl.NumberFormat("sr-RS").format(Math.round(v || 0)) + " RSD";

  const formatNum = (v) =>
    new Intl.NumberFormat("sr-RS").format(Math.round(v || 0));

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "repeat(5, 1fr)",
        gap: "12px",
        marginTop: "12px",
        marginBottom: "20px"
      }}
    >
      <KpiCard label="Reserve / km" value={formatRsd(reservePerKm)} />
      <KpiCard label="Reserve / month" value={formatRsd(reservePerMonth)} />
      <KpiCard label="Servisne posete" value={formatNum(serviceVisits)} />
      <KpiCard label="Ciklusi kočnica" value={formatNum(brakeCycles)} />
      <KpiCard label="Ciklusi guma" value={formatNum(tireCycles)} />
    </div>
  );
}

function KpiCard({ label, value }) {
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