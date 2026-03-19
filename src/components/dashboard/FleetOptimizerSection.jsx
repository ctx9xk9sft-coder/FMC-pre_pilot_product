import React from "react";

export default function FleetOptimizerSection({ scenarios = [], formatRsd, formatNum }) {
  if (!Array.isArray(scenarios) || scenarios.length === 0) {
    return null;
  }

  const cheapestTotal = [...scenarios].sort((a, b) => a.totalCost - b.totalCost)[0];
  const cheapestPerKm = [...scenarios].sort((a, b) => a.costPerKm - b.costPerKm)[0];
  const cheapestPerMonth = [...scenarios].sort((a, b) => a.costPerMonth - b.costPerMonth)[0];

  return (
    <div
      style={{
        background: "#ffffff",
        border: "1px solid #e2e8f0",
        borderRadius: "16px",
        padding: "18px",
        marginBottom: "20px"
      }}
    >
      <div
        style={{
          fontSize: "20px",
          fontWeight: 700,
          marginBottom: "14px",
          color: "#0f172a"
        }}
      >
        Fleet optimizer
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(3, 1fr)",
          gap: "12px"
        }}
      >
        <OptimizerCard
          label="Najniži ukupni trošak"
          scenario={cheapestTotal?.label}
          value={formatRsd(cheapestTotal?.totalCost || 0)}
        />
        <OptimizerCard
          label="Najniži trošak po km"
          scenario={cheapestPerKm?.label}
          value={formatRsd(cheapestPerKm?.costPerKm || 0)}
        />
        <OptimizerCard
          label="Najniži trošak po mesecu"
          scenario={cheapestPerMonth?.label}
          value={formatRsd(cheapestPerMonth?.costPerMonth || 0)}
        />
      </div>
    </div>
  );
}

function OptimizerCard({ label, scenario, value }) {
  return (
    <div
      style={{
        background: "#f8fafc",
        border: "1px solid #e2e8f0",
        borderRadius: "12px",
        padding: "14px"
      }}
    >
      <div style={{ fontSize: "12px", color: "#64748b", marginBottom: "6px" }}>
        {label}
      </div>
      <div style={{ fontSize: "16px", fontWeight: 700, color: "#0f172a" }}>
        {scenario || "-"}
      </div>
      <div style={{ fontSize: "14px", color: "#334155", marginTop: "4px" }}>
        {value}
      </div>
    </div>
  );
}