import React from "react";

export default function CostStructureSection({
  totalCost,
  serviceCost,
  brakeCost,
  tireCost
}) {
  const safeTotal = totalCost || 0;

  const servicePct = safeTotal > 0 ? (serviceCost / safeTotal) * 100 : 0;
  const brakePct = safeTotal > 0 ? (brakeCost / safeTotal) * 100 : 0;
  const tirePct = safeTotal > 0 ? (tireCost / safeTotal) * 100 : 0;

  const otherCost = Math.max(
    0,
    safeTotal - (serviceCost || 0) - (brakeCost || 0) - (tireCost || 0)
  );
  const otherPct = safeTotal > 0 ? (otherCost / safeTotal) * 100 : 0;

  const formatRsd = (v) =>
    new Intl.NumberFormat("sr-RS").format(Math.round(v || 0)) + " RSD";

  const formatPct = (v) => `${Math.round(v || 0)}%`;

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
        Cost structure
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(4, 1fr)",
          gap: "12px"
        }}
      >
        <CostCard
          label="Servisi"
          value={formatRsd(serviceCost)}
          percent={formatPct(servicePct)}
        />
        <CostCard
          label="Kočnice"
          value={formatRsd(brakeCost)}
          percent={formatPct(brakePct)}
        />
        <CostCard
          label="Gume"
          value={formatRsd(tireCost)}
          percent={formatPct(tirePct)}
        />
        <CostCard
          label="Ostalo"
          value={formatRsd(otherCost)}
          percent={formatPct(otherPct)}
        />
      </div>
    </div>
  );
}

function CostCard({ label, value, percent }) {
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
      <div style={{ fontSize: "18px", fontWeight: 700, color: "#0f172a" }}>
        {value}
      </div>
      <div style={{ fontSize: "13px", color: "#334155", marginTop: "4px" }}>
        {percent}
      </div>
    </div>
  );
}