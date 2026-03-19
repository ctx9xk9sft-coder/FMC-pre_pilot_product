import React from "react";

export default function ScenarioComparisonSection({
  scenarios = [],
  formatRsd,
  formatNum
}) {
  if (!Array.isArray(scenarios) || scenarios.length === 0) {
    return null;
  }

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
        Scenario comparison
      </div>

      <div style={{ overflowX: "auto" }}>
        <table
          style={{
            width: "100%",
            borderCollapse: "collapse",
            fontSize: "14px"
          }}
        >
          <thead>
            <tr style={{ textAlign: "left", borderBottom: "1px solid #e2e8f0" }}>
              <th style={thStyle}>Scenario</th>
              <th style={thStyle}>KM</th>
              <th style={thStyle}>Meseci</th>
              <th style={thStyle}>Ukupno održavanje</th>
              <th style={thStyle}>Trošak po km</th>
              <th style={thStyle}>Trošak po mesecu</th>
              <th style={thStyle}>Događaji</th>
            </tr>
          </thead>
          <tbody>
            {scenarios.map((scenario) => (
              <tr
                key={scenario.label}
                style={{ borderBottom: "1px solid #f1f5f9" }}
              >
                <td style={tdStyle}>{scenario.label}</td>
                <td style={tdStyle}>{formatNum(scenario.km)}</td>
                <td style={tdStyle}>{formatNum(scenario.months)}</td>
                <td style={tdStyle}>{formatRsd(scenario.totalCost)}</td>
                <td style={tdStyle}>{formatRsd(scenario.costPerKm)}</td>
                <td style={tdStyle}>{formatRsd(scenario.costPerMonth)}</td>
                <td style={tdStyle}>{formatNum(scenario.eventCount)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

const thStyle = {
  padding: "10px 8px",
  color: "#475569",
  fontWeight: 600,
  fontSize: "13px"
};

const tdStyle = {
  padding: "12px 8px",
  color: "#0f172a"
};