export default function InfoCard({ label, value }) {
  return (
    <div style={{
      border: "1px solid #e2e8f0",
      borderRadius: 14,
      padding: 16,
      background: "#fff"
    }}>
      <div style={{ fontSize: 13, color: "#64748b" }}>{label}</div>
      <div style={{ fontSize: 18, fontWeight: 600 }}>{value}</div>
    </div>
  )
}