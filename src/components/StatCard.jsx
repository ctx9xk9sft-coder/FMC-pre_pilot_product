export default function StatCard({ title, value }) {
  return (
    <div style={{
      background: "#fff",
      borderRadius: 14,
      padding: 18,
      border: "1px solid #e2e8f0"
    }}>
      <div style={{ fontSize: 14, color: "#64748b" }}>{title}</div>
      <div style={{ fontSize: 24, fontWeight: 700 }}>{value}</div>
    </div>
  )
}