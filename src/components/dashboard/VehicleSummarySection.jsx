export default function VehicleSummarySection({
  vehicleLabel,
  engineLabel,
  gearboxLabel,
  drivetrainLabel,
  plannedKm,
  contractMonths,
  exploitationLabel,
  tireCategory,
  hourlyRate,
  oilPricePerLiter
}) {
  return (
    <div style={styles.card}>

      <div style={styles.title}>
        Sažetak vozila i ugovora
      </div>

      <div style={styles.grid}>

        <div style={styles.vehicleCard}>
          <div style={styles.vehicleTitle}>{vehicleLabel}</div>
        </div>

        <div style={styles.specCard}>
          <div>{engineLabel}</div>
          <div>{gearboxLabel}</div>
          <div>{drivetrainLabel}</div>
        </div>

        <div style={styles.infoCard}>
          {contractMonths} mes / {plannedKm.toLocaleString()} km
        </div>

        <div style={styles.infoCard}>
          {exploitationLabel}
        </div>

        <div style={styles.infoCard}>
          gume: {tireCategory}
        </div>

        <div style={styles.infoCard}>
          {hourlyRate.toLocaleString()} RSD / {oilPricePerLiter.toLocaleString()} RSD
        </div>

      </div>

    </div>
  )
}

const styles = {

  card: {
    background: "#fff",
    borderRadius: 24,
    padding: 24,
    boxShadow: "0 2px 12px rgba(0,0,0,0.08)",
    marginBottom: 20
  },

  title: {
    fontSize: 20,
    fontWeight: 700,
    marginBottom: 16
  },

  grid: {
    display: "grid",
    gridTemplateColumns: "repeat(3,1fr)",
    gap: 12
  },

  vehicleCard: {
    gridColumn: "span 3",
    background: "#f1f5f9",
    borderRadius: 16,
    padding: 16
  },

  vehicleTitle: {
    fontSize: 22,
    fontWeight: 700
  },

  specCard: {
    background: "#f8fafc",
    borderRadius: 16,
    padding: 16,
    display: "flex",
    gap: 12
  },

  infoCard: {
    background: "#f8fafc",
    borderRadius: 16,
    padding: 16
  }

}