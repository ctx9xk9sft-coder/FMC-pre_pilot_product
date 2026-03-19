export default function VehicleHeader({
  image,
  title,
  engine,
  gearbox,
  drivetrain,
  powerKw,
}) {
  return (
    <div style={styles.card}>
      <div style={styles.imageWrap}>
        {image ? (
          <img src={image} alt={title} style={styles.image} />
        ) : (
          <div style={styles.imageFallback}>ŠKODA</div>
        )}
      </div>

      <div style={styles.content}>
        <div style={styles.kicker}>Vozilo</div>
        <div style={styles.title}>{title}</div>

        <div style={styles.chips}>
          {engine ? <span style={styles.chip}>{engine}</span> : null}
          {gearbox ? <span style={styles.chip}>{gearbox}</span> : null}
          {drivetrain ? <span style={styles.chip}>{drivetrain}</span> : null}
          {powerKw ? <span style={styles.chip}>{powerKw} kW</span> : null}
        </div>
      </div>
    </div>
  );
}

const styles = {
  card: {
    background: "#fff",
    borderRadius: 24,
    padding: 20,
    boxShadow: "0 1px 10px rgba(0,0,0,0.08)",
    display: "grid",
    gridTemplateColumns: "280px 1fr",
    gap: 20,
    alignItems: "center",
    marginBottom: 20,
  },
  imageWrap: {
    width: "100%",
    height: 170,
    borderRadius: 18,
    overflow: "hidden",
    background: "#f8fafc",
    border: "1px solid #e2e8f0",
  },
  image: {
    width: "100%",
    height: "100%",
    objectFit: "cover",
    display: "block",
  },
  imageFallback: {
    width: "100%",
    height: "100%",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    fontSize: 28,
    fontWeight: 800,
    color: "#0f172a",
    background: "linear-gradient(to bottom right, #e2e8f0, #f8fafc)",
  },
  content: {
    display: "grid",
    gap: 12,
  },
  kicker: {
    fontSize: 13,
    color: "#64748b",
    textTransform: "uppercase",
    letterSpacing: 0.8,
  },
  title: {
    fontSize: 34,
    fontWeight: 800,
    lineHeight: 1.1,
    color: "#0f172a",
  },
  chips: {
    display: "flex",
    flexWrap: "wrap",
    gap: 10,
  },
  chip: {
    display: "inline-flex",
    alignItems: "center",
    padding: "10px 14px",
    borderRadius: 999,
    background: "#f8fafc",
    border: "1px solid #e2e8f0",
    fontSize: 15,
    fontWeight: 700,
    color: "#0f172a",
  },
};