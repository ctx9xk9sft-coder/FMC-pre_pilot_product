export default function CostSummarySection({
  totalCost,
  serviceCost,
  brakeCost,
  tireCost,
  costPerKm,
  costPerMonth,
  eventCount
}) {

  return (
    <div style={styles.row}>

      <Card label="Ukupno održavanje" value={totalCost} />

      <Card label="Servisi" value={serviceCost} />

      <Card label="Kočnice" value={brakeCost} />

      <Card label="Gume" value={tireCost} />

      <Card label="Trošak po km" value={costPerKm} />

      <Card label="Trošak po mesecu" value={costPerMonth} />

      <Card label="Ukupni događaji" value={eventCount} />

    </div>
  )

}

function Card({label,value}){

  return(

    <div style={styles.card}>

      <div style={styles.label}>
        {label}
      </div>

      <div style={styles.value}>
        {value}
      </div>

    </div>

  )

}

const styles = {

  row:{
    display:"grid",
    gridTemplateColumns:"repeat(7,1fr)",
    gap:10,
    marginBottom:20
  },

  card:{
    background:"#fff",
    borderRadius:16,
    padding:16,
    boxShadow:"0 1px 8px rgba(0,0,0,0.08)"
  },

  label:{
    fontSize:12,
    color:"#64748b"
  },

  value:{
    fontSize:18,
    fontWeight:700
  }

}