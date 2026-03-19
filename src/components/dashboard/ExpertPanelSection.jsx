export default function ExpertPanelSection({
  resolverStatus,
  vehicleGate,
  planningGate
}){

  return(

    <div style={styles.card}>

      <div style={styles.title}>
        Expert Validation
      </div>

      <div style={styles.row}>

        <Status label="Resolver" value={resolverStatus}/>

        <Status label="Vehicle gate" value={vehicleGate}/>

        <Status label="Planning gate" value={planningGate}/>

      </div>

    </div>

  )

}

function Status({label,value}){

  return(

    <div style={styles.status}>

      <div>{label}</div>

      <div style={styles.badge}>
        {value}
      </div>

    </div>

  )

}

const styles={

  card:{
    background:"#fff",
    borderRadius:20,
    padding:20,
    marginTop:20
  },

  title:{
    fontSize:18,
    fontWeight:700,
    marginBottom:12
  },

  row:{
    display:"flex",
    gap:12
  },

  status:{
    background:"#f1f5f9",
    borderRadius:12,
    padding:12
  },

  badge:{
    fontWeight:700
  }

}