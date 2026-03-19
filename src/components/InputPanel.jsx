import engineBusinessGroups from "../data/engine_business_groups.json" with { type: 'json' };
import gearboxBusinessGroups from "../data/gearbox_business_groups.json" with { type: 'json' };

export default function InputPanel({
  viewMode,
  selectedProfileId,
  loadProfile,
  vin,
  setVin,
  plannedKm,
  setPlannedKm,
  contractMonths,
  setContractMonths,
  exploitationType,
  setExploitationType,
  hourlyRate,
  setHourlyRate,
  oilPricePerLiter,
  setOilPricePerLiter,
  tireCategory,
  setTireCategory,
  flexInterval,
  setFlexInterval,
  laborDiscount,
  setLaborDiscount,
  partsDiscount,
  setPartsDiscount,
  oilDiscount,
  setOilDiscount,
  exploitation,
  decoded,
  annualKm,
  formatRsd,
  formatNum,
  EXPLOITATION_PROFILES,
  engineOverride,
  setEngineOverride,
  gearboxOverride,
  setGearboxOverride,
  drivetrainOverride,
  setDrivetrainOverride,
  resolverMissingConfirmations,
}) {
  const isExpert = viewMode === "expert";
  const hasMissingConfirmations = resolverMissingConfirmations?.length > 0;
  const engineBusinessOptions = Object.values(engineBusinessGroups);
const gearboxBusinessOptions = Object.values(gearboxBusinessGroups);

  return (
    <div style={styles.sidebarCard}>
      <div style={styles.headerRow}>
        <div>
          <h2 style={styles.sectionTitle}>
            {isExpert ? "Fleet TCO Calculator" : "Unos parametara"}
          </h2>
          <div style={styles.subtitle}>
            {isExpert
              ? "Napredni unos i tehnička kontrola kalkulacije."
              : "Unesi vozilo, dopuni podatke i podesi uslove održavanja."}
          </div>
        </div>

        <div
          style={{
            ...styles.modeBadge,
            ...(isExpert ? styles.modeBadgeExpert : styles.modeBadgeBusiness),
          }}
        >
          {isExpert ? "Expert" : "Business"}
        </div>
      </div>

      <div style={styles.sectionBlock}>
        <div style={styles.blockTitle}>1. Vozilo</div>

        <label style={styles.label}>Test profil</label>
        <select
          style={styles.input}
          value={selectedProfileId}
          onChange={(e) => loadProfile(e.target.value)}
        >
          <option value="octavia_diesel_dsg">Octavia 2.0 TDI DSG</option>
          <option value="octavia_petrol_dsg">Octavia 1.5 TSI DSG</option>
          <option value="superb_diesel_dsg">Superb 2.0 TDI DSG</option>
          <option value="kodiaq_diesel_dsg">Kodiaq 2.0 TDI DSG 4x4</option>
        </select>

        <label style={styles.label}>VIN</label>
        <input
          style={styles.input}
          value={vin}
          onChange={(e) => setVin(e.target.value)}
          placeholder="Unesi VIN"
        />
      </div>

      <div style={styles.sectionBlock}>
        <div style={styles.blockTitle}>2. Dopuna vozila</div>

        {hasMissingConfirmations ? (
          <div style={styles.manualHint}>
            Potrebna dopuna: {resolverMissingConfirmations.join(", ")}
          </div>
        ) : (
          <div style={styles.manualHintOk}>Nema obaveznih ručnih potvrda.</div>
        )}

<label style={styles.label}>Motor</label>
{isExpert ? (
  <input
    style={styles.input}
    value={engineOverride}
    onChange={(e) => setEngineOverride(e.target.value.toUpperCase())}
    placeholder="npr. DTRD"
  />
) : (
  <select
    style={styles.input}
    value={engineOverride}
    onChange={(e) => setEngineOverride(e.target.value)}
  >
    <option value="">Izaberi motor</option>
    {engineBusinessOptions.map((group) => (
      <option key={group.label} value={group.engineCodes[0]}>
        {group.label}
      </option>
    ))}
  </select>
)}

<label style={styles.label}>Menjač</label>
{isExpert ? (
  <input
    style={styles.input}
    value={gearboxOverride}
    onChange={(e) => setGearboxOverride(e.target.value.toUpperCase())}
    placeholder="npr. WJV ili DSG"
  />
) : (
  <select
    style={styles.input}
    value={gearboxOverride}
    onChange={(e) => setGearboxOverride(e.target.value)}
  >
    <option value="">Izaberi menjač</option>
    {gearboxBusinessOptions.map((group) => (
      <option key={group.label} value={group.label === "Automatik" ? "DSG" : "manual"}>
        {group.label}
      </option>
    ))}
  </select>
)}

        <label style={styles.label}>Pogon</label>
        <select
          style={styles.input}
          value={drivetrainOverride}
          onChange={(e) => setDrivetrainOverride(e.target.value)}
        >
          <option value="">Bez potvrde</option>
          <option value="FWD">FWD</option>
          <option value="AWD">AWD</option>
        </select>
      </div>

      <div style={styles.sectionBlock}>
        <div style={styles.blockTitle}>3. Ugovor i korišćenje</div>

        <div style={styles.twoCol}>
          <div>
            <label style={styles.label}>Planirana kilometraža</label>
            <input
              style={styles.input}
              type="number"
              value={plannedKm}
              onChange={(e) => setPlannedKm(Number(e.target.value) || 0)}
            />
          </div>
          <div>
            <label style={styles.label}>Trajanje ugovora</label>
            <input
              style={styles.input}
              type="number"
              value={contractMonths}
              onChange={(e) => setContractMonths(Number(e.target.value) || 0)}
            />
          </div>
        </div>

        <label style={styles.label}>Tip eksploatacije</label>
        <select
          style={styles.input}
          value={exploitationType}
          onChange={(e) => setExploitationType(e.target.value)}
        >
          {Object.entries(EXPLOITATION_PROFILES).map(([key, item]) => (
            <option key={key} value={key}>
              {item.label}
            </option>
          ))}
        </select>

        <label style={styles.label}>Fleksibilni servisni interval</label>
        <select
          style={styles.input}
          value={flexInterval}
          onChange={(e) => setFlexInterval(Number(e.target.value))}
        >
          <option value={15000}>15.000 km</option>
          <option value={20000}>20.000 km</option>
          <option value={25000}>25.000 km</option>
          <option value={30000}>30.000 km</option>
        </select>

        <div style={styles.quickActionsTitle}>Brzi izbor</div>
        <div style={styles.buttonRow}>
          <button style={styles.secondaryBtn} onClick={() => setPlannedKm(120000)}>
            120.000 km
          </button>
          <button style={styles.secondaryBtn} onClick={() => setPlannedKm(200000)}>
            200.000 km
          </button>
          <button
            style={styles.secondaryBtn}
            onClick={() => setFlexInterval(exploitation.recommendedFlex)}
          >
            Preporučeni interval
          </button>
        </div>
      </div>

      <div style={styles.sectionBlock}>
        <div style={styles.blockTitle}>4. Cenovni parametri</div>

        <div style={styles.twoCol}>
          <div>
            <label style={styles.label}>Cena radnog sata (RSD)</label>
            <input
              style={styles.input}
              type="number"
              value={hourlyRate}
              onChange={(e) => setHourlyRate(Number(e.target.value) || 0)}
            />
          </div>
          <div>
            <label style={styles.label}>Cena motornog ulja / L</label>
            <input
              style={styles.input}
              type="number"
              value={oilPricePerLiter}
              onChange={(e) => setOilPricePerLiter(Number(e.target.value) || 0)}
            />
          </div>
        </div>

        <div style={styles.twoCol}>
          <div>
            <label style={styles.label}>Klasa guma</label>
            <select
              style={styles.input}
              value={tireCategory}
              onChange={(e) => setTireCategory(e.target.value)}
            >
              <option value="economy">Economy</option>
              <option value="standard">Standard</option>
              <option value="premium">Premium</option>
            </select>
          </div>
          <div>
            <label style={styles.label}>Brzi izbor guma</label>
            <button
              style={{ ...styles.secondaryBtn, width: "100%", marginTop: 0, padding: "14px 16px" }}
              onClick={() => setTireCategory("standard")}
            >
              Standard gume
            </button>
          </div>
        </div>

        <div style={styles.threeCol}>
          <div>
            <label style={styles.label}>Rabat rad %</label>
            <input
              style={styles.input}
              type="number"
              value={laborDiscount}
              onChange={(e) => setLaborDiscount(Number(e.target.value) || 0)}
            />
          </div>
          <div>
            <label style={styles.label}>Rabat delovi %</label>
            <input
              style={styles.input}
              type="number"
              value={partsDiscount}
              onChange={(e) => setPartsDiscount(Number(e.target.value) || 0)}
            />
          </div>
          <div>
            <label style={styles.label}>Rabat ulje %</label>
            <input
              style={styles.input}
              type="number"
              value={oilDiscount}
              onChange={(e) => setOilDiscount(Number(e.target.value) || 0)}
            />
          </div>
        </div>

        <div style={styles.buttonRow}>
          <button
            style={styles.secondaryBtn}
            onClick={() => {
              setLaborDiscount(10);
              setPartsDiscount(12);
              setOilDiscount(18);
            }}
          >
            Primer rabata
          </button>
        </div>
      </div>

      {isExpert ? (
        <div style={styles.helperBox}>
          <div>1 h = 100 TU</div>
          <div>Preporučena satnica za klasu: {formatRsd(decoded.hourlyRate || hourlyRate)}</div>
          <div>
            Cena 1 TU posle rabata:{" "}
            {formatRsd(((hourlyRate || 0) / 100) * (1 - laborDiscount / 100))}
          </div>
          <div>Količina ulja: {decoded.oilCapacity ? `${decoded.oilCapacity} L` : "-"}</div>
          <div>Godišnja kilometraža: {formatNum(annualKm, 0)} km</div>
          <div>Model habanja kočnica: {exploitation.label}</div>
        </div>
      ) : (
        <div style={styles.businessHelperBox}>
          <div style={styles.businessHelperRow}>
            <span>Godišnja kilometraža</span>
            <strong>{formatNum(annualKm, 0)} km</strong>
          </div>
          <div style={styles.businessHelperRow}>
            <span>Servisni režim</span>
            <strong>Flex / {formatNum(flexInterval, 0)} km</strong>
          </div>
          <div style={styles.businessHelperRow}>
            <span>Model habanja</span>
            <strong>{exploitation.label}</strong>
          </div>
        </div>
      )}
    </div>
  );
}

const styles = {
  sidebarCard: {
    background: "#fff",
    borderRadius: 28,
    padding: 28,
    boxShadow: "0 1px 10px rgba(0,0,0,0.08)",
  },
  headerRow: {
    display: "flex",
    justifyContent: "space-between",
    gap: 12,
    alignItems: "start",
    marginBottom: 8,
  },
  sectionTitle: {
    fontSize: 22,
    margin: "0 0 6px",
  },
  subtitle: {
    fontSize: 14,
    color: "#64748b",
  },
  modeBadge: {
    padding: "8px 12px",
    borderRadius: 999,
    fontSize: 13,
    fontWeight: 800,
    border: "1px solid transparent",
    whiteSpace: "nowrap",
  },
  modeBadgeBusiness: {
    background: "#dcfce7",
    color: "#166534",
    border: "1px solid #86efac",
  },
  modeBadgeExpert: {
    background: "#0f172a",
    color: "#fff",
    border: "1px solid #0f172a",
  },
  sectionBlock: {
    marginTop: 18,
    paddingTop: 18,
    borderTop: "1px solid #e2e8f0",
  },
  blockTitle: {
    fontSize: 17,
    fontWeight: 800,
    color: "#0f172a",
    marginBottom: 6,
  },
  label: {
    display: "block",
    marginBottom: 8,
    marginTop: 12,
    fontWeight: 700,
    color: "#334155",
  },
  input: {
    width: "100%",
    padding: "14px 16px",
    borderRadius: 14,
    border: "1px solid #cbd5e1",
    fontSize: 18,
    outline: "none",
    marginBottom: 2,
    background: "#fff",
  },
  twoCol: {
    display: "grid",
    gridTemplateColumns: "1fr 1fr",
    gap: 12,
  },
  threeCol: {
    display: "grid",
    gridTemplateColumns: "1fr 1fr 1fr",
    gap: 12,
  },
  quickActionsTitle: {
    marginTop: 18,
    fontSize: 16,
    fontWeight: 700,
    color: "#0f172a",
  },
  buttonRow: {
    display: "flex",
    gap: 10,
    flexWrap: "wrap",
    marginTop: 12,
  },
  secondaryBtn: {
    border: "1px solid #cbd5e1",
    background: "#fff",
    color: "#0f172a",
    padding: "12px 14px",
    borderRadius: 12,
    cursor: "pointer",
    fontSize: 15,
    fontWeight: 700,
  },
  helperBox: {
    marginTop: 20,
    paddingTop: 16,
    borderTop: "1px solid #e2e8f0",
    display: "grid",
    gap: 8,
    color: "#334155",
    fontSize: 16,
  },
  businessHelperBox: {
    marginTop: 20,
    paddingTop: 16,
    borderTop: "1px solid #e2e8f0",
    display: "grid",
    gap: 10,
  },
  businessHelperRow: {
    display: "flex",
    justifyContent: "space-between",
    gap: 10,
    fontSize: 15,
    color: "#334155",
  },
  manualHint: {
    fontSize: 14,
    color: "#92400e",
    background: "#fffbeb",
    border: "1px solid #fcd34d",
    borderRadius: 12,
    padding: 10,
    marginTop: 8,
  },
  manualHintOk: {
    fontSize: 14,
    color: "#166534",
    background: "#f0fdf4",
    border: "1px solid #86efac",
    borderRadius: 12,
    padding: 10,
    marginTop: 8,
  },
};