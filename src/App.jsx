import InfoCard from "./components/InfoCard.jsx";
import { formatRsd, formatNum } from "./utils/formatters.js";
import InputPanel from "./components/InputPanel.jsx";
import VehicleSummarySection from "./components/dashboard/VehicleSummarySection.jsx";
import MaintenancePlanSection from "./components/dashboard/MaintenancePlanSection.jsx";
import BusinessDashboard from "./components/dashboard/BusinessDashboard.jsx";
import VehicleHeader from "./components/dashboard/VehicleHeader.jsx";
import FleetKpiSection from "./components/dashboard/FleetKpiSection.jsx";
import CostStructureSection from "./components/dashboard/CostStructureSection.jsx";
import ScenarioComparisonSection from "./components/dashboard/ScenarioComparisonSection.jsx";
import FleetOptimizerSection from "./components/dashboard/FleetOptimizerSection.jsx";
import { useFleetCalculatorController } from "./hooks/useFleetCalculatorController.js";

export default function App() {
  const {
    sessionUser,
    viewMode,
    setViewMode,
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
    engineOverride,
    setEngineOverride,
    gearboxOverride,
    setGearboxOverride,
    drivetrainOverride,
    setDrivetrainOverride,
    decoded,
    exploitation,
    annualKm,
    resolvedVehicle,
    resolverMissingConfirmations,
    gate,
    vehicleGate,
    planningGate,
    missingFields,
    warnings,
    overallStatus,
    canBuildProvisionalPlan,
    maintenancePlan,
    finalEngine,
    finalGearbox,
    finalDrivetrain,
    maintenanceGateMessage,
    vehicleLabel,
    exploitationLabel,
    vehiclePresentation,
    showExpertSections,
    engineLabel,
    gearboxLabel,
    drivetrainLabel,
    businessStatusLabel,
    totalsDisplay,
    resolverSourceLabels,
    resolverStatusUi,
    vehicleGateUi,
    planningGateUi,
    overallStatusUi,
    planStatusUi,
    vehicleConfidence,
    pricingConfidence,
    quoteReadiness,
    quoteReadinessUi,
    explainabilityNotes,
    getFieldLabel,
    planTotalCost,
    planTotalService,
    planTotalBrakes,
    planTotalTires,
    planTotalEvents,
    planCostPerKm,
    planCostPerMonth,
    serviceEvents,
    brakeEvents,
    tireEvents,
    scenarioRows,
    scenarioComparisonData,
    EXPLOITATION_PROFILES,
  } = useFleetCalculatorController();


  return (
    <div style={styles.page}>
      <div style={styles.container}>
        <div style={styles.hero}>
          <div>
            <div style={styles.heroBadge}>Demo licensed environment</div>
            <h1 style={styles.heroTitle}>Fleet Maintenance &amp; TCO Calculator</h1>
            <div style={styles.heroSubtitle}>
              Škoda-only kalkulator za leasing, rent-a-car i fleet operacije.
            </div>
          </div>

          <div style={styles.heroMetaWrap}>
            <div style={styles.heroMetaCard}>
              <div style={styles.heroMetaLabel}>KORISNIK</div>
              <div style={styles.heroMetaValue}>{sessionUser.displayName}</div>
            </div>
            <div style={styles.heroMetaCard}>
              <div style={styles.heroMetaLabel}>KOMPANIJA</div>
              <div style={styles.heroMetaValue}>{sessionUser.company}</div>
            </div>
            <div style={styles.modeSwitcher}>
              <button
                type="button"
                onClick={() => setViewMode("business")}
                style={{
                  ...styles.modeBtn,
                  ...(viewMode === "business" ? styles.modeBtnActive : {}),
                }}
              >
                Business
              </button>
              <button
                type="button"
                onClick={() => setViewMode("expert")}
                style={{
                  ...styles.modeBtn,
                  ...(viewMode === "expert" ? styles.modeBtnActiveDark : {}),
                }}
              >
                Expert
              </button>
            </div>
          </div>
        </div>

        <div style={styles.mainGrid}>
          <InputPanel
            viewMode={viewMode}
            selectedProfileId={selectedProfileId}
            loadProfile={loadProfile}
            vin={vin}
            setVin={setVin}
            plannedKm={plannedKm}
            setPlannedKm={setPlannedKm}
            contractMonths={contractMonths}
            setContractMonths={setContractMonths}
            exploitationType={exploitationType}
            setExploitationType={setExploitationType}
            hourlyRate={hourlyRate}
            setHourlyRate={setHourlyRate}
            oilPricePerLiter={oilPricePerLiter}
            setOilPricePerLiter={setOilPricePerLiter}
            tireCategory={tireCategory}
            setTireCategory={setTireCategory}
            flexInterval={flexInterval}
            setFlexInterval={setFlexInterval}
            laborDiscount={laborDiscount}
            setLaborDiscount={setLaborDiscount}
            partsDiscount={partsDiscount}
            setPartsDiscount={setPartsDiscount}
            oilDiscount={oilDiscount}
            setOilDiscount={setOilDiscount}
            exploitation={exploitation}
            decoded={decoded}
            annualKm={annualKm}
            formatRsd={formatRsd}
            formatNum={formatNum}
            EXPLOITATION_PROFILES={EXPLOITATION_PROFILES}
            engineOverride={engineOverride}
            setEngineOverride={setEngineOverride}
            gearboxOverride={gearboxOverride}
            setGearboxOverride={setGearboxOverride}
            drivetrainOverride={drivetrainOverride}
            setDrivetrainOverride={setDrivetrainOverride}
            resolverMissingConfirmations={resolverMissingConfirmations}
          />

          <div>
            <div style={styles.contentCard}>
              <div style={styles.contentHeader}>
                <div>
                  <h2 style={styles.sectionTitle}>
                    {viewMode === "business" ? "Sažetak vozila i ugovora" : "Vozilo i tehnički profil"}
                  </h2>
                  <div style={styles.muted}>
                    {viewMode === "business"
                      ? "Jasan pregled vozila, ugovora i statusa kalkulacije."
                      : "Dekodirani podaci, engine/fluid logika, menjač i aktivna varijanta."}
                  </div>
                </div>

                <div style={styles.headerBadgeWrap}>
                  <span style={styles.darkBadge}>
                    {viewMode === "business"
                      ? `Status: ${businessStatusLabel}`
                      : `VIN ${decoded.supported ? "prepoznat" : "nije podržan"} (${decoded.confidence || "-"})`}
                  </span>
                  <span style={styles.lightBadge}>{sessionUser.company}</span>
                </div>
              </div>

              {!decoded.supported ? (
                <div style={{ color: "#b91c1c", fontWeight: 700, marginTop: 12 }}>{decoded.reason}</div>
              ) : (
                <>
                  {viewMode === "business" ? (
                    <>
                      <VehicleHeader
                        image={vehiclePresentation.image}
                        title={vehiclePresentation.label}
                        engine={engineLabel}
                        gearbox={gearboxLabel}
                        drivetrain={drivetrainLabel}
                        powerKw={decoded?.powerKw || decoded?.enginePowerKw || decoded?.kw || null}
                      />

                      <VehicleSummarySection
                        vehicleLabel={vehicleLabel}
                        engineLabel={engineLabel}
                        gearboxLabel={gearboxLabel}
                        drivetrainLabel={drivetrainLabel}
                        plannedKm={plannedKm}
                        contractMonths={contractMonths}
                        exploitationLabel={exploitationLabel}
                        tireCategory={tireCategory}
                        hourlyRate={hourlyRate}
                        oilPricePerLiter={oilPricePerLiter}
                      />

                      <div style={styles.contentCardInner}>
                        <div style={styles.candidateTitle}>Confidence &amp; guardrails</div>
                        <div style={styles.resolverGrid}>
                          <div style={styles.gateCard}>
                            <div style={styles.gateLabel}>Quote readiness</div>
                            <div style={{ ...styles.gateBadge, ...quoteReadinessUi.style }}>
                              {quoteReadinessUi.label}
                            </div>
                          </div>

                          <div style={styles.gateCard}>
                            <div style={styles.gateLabel}>Vehicle confidence</div>
                            <div style={styles.resolverValue}>{String(vehicleConfidence?.level || '-').toUpperCase()}</div>
                          </div>

                          <div style={styles.gateCard}>
                            <div style={styles.gateLabel}>Pricing confidence</div>
                            <div style={styles.resolverValue}>{String(pricingConfidence?.level || '-').toUpperCase()}</div>
                          </div>

                          <div style={styles.gateCard}>
                            <div style={styles.gateLabel}>Pricing coverage</div>
                            <div style={styles.resolverValue}>
                              {maintenancePlan?.pricingMeta?.pricingCoveragePercent != null
                                ? `${formatNum(maintenancePlan.pricingMeta.pricingCoveragePercent, 0)}%`
                                : '-'}
                            </div>
                          </div>

                          <div style={styles.gateCard}>
                            <div style={styles.gateLabel}>Raspon troška</div>
                            <div style={styles.resolverValue}>
                              {maintenancePlan?.pricingMeta?.pricingRange
                                ? `${formatRsd(maintenancePlan.pricingMeta.pricingRange.min)} – ${formatRsd(maintenancePlan.pricingMeta.pricingRange.max)}`
                                : '-'}
                            </div>
                          </div>
                        </div>

                        {explainabilityNotes?.length > 0 ? (
                          <div style={styles.alertInfo}>
                            <div style={styles.alertTitle}>Guardrail upozorenja i pretpostavke</div>
                            <div style={styles.warningList}>
                              {explainabilityNotes.slice(0, 6).map((warning, index) => (
                                <div key={`${warning}-${index}`} style={styles.warningItem}>
                                  {warning}
                                </div>
                              ))}
                            </div>
                          </div>
                        ) : null}
                      </div>

                      {resolverMissingConfirmations.length > 0 ? (
                        <div style={styles.alertWarning}>
                          <div style={styles.alertTitle}>Potrebna ručna dopuna</div>
                          <div style={styles.warningList}>
                            <div style={styles.warningItem}>
                              Da bi plan održavanja bio potpuno validan, dopuni sledeća polja u levom panelu.
                            </div>
                          </div>
                          <div style={styles.tagWrap}>
                            {resolverMissingConfirmations.map((field) => (
                              <span key={field} style={styles.warningTag}>
                                {getFieldLabel(field)}
                              </span>
                            ))}
                          </div>
                        </div>
                      ) : null}

                      {maintenanceGateMessage ? (
                        <div style={styles.blockedBox}>
                          <div style={styles.alertTitle}>Plan trenutno nije otključan</div>
                          <div>{maintenanceGateMessage}</div>
                        </div>
                      ) : null}
                    </>
                  ) : (
                    <>
                      <div style={styles.infoGrid}>
                        <InfoCard label="Marka / model" value={`${decoded.marka} ${decoded.model}`} />
                        <InfoCard label="Motor" value={`${finalEngine} / ${decoded.motor}`} />
                        <InfoCard label="Menjač / godina" value={`${finalGearbox} / ${decoded.modelYear}`} />
                        <InfoCard label="Kategorija satnice" value={formatRsd(decoded.hourlyRate || hourlyRate)} />
                        <InfoCard label="Cena ulja / L" value={formatRsd(oilPricePerLiter)} />
                        <InfoCard label="Klasa guma" value={tireCategory} />
                        <InfoCard label="DPF / SCR" value={decoded.fuelType === "Diesel" ? "DPF / SCR" : "GPF / -"} />
                        <InfoCard
                          label="Svećice / filter goriva"
                          value={decoded.fuelType === "Diesel" ? "0 / da" : "4 / ne"}
                        />
                        <InfoCard label="Količina motornog ulja" value={`${decoded.oilCapacity} L`} />
                        <InfoCard label="VW norma ulja" value={decoded.oilSpec} />
                        <InfoCard label="SAE" value={decoded.oilSae} />
                        <InfoCard
                          label="Varijanta"
                          value={`${decoded.motor} / ${finalGearbox} / ${finalDrivetrain}`}
                        />
                        <InfoCard label="Tip menjača" value={finalGearbox} />
                        <InfoCard
                          label="Kod menjača"
                          value={resolvedVehicle?.fields?.gearbox?.value || decoded.gearboxCode || "-"}
                        />
                        <InfoCard label="Ulje menjača" value={String(finalGearbox).includes("DSG") ? "6 L" : "N/A"} />
                      </div>

                      <div style={styles.resolverSection}>
                        <div style={styles.gateTitle}>Finalna vehicle konfiguracija</div>

                        <div style={styles.resolverGrid}>
                          <div style={styles.gateCard}>
                            <div style={styles.gateLabel}>Engine source</div>
                            <div style={styles.resolverValue}>
                              {resolverSourceLabels.engine}
                            </div>
                          </div>

                          <div style={styles.gateCard}>
                            <div style={styles.gateLabel}>Gearbox source</div>
                            <div style={styles.resolverValue}>
                              {resolverSourceLabels.gearbox}
                            </div>
                          </div>

                          <div style={styles.gateCard}>
                            <div style={styles.gateLabel}>Drivetrain source</div>
                            <div style={styles.resolverValue}>
                              {resolverSourceLabels.drivetrain}
                            </div>
                          </div>

                          <div style={styles.gateCard}>
                            <div style={styles.gateLabel}>Resolver status</div>
                            <div
                              style={{
                                ...styles.gateBadge,
                                ...resolverStatusUi.style,
                              }}
                            >
                              {resolverStatusUi.label}
                            </div>
                          </div>
                        </div>

                        {resolverMissingConfirmations.length > 0 ? (
                          <div style={styles.alertWarning}>
                            <div style={styles.alertTitle}>Polja za ručnu potvrdu</div>
                            <div style={styles.tagWrap}>
                              {resolverMissingConfirmations.map((field) => (
                                <span key={field} style={styles.warningTag}>
                                  {getFieldLabel(field)}
                                </span>
                              ))}
                            </div>
                          </div>
                        ) : null}
                      </div>

                      {gate ? (
                        <div style={styles.gateSection}>
                          <div style={styles.gateTitle}>QA validacija za maintenance plan</div>

                          <div style={styles.gateGrid}>
                            <div style={styles.gateCard}>
                              <div style={styles.gateLabel}>Vehicle gate</div>
                              <div
                                style={{
                                  ...styles.gateBadge,
                                  ...vehicleGateUi.style,
                                }}
                              >
                                {vehicleGateUi.label}
                              </div>
                            </div>

                            <div style={styles.gateCard}>
                              <div style={styles.gateLabel}>Planning gate</div>
                              <div
                                style={{
                                  ...styles.gateBadge,
                                  ...planningGateUi.style,
                                }}
                              >
                                {planningGateUi.label}
                              </div>
                            </div>

                            <div style={styles.gateCard}>
                              <div style={styles.gateLabel}>Overall</div>
                              <div
                                style={{
                                  ...styles.gateBadge,
                                  ...overallStatusUi.style,
                                }}
                              >
                                {overallStatusUi.label}
                              </div>
                            </div>

                            <div style={styles.gateCard}>
                              <div style={styles.gateLabel}>Plan status</div>
                              <div
                                style={{
                                  ...styles.gateBadge,
                                  ...planStatusUi.style,
                                }}
                              >
                                {planStatusUi.label}
                              </div>
                            </div>
                          </div>

                          {missingFields.length > 0 ? (
                            <div style={styles.alertWarning}>
                              <div style={styles.alertTitle}>Nedostajuća polja za tačan plan</div>
                              <div style={styles.tagWrap}>
                                {missingFields.map((field) => (
                                  <span key={field} style={styles.warningTag}>
                                    {getFieldLabel(field)}
                                  </span>
                                ))}
                              </div>
                            </div>
                          ) : null}

                          {warnings.length > 0 ? (
                            <div style={styles.alertInfo}>
                              <div style={styles.alertTitle}>Upozorenja</div>
                              <div style={styles.warningList}>
                                {warnings.map((warning, index) => (
                                  <div key={`${warning}-${index}`} style={styles.warningItem}>
                                    {warning}
                                  </div>
                                ))}
                              </div>
                            </div>
                          ) : null}
                        </div>
                      ) : null}

                      {decoded.candidates && decoded.candidates.length > 0 ? (
                        <div style={styles.contentCardInner}>
                          <div style={styles.candidateTitle}>Moguće varijante vozila</div>
                          <div style={styles.candidateList}>
                            {decoded.candidates.map((candidate) => (
                              <div key={candidate} style={styles.candidateItem}>
                                {candidate}
                              </div>
                            ))}
                          </div>
                        </div>
                      ) : null}
                    </>
                  )}
                </>
              )}
            </div>

            <BusinessDashboard
              totalCost={planTotalCost}
              serviceCost={planTotalService}
              brakeCost={planTotalBrakes}
              tireCost={planTotalTires}
              costPerKm={planCostPerKm}
              costPerMonth={planCostPerMonth}
              eventCount={planTotalEvents}
            />

            <FleetKpiSection
              totalCost={planTotalCost}
              contractMonths={contractMonths}
              plannedKm={plannedKm}
              serviceEvents={serviceEvents}
              brakeEvents={brakeEvents}
              tireEvents={tireEvents}
            />
            <CostStructureSection
              totalCost={planTotalCost}
              serviceCost={planTotalService}
              brakeCost={planTotalBrakes}
              tireCost={planTotalTires}
            />
            <ScenarioComparisonSection
              scenarios={scenarioComparisonData}
              formatRsd={formatRsd}
              formatNum={formatNum}
            />
            <FleetOptimizerSection
              scenarios={scenarioComparisonData}
              formatRsd={formatRsd}
              formatNum={formatNum}
            />
            <MaintenancePlanSection
              serviceEvents={serviceEvents}
              brakeEvents={brakeEvents}
              tireEvents={tireEvents}
              totalServiceCost={planTotalService}
              totalBrakeCost={planTotalBrakes}
              totalTireCost={planTotalTires}
              totalCost={planTotalCost}
              totalEvents={planTotalEvents}
              showExpertSections={showExpertSections}
              formatNum={formatNum}
              formatRsd={formatRsd}
            />

            <div style={styles.contentCard}>
              <h2 style={styles.sectionTitle}>TCO scenario analysis</h2>

              {!canBuildProvisionalPlan ? (
                <div style={styles.blockedBox}>
                  Plan je blokiran dok se ne dopune obavezni tehnički podaci o vozilu.
                </div>
              ) : (
                <div style={styles.tableWrap}>
                  <table style={styles.table}>
                    <thead>
                      <tr>
                        <th style={styles.th}>Scenario</th>
                        <th style={styles.th}>Profil eksploatacije</th>
                        <th style={styles.thRight}>Flex interval</th>
                        <th style={styles.thRight}>Događaji</th>
                        <th style={styles.thRight}>Ukupno</th>
                      </tr>
                    </thead>
                    <tbody>
                      {scenarioRows.map((row) => (
                        <tr key={row.label}>
                          <td style={styles.td}>{row.label}</td>
                          <td style={styles.td}>{row.usageLabel || "-"}</td>
                          <td style={styles.tdRight}>{formatNum(row.flexInterval, 0)} km</td>
                          <td style={styles.tdRight}>{formatNum(row.eventCount, 0)}</td>
                          <td style={styles.tdRight}>{formatRsd(row.totalCost)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

const styles = {
  page: {
    minHeight: "100vh",
    background: "linear-gradient(to bottom, #f1f5f9, #ffffff, #f1f5f9)",
    padding: 24,
    color: "#0f172a",
    fontFamily: "Arial, sans-serif",
  },
  container: {
    maxWidth: 1500,
    margin: "0 auto",
  },
  hero: {
    background: "#020617",
    color: "#fff",
    borderRadius: 28,
    padding: 28,
    display: "flex",
    justifyContent: "space-between",
    gap: 20,
    alignItems: "center",
    marginBottom: 24,
    boxShadow: "0 4px 20px rgba(0,0,0,0.12)",
  },
  heroBadge: {
    fontSize: 14,
    color: "#cbd5e1",
    marginBottom: 12,
  },
  heroTitle: {
    fontSize: 48,
    margin: 0,
    lineHeight: 1.05,
  },
  heroSubtitle: {
    marginTop: 12,
    color: "#cbd5e1",
    fontSize: 18,
  },
  heroMetaWrap: {
    display: "grid",
    gridTemplateColumns: "repeat(3, minmax(160px, 1fr))",
    gap: 12,
    minWidth: 520,
    alignItems: "stretch",
  },
  heroMetaCard: {
    border: "1px solid rgba(255,255,255,0.12)",
    background: "rgba(255,255,255,0.04)",
    padding: 18,
    borderRadius: 20,
  },
  heroMetaLabel: {
    fontSize: 12,
    color: "#94a3b8",
    marginBottom: 8,
  },
  heroMetaValue: {
    fontSize: 18,
    fontWeight: 700,
  },
  modeSwitcher: {
    display: "grid",
    gridTemplateColumns: "1fr 1fr",
    gap: 8,
    background: "rgba(255,255,255,0.04)",
    border: "1px solid rgba(255,255,255,0.12)",
    borderRadius: 20,
    padding: 8,
    minWidth: 220,
  },
  modeBtn: {
    border: "1px solid rgba(255,255,255,0.18)",
    background: "transparent",
    color: "#cbd5e1",
    borderRadius: 14,
    padding: "14px 16px",
    fontSize: 15,
    fontWeight: 700,
    cursor: "pointer",
  },
  modeBtnActive: {
    background: "#fff",
    color: "#0f172a",
    border: "1px solid #fff",
  },
  modeBtnActiveDark: {
    background: "#16a34a",
    color: "#fff",
    border: "1px solid #16a34a",
  },
  mainGrid: {
    display: "grid",
    gridTemplateColumns: "460px 1fr",
    gap: 24,
    alignItems: "start",
  },
  contentCard: {
    background: "#fff",
    borderRadius: 28,
    padding: 28,
    boxShadow: "0 1px 10px rgba(0,0,0,0.08)",
    marginBottom: 20,
  },
  sectionTitle: {
    fontSize: 22,
    margin: "0 0 16px",
  },
  subSectionTitle: {
    fontSize: 18,
    fontWeight: 700,
    margin: "24px 0 12px",
    color: "#0f172a",
  },
  muted: {
    color: "#64748b",
    fontSize: 16,
  },
  mutedEmpty: {
    color: "#64748b",
    fontSize: 15,
    padding: "10px 0 4px",
  },
  contentHeader: {
    display: "flex",
    justifyContent: "space-between",
    gap: 12,
    alignItems: "start",
    marginBottom: 18,
  },
  headerBadgeWrap: {
    display: "flex",
    gap: 10,
    alignItems: "center",
  },
  darkBadge: {
    background: "#0f172a",
    color: "#fff",
    padding: "8px 12px",
    borderRadius: 999,
    fontWeight: 700,
    fontSize: 14,
  },
  lightBadge: {
    border: "1px solid #cbd5e1",
    padding: "8px 12px",
    borderRadius: 12,
    fontSize: 14,
    color: "#334155",
    background: "#fff",
  },
  infoGrid: {
    display: "grid",
    gridTemplateColumns: "repeat(4, minmax(180px, 1fr))",
    gap: 14,
  },
  businessSummaryGrid: {
    display: "grid",
    gridTemplateColumns: "2fr repeat(3, 1fr)",
    gap: 14,
    alignItems: "stretch",
  },
  businessHeroCard: {
    border: "1px solid #dbeafe",
    background: "linear-gradient(to bottom right, #eff6ff, #ffffff)",
    borderRadius: 22,
    padding: 22,
    minHeight: 120,
    display: "flex",
    flexDirection: "column",
    justifyContent: "center",
  },
  businessVehicleName: {
    fontSize: 28,
    fontWeight: 800,
    color: "#0f172a",
    marginBottom: 8,
  },
  businessVehicleMeta: {
    fontSize: 16,
    color: "#475569",
    lineHeight: 1.4,
  },
  businessMiniCard: {
    border: "1px solid #e2e8f0",
    borderRadius: 18,
    padding: 18,
    background: "#fff",
    display: "flex",
    flexDirection: "column",
    justifyContent: "center",
    minHeight: 120,
  },
  businessMiniLabel: {
    fontSize: 13,
    color: "#64748b",
    marginBottom: 10,
  },
  businessMiniValue: {
    fontSize: 18,
    fontWeight: 700,
    color: "#0f172a",
    lineHeight: 1.3,
  },
  statsGrid: {
    display: "grid",
    gridTemplateColumns: "repeat(7, 1fr)",
    gap: 14,
    marginBottom: 20,
  },
  planSummaryGrid: {
    display: "grid",
    gridTemplateColumns: "repeat(5, minmax(160px, 1fr))",
    gap: 12,
    marginBottom: 18,
  },
  tableWrap: {
    overflowX: "auto",
  },
  table: {
    width: "100%",
    borderCollapse: "collapse",
  },
  th: {
    textAlign: "left",
    padding: "12px 10px",
    borderBottom: "1px solid #e2e8f0",
    color: "#64748b",
    fontSize: 14,
    verticalAlign: "top",
  },
  thRight: {
    textAlign: "right",
    padding: "12px 10px",
    borderBottom: "1px solid #e2e8f0",
    color: "#64748b",
    fontSize: 14,
    verticalAlign: "top",
  },
  td: {
    padding: "14px 10px",
    borderBottom: "1px solid #f1f5f9",
    fontSize: 16,
    verticalAlign: "top",
  },
  tdRight: {
    padding: "14px 10px",
    borderBottom: "1px solid #f1f5f9",
    textAlign: "right",
    fontSize: 16,
    verticalAlign: "top",
  },
  contentCardInner: {
    marginTop: 18,
    paddingTop: 18,
    borderTop: "1px solid #e2e8f0",
  },
  candidateTitle: {
    fontSize: 18,
    fontWeight: 700,
    marginBottom: 12,
  },
  candidateList: {
    display: "grid",
    gap: 10,
  },
  candidateItem: {
    border: "1px solid #e2e8f0",
    borderRadius: 14,
    padding: 14,
    background: "#f8fafc",
    fontSize: 16,
  },
  gateSection: {
    marginTop: 18,
    paddingTop: 18,
    borderTop: "1px solid #e2e8f0",
    display: "grid",
    gap: 14,
  },
  resolverSection: {
    marginTop: 18,
    paddingTop: 18,
    borderTop: "1px solid #e2e8f0",
    display: "grid",
    gap: 14,
  },
  gateTitle: {
    fontSize: 18,
    fontWeight: 700,
  },
  gateGrid: {
    display: "grid",
    gridTemplateColumns: "repeat(4, minmax(160px, 1fr))",
    gap: 12,
  },
  resolverGrid: {
    display: "grid",
    gridTemplateColumns: "repeat(4, minmax(160px, 1fr))",
    gap: 12,
  },
  gateCard: {
    border: "1px solid #e2e8f0",
    borderRadius: 16,
    padding: 14,
    background: "#fff",
  },
  gateLabel: {
    fontSize: 14,
    color: "#64748b",
    marginBottom: 10,
  },
  gateBadge: {
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    minHeight: 38,
    padding: "8px 12px",
    borderRadius: 999,
    fontSize: 13,
    fontWeight: 800,
  },
  resolverValue: {
    fontSize: 15,
    fontWeight: 700,
    color: "#0f172a",
  },
  alertWarning: {
    border: "1px solid #fcd34d",
    background: "#fffbeb",
    borderRadius: 18,
    padding: 16,
  },
  alertInfo: {
    border: "1px solid #cbd5e1",
    background: "#f8fafc",
    borderRadius: 18,
    padding: 16,
  },
  alertTitle: {
    fontSize: 16,
    fontWeight: 700,
    marginBottom: 10,
  },
  tagWrap: {
    display: "flex",
    gap: 10,
    flexWrap: "wrap",
  },
  warningTag: {
    padding: "8px 12px",
    borderRadius: 999,
    background: "#fef3c7",
    color: "#92400e",
    border: "1px solid #fcd34d",
    fontSize: 14,
    fontWeight: 700,
  },
  warningList: {
    display: "grid",
    gap: 8,
  },
  warningItem: {
    fontSize: 15,
    color: "#334155",
  },
  blockedBox: {
    border: "1px solid #fca5a5",
    background: "#fef2f2",
    color: "#991b1b",
    borderRadius: 18,
    padding: 18,
    fontSize: 16,
    fontWeight: 700,
  },
  breakdownList: {
    display: "grid",
    gap: 8,
    minWidth: 280,
  },
  breakdownRow: {
    display: "grid",
    gap: 2,
    padding: "8px 10px",
    borderRadius: 12,
    background: "#f8fafc",
    border: "1px solid #e2e8f0",
  },
  breakdownName: {
    fontSize: 13,
    fontWeight: 700,
    color: "#0f172a",
  },
  breakdownMeta: {
    fontSize: 12,
    color: "#64748b",
  },
  breakdownTotal: {
    fontSize: 13,
    fontWeight: 700,
    color: "#334155",
  },
};