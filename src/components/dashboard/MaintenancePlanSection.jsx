function sumEstimatedCost(events = []) {
  return events.reduce((sum, event) => sum + Number(event?.estimatedCost || 0), 0);
}

function renderDueKm(event, formatNum) {
  return event?.due?.km ? `${formatNum(event.due.km, 0)} km` : "-";
}

function renderDueMonth(event) {
  return event?.due?.month || "-";
}

function renderCategoryLabel(category) {
  const map = {
    regular_service: "Servis",
    time_based: "Vremenski",
    powertrain: "Pogon",
    drivetrain: "Drivetrain",
    brakes: "Kočnice",
    tires: "Gume",
  };

  return map[category] || category || "-";
}

function renderRuleLabel(rule) {
  const map = {
    flex_oil_service: "Flex servis",
    spark_plugs_60k_48m: "Svećice 60k / 48m",
    brake_fluid_24m: "Kočiona tečnost 24m",
    dsg_service_120k: "DSG 120k",
    haldex_service_3y: "Haldex 3 godine",
    front_brakes_usage_based: "Prednje kočnice",
    rear_brakes_usage_based: "Zadnje kočnice",
    summer_tires_usage_based: "Letnje gume",
    winter_tires_usage_based: "Zimske gume",
    seasonal_tire_change: "Sezonska zamena",
  };

  return map[rule] || rule || "-";
}

function SectionSummaryCard({ label, value }) {
  return (
    <div style={styles.summaryCard}>
      <div style={styles.summaryLabel}>{label}</div>
      <div style={styles.summaryValue}>{value}</div>
    </div>
  );
}

function BreakdownCell({ event, showExpertSections, formatNum, formatRsd }) {
  if (!showExpertSections) return null;

  const breakdown = event?.pricingBreakdown || [];

  if (!breakdown.length) {
    return <td style={styles.td}>-</td>;
  }

  return (
    <td style={styles.td}>
      <div style={styles.breakdownList}>
        {breakdown.map((line) => (
          <div key={`${event.id}-${line.id}-${line.name}`} style={styles.breakdownRow}>
            <div style={styles.breakdownName}>{line.name}</div>
            <div style={styles.breakdownMeta}>
              {formatNum(line.qty, 2)} {line.unit} × {formatRsd(line.unitPrice)}
            </div>
            <div style={styles.breakdownTotal}>{formatRsd(line.total)}</div>
          </div>
        ))}
      </div>
    </td>
  );
}

function EventsTable({
  title,
  events,
  emptyMessage,
  formatNum,
  formatRsd,
  showExpertSections,
  simple = false,
}) {
  return (
    <div style={styles.sectionBlock}>
      <h3 style={styles.subSectionTitle}>{title}</h3>

      {events.length > 0 ? (
        <div style={styles.tableWrap}>
          <table style={styles.table}>
            <thead>
              <tr>
                <th style={styles.th}>Događaj</th>
                <th style={styles.thRight}>Km</th>
                <th style={styles.thRight}>Mesec</th>
                {!simple ? <th style={styles.th}>Kategorija</th> : null}
                {!simple && showExpertSections ? <th style={styles.th}>Cenovna razrada</th> : null}
                {showExpertSections ? <th style={styles.th}>Pravilo</th> : null}
                <th style={styles.thRight}>Procena</th>
              </tr>
            </thead>

            <tbody>
              {events.map((event) => (
                <tr key={event.id}>
                  <td style={styles.td}>
                    <div style={styles.eventTitle}>{event.title}</div>
                    {showExpertSections && event?.notes?.length > 0 ? (
                      <div style={styles.eventNotes}>
                        {event.notes.map((note, idx) => (
                          <div key={`${event.id}-note-${idx}`}>{note}</div>
                        ))}
                      </div>
                    ) : null}
                  </td>

                  <td style={styles.tdRight}>{renderDueKm(event, formatNum)}</td>
                  <td style={styles.tdRight}>{renderDueMonth(event)}</td>

                  {!simple ? (
                    <td style={styles.td}>{renderCategoryLabel(event.category)}</td>
                  ) : null}

                  {!simple ? (
                    <BreakdownCell
                      event={event}
                      showExpertSections={showExpertSections}
                      formatNum={formatNum}
                      formatRsd={formatRsd}
                    />
                  ) : null}

                  {showExpertSections ? (
                    <td style={styles.td}>{renderRuleLabel(event?.source?.rule)}</td>
                  ) : null}

                  <td style={styles.tdRight}>{formatRsd(event?.estimatedCost || 0)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div style={styles.emptyState}>{emptyMessage}</div>
      )}
    </div>
  );
}

export default function MaintenancePlanSection({
  serviceEvents = [],
  brakeEvents = [],
  tireEvents = [],
  totalServiceCost = 0,
  totalBrakeCost = 0,
  totalTireCost = 0,
  totalCost = 0,
  totalEvents = 0,
  showExpertSections = false,
  formatNum,
  formatRsd,
}) {
  const serviceSubtotal = sumEstimatedCost(serviceEvents);
  const brakeSubtotal = sumEstimatedCost(brakeEvents);
  const tireSubtotal = sumEstimatedCost(tireEvents);

  return (
    <div style={styles.card}>
      <h2 style={styles.sectionTitle}>Maintenance plan</h2>

      <div style={styles.summaryGrid}>
        <SectionSummaryCard label="Ukupni događaji" value={String(totalEvents)} />
        <SectionSummaryCard label="Servisi" value={formatRsd(totalServiceCost || serviceSubtotal)} />
        <SectionSummaryCard label="Kočnice" value={formatRsd(totalBrakeCost || brakeSubtotal)} />
        <SectionSummaryCard label="Gume" value={formatRsd(totalTireCost || tireSubtotal)} />
        <SectionSummaryCard label="Ukupno" value={formatRsd(totalCost)} />
      </div>

      <EventsTable
        title="Service events"
        events={serviceEvents}
        emptyMessage="Nema planiranih servisnih događaja."
        formatNum={formatNum}
        formatRsd={formatRsd}
        showExpertSections={showExpertSections}
      />

      <EventsTable
        title="Brake replacements"
        events={brakeEvents}
        emptyMessage="Nema planiranih kočionih intervencija."
        formatNum={formatNum}
        formatRsd={formatRsd}
        showExpertSections={showExpertSections}
        simple
      />

      <EventsTable
        title="Tire events"
        events={tireEvents}
        emptyMessage="Nema planiranih događaja za gume."
        formatNum={formatNum}
        formatRsd={formatRsd}
        showExpertSections={showExpertSections}
        simple
      />
    </div>
  );
}

const styles = {
  card: {
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
  summaryGrid: {
    display: "grid",
    gridTemplateColumns: "repeat(5, minmax(160px, 1fr))",
    gap: 12,
    marginBottom: 18,
  },
  summaryCard: {
    border: "1px solid #e2e8f0",
    borderRadius: 16,
    padding: 14,
    background: "#fff",
  },
  summaryLabel: {
    fontSize: 14,
    color: "#64748b",
    marginBottom: 10,
  },
  summaryValue: {
    fontSize: 15,
    fontWeight: 700,
    color: "#0f172a",
  },
  sectionBlock: {
    marginTop: 24,
  },
  subSectionTitle: {
    fontSize: 18,
    fontWeight: 700,
    margin: "0 0 12px",
    color: "#0f172a",
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
  eventTitle: {
    fontWeight: 700,
    color: "#0f172a",
  },
  eventNotes: {
    marginTop: 6,
    fontSize: 12,
    color: "#64748b",
    display: "grid",
    gap: 4,
  },
  emptyState: {
    color: "#64748b",
    fontSize: 15,
    padding: "10px 0 4px",
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