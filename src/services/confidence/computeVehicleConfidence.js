function getFieldConfidenceScore(field = {}, weight = 1) {
  const confidence = String(field?.confidence || '').toLowerCase();
  const resolved = Boolean(field?.resolved);
  if (!resolved) return 0;
  if (['exact', 'confirmed', 'high'].includes(confidence)) return 1 * weight;
  if (confidence === 'medium') return 0.7 * weight;
  if (confidence === 'low') return 0.4 * weight;
  return 0.2 * weight;
}

function unique(values = []) {
  return [...new Set(values.filter(Boolean))];
}

function hasManualOverrides(resolvedVehicle = null) {
  return Boolean(resolvedVehicle?.vehicle?.source?.manualOverrideUsed);
}

export function computeVehicleConfidence({ resolvedVehicle = null, decoded = null } = {}) {
  if (!resolvedVehicle?.supported) {
    return {
      level: 'low',
      warnings: ['VIN nije podržan; vozilo nije spremno za pouzdanu kalkulaciju.'],
      blockers: ['unsupported_vehicle'],
      metrics: {
        weightedScore: 0,
        requiredFieldCount: 3,
        manualOverrideUsed: false,
        internalStatus: 'invalid',
        operationalReadiness: 'blocked',
      },
    };
  }

  const fields = resolvedVehicle?.fields || {};
  const internalStatus = resolvedVehicle?.internalStatus || 'needs_manual_input';
  const operationalReadiness = resolvedVehicle?.operationalReadiness || 'manual_review_required';
  const weights = { engine: 0.5, gearbox: 0.3, drivetrain: 0.2 };
  const weightedScore =
    getFieldConfidenceScore(fields.engine, weights.engine) +
    getFieldConfidenceScore(fields.gearbox, weights.gearbox) +
    getFieldConfidenceScore(fields.drivetrain, weights.drivetrain);

  const resolvedRequiredFields = [fields.engine, fields.gearbox, fields.drivetrain].filter((field) => field?.resolved).length;
  const manualOverrideUsed = hasManualOverrides(resolvedVehicle);
  const warnings = [];
  const blockers = [];
  const gearboxCandidateCount = (fields.gearbox?.candidates || []).length;
  const engineCandidateCount = (fields.engine?.candidates || []).length;
  const drivetrainSource = String(fields.drivetrain?.source || '').toLowerCase();

  if (operationalReadiness === 'blocked') {
    blockers.push('vehicle_blocked');
    warnings.push('Tehnički profil vozila je blokiran i zahteva ručnu obradu.');
  }

  if (resolvedRequiredFields < 3) {
    blockers.push('missing_required_vehicle_fields');
    warnings.push('Nedostaju obavezna tehnička polja za potpuno pouzdan rezultat.');
  }

  if (manualOverrideUsed) {
    warnings.push('Rezultat koristi ručnu dopunu tehničkih podataka.');
  }

  if (internalStatus === 'ready_high_confidence_inferred') {
    warnings.push('Vozilo je estimate-ready na osnovu dominantne inference i traži potvrdu pre finalne ponude.');
  }

  if (internalStatus === 'partial_inferred') {
    warnings.push('Inference postoji, ali nema dovoljno zatvoren tehnički profil za sigurnu auto-potvrdu.');
  }

  if (internalStatus === 'needs_manual_input') {
    warnings.push('Potrebna je ručna potvrda tehničkog profila vozila.');
  }

  if (gearboxCandidateCount > 1 && String(fields.gearbox?.confidence || '').toLowerCase() !== 'confirmed') {
    warnings.push('Gearbox ostaje delimično ambiguozan; preporučena je dodatna provera.');
  }

  if (gearboxCandidateCount > 2) {
    warnings.push('Gearbox je izabran iz širokog kandidatskog klastera.');
  }

  if (engineCandidateCount > 2 && String(fields.engine?.confidence || '').toLowerCase() !== 'confirmed') {
    warnings.push('Engine i dalje ima više kandidata nego što je poželjno za visoko poverenje.');
  }

  if ((fields.drivetrain?.confidence || '').toLowerCase() === 'medium') {
    warnings.push('Drivetrain je izveden bez pune potvrde exact VIN putem.');
  }

  if (drivetrainSource.includes('partial_consensus')) {
    warnings.push('Drivetrain je izveden iz delimičnog gearbox konsenzusa i traži proveru.');
  }

  if (resolvedVehicle?.warnings?.length) {
    warnings.push(...resolvedVehicle.warnings.map((warning) => `Resolver warning: ${warning}`));
  }

  let level = 'low';
  if (internalStatus === 'ready_exact' && weightedScore >= 0.95 && blockers.length === 0) {
    level = 'high';
  } else if (['ready_high_confidence_inferred', 'ready_exact'].includes(internalStatus) && weightedScore >= 0.75 && blockers.length === 0) {
    level = internalStatus === 'ready_exact' ? 'high' : 'medium';
  } else if (weightedScore >= 0.55 && blockers.length === 0) {
    level = 'medium';
  }

  return {
    level,
    warnings: unique(warnings),
    blockers: unique(blockers),
    metrics: {
      weightedScore: Number(weightedScore.toFixed(2)),
      resolvedRequiredFields,
      requiredFieldCount: 3,
      manualOverrideUsed,
      decoderConfidence: decoded?.confidence || null,
      internalStatus,
      operationalReadiness,
      missingConfirmationsCount: (resolvedVehicle?.missingConfirmations || []).length,
    },
  };
}

export default computeVehicleConfidence;
