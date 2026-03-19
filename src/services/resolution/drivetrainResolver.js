import gearboxCodesMaster from '../../data/gearbox_codes_master.json' with { type: 'json' };

function unique(values = []) {
  return [...new Set(values.filter(Boolean))];
}

function normalizeDrivetrain(value) {
  const normalized = typeof value === 'string' ? value.trim().toUpperCase() : '';
  if (['AWD', '4X4'].includes(normalized)) return 'AWD';
  if (['FWD'].includes(normalized)) return 'FWD';
  if (['2WD'].includes(normalized)) return '2WD';
  return null;
}

function candidateConsensus(codes = []) {
  const normalizedCodes = unique(codes.map((value) => String(value || '').trim().toUpperCase()).filter(Boolean));
  if (normalizedCodes.length === 0) return null;

  const mapped = normalizedCodes.map((code) => ({ code, drivetrain: normalizeDrivetrain(gearboxCodesMaster?.[code]?.drivetrain) }));
  const known = mapped.filter((item) => item.drivetrain && item.drivetrain !== '2WD');
  if (known.length === 0) return null;

  const drives = unique(known.map((item) => item.drivetrain));
  if (drives.length !== 1) return null;

  const allKnown = known.length === normalizedCodes.length;
  return {
    value: drives[0],
    source: allKnown ? 'gearbox_candidates_consensus' : 'gearbox_candidates_partial_consensus',
    confidence: allKnown ? 'high' : 'medium',
    exact: allKnown,
    reason: null,
  };
}

export function resolveDrivetrainField(decoded = {}, manualOverrides = {}) {
  const override = normalizeDrivetrain(manualOverrides?.drivetrain);
  if (override && override !== '2WD') {
    return {
      field: 'drivetrain',
      resolved: true,
      value: override,
      source: 'manual',
      confidence: 'confirmed',
      exact: true,
      warnings: [],
      reason: null,
    };
  }

  const exactDrive = normalizeDrivetrain(decoded?.enrichment?.exactVinMatch?.drivetrain);
  if (exactDrive && exactDrive !== '2WD') {
    return {
      field: 'drivetrain',
      resolved: true,
      value: exactDrive,
      source: 'exactVin',
      confidence: 'high',
      exact: true,
      warnings: [],
      reason: null,
    };
  }

  const selectedDrive = normalizeDrivetrain(decoded?.enrichment?.selectedGearbox?.drivetrain);
  if (selectedDrive && selectedDrive !== '2WD') {
    return {
      field: 'drivetrain',
      resolved: true,
      value: selectedDrive,
      source: 'selected_gearbox_master',
      confidence: 'high',
      exact: true,
      warnings: [],
      reason: null,
    };
  }

  const decodedDrive = normalizeDrivetrain(decoded?.drivetrain);
  if (decodedDrive === 'AWD' || decodedDrive === 'FWD') {
    return {
      field: 'drivetrain',
      resolved: true,
      value: decodedDrive,
      source: 'decoded',
      confidence: decodedDrive === 'AWD' ? 'high' : 'medium',
      exact: decodedDrive === 'AWD',
      warnings: [],
      reason: null,
    };
  }

  const consensus = candidateConsensus(decoded?.enrichment?.possibleGearboxCodes || []);
  if (consensus) {
    return {
      field: 'drivetrain',
      resolved: true,
      value: consensus.value,
      source: consensus.source,
      confidence: consensus.confidence,
      exact: consensus.exact,
      warnings: consensus.confidence === 'medium' ? ['drivetrain_inferred_from_partial_gearbox_consensus'] : [],
      reason: null,
    };
  }

  const patternDrives = unique((decoded?.enrichment?.patternRule?.drivetrains || []).map(normalizeDrivetrain).filter(Boolean));
  const precisePatternDrives = patternDrives.filter((value) => value !== '2WD');
  if (precisePatternDrives.length === 1) {
    return {
      field: 'drivetrain',
      resolved: true,
      value: precisePatternDrives[0],
      source: 'pattern_rule',
      confidence: 'medium',
      exact: false,
      warnings: ['drivetrain_inferred_from_pattern_rule'],
      reason: null,
    };
  }

  if (decodedDrive === '2WD') {
    return {
      field: 'drivetrain',
      resolved: false,
      value: null,
      displayValue: '2WD',
      source: 'non_awd_only',
      confidence: 'low',
      exact: false,
      warnings: [],
      reason: 'drivetrain_not_precise_enough',
    };
  }

  return {
    field: 'drivetrain',
    resolved: false,
    value: null,
    source: 'missing',
    confidence: 'none',
    exact: false,
    warnings: [],
    reason: 'drivetrain_unknown',
  };
}

export default resolveDrivetrainField;
