import gearboxCodesMaster from '../../data/gearbox_codes_master.json' with { type: 'json' };
import gearboxBusinessGroups from '../../data/gearbox_business_groups.json' with { type: 'json' };

function normalize(value) {
  return typeof value === 'string' ? value.trim().toUpperCase() : '';
}

function unique(values = []) {
  return [...new Set((values || []).filter(Boolean))];
}

function isCodeLike(value) {
  return /^[A-Z0-9]{3}$/u.test(normalize(value));
}

function normalizeTransmissionType(value) {
  const normalized = normalize(value);
  if (!normalized) return null;
  if (normalized === 'DSG') return 'dsg';
  if (normalized === 'MANUAL') return 'manual';
  if (normalized === 'AUTOMATIC' || normalized === 'AUTOMATIK' || normalized === 'AUTO') return 'automatic';
  return normalized.toLowerCase();
}

function familyFromLabelHint(value) {
  const normalized = normalize(value);
  if (!normalized) return null;
  if (normalized.includes('DQ200')) return 'DQ200';
  if (normalized.includes('DQ381')) return 'DQ381';
  if (normalized.includes('DQ250')) return 'DQ250';
  if (normalized.includes('DQ400')) return 'DQ400';
  if (normalized.includes('MQ200')) return 'MQ200';
  if (normalized.includes('MQ250')) return 'MQ250';
  if (normalized.includes('MQ281')) return 'MQ281';
  return null;
}

function familyFromBusinessGroup(groupKey) {
  if (!groupKey) return null;
  if (groupKey === 'DSG_DQ200_NO_SERVICE') return 'DQ200';
  return null;
}

function transmissionTypeFromBusinessGroup(groupKey) {
  if (!groupKey) return null;
  if (groupKey.startsWith('DSG')) return 'dsg';
  if (groupKey.startsWith('MANUAL')) return 'manual';
  if (groupKey === 'AUTOMATIK' || groupKey === 'TORQUE_CONVERTER_AUTO') return 'automatic';
  return null;
}

function displayLabelForTransmissionType(transmissionType, fallback = null) {
  if (transmissionType === 'dsg') return 'DSG';
  if (transmissionType === 'manual') return 'Manual';
  if (transmissionType === 'automatic') return 'Automatic';
  return fallback;
}

function findGroupForCode(code) {
  if (!code) return null;
  for (const [groupKey, group] of Object.entries(gearboxBusinessGroups || {})) {
    if (Array.isArray(group?.codes) && group.codes.includes(code)) {
      return { key: groupKey, ...group };
    }
  }
  return null;
}

function getMasterSemanticForCode(code) {
  const normalizedCode = normalize(code);
  if (!normalizedCode) {
    return {
      code: null,
      family: null,
      transmissionType: null,
      drivetrain: null,
      serviceRequired: false,
      maintenanceGroup: null,
      groupLabel: null,
      closureSource: null,
    };
  }

  const master = gearboxCodesMaster?.[normalizedCode] || null;
  const group = findGroupForCode(normalizedCode);

  const transmissionType =
    normalizeTransmissionType(master?.type) || transmissionTypeFromBusinessGroup(group?.key) || null;

  const family = master?.family || familyFromBusinessGroup(group?.key) || null;
  const drivetrain = master?.drivetrain || null;
  const serviceRequired = Boolean(master?.serviceRequired);

  let maintenanceGroup = group?.key || null;
  if (!maintenanceGroup) {
    if (transmissionType === 'dsg' && family === 'DQ200') maintenanceGroup = 'DSG_DQ200_NO_SERVICE';
    else if (transmissionType === 'dsg' && serviceRequired) maintenanceGroup = 'DSG_WET_SERVICEABLE';
    else if (transmissionType === 'manual' && ['MQ200', 'MQ250'].includes(family)) maintenanceGroup = 'MANUAL_5';
    else if (transmissionType === 'manual' && family) maintenanceGroup = 'MANUAL_6';
    else if (transmissionType === 'automatic') maintenanceGroup = 'TORQUE_CONVERTER_AUTO';
  }

  return {
    code: normalizedCode,
    family,
    transmissionType,
    drivetrain,
    serviceRequired,
    maintenanceGroup,
    groupLabel: group?.label || null,
    closureSource: master ? 'code_master' : group ? 'business_group' : null,
  };
}

function deriveCandidateConsensus(candidates = []) {
  const codes = unique((candidates || []).map(normalize).filter(isCodeLike));
  const semantics = codes.map((code) => getMasterSemanticForCode(code));

  const families = unique(semantics.map((item) => item.family).filter(Boolean));
  const transmissionTypes = unique(semantics.map((item) => item.transmissionType).filter(Boolean));
  const drivetrains = unique(semantics.map((item) => item.drivetrain).filter(Boolean));
  const maintenanceGroups = unique(semantics.map((item) => item.maintenanceGroup).filter(Boolean));

  const family = families.length === 1 ? families[0] : null;
  const transmissionType = transmissionTypes.length === 1 ? transmissionTypes[0] : null;
  const drivetrain = drivetrains.length === 1 ? drivetrains[0] : null;
  const maintenanceGroup = maintenanceGroups.length === 1 ? maintenanceGroups[0] : null;
  const hasConflict = transmissionTypes.length > 1 || families.length > 1;
  const conflictCount = Math.max(0, transmissionTypes.length - 1) + Math.max(0, families.length - 1);

  return {
    codes,
    codeCount: codes.length,
    family,
    transmissionType,
    drivetrain,
    maintenanceGroup,
    familyClosed: Boolean(family),
    transmissionTypeClosed: Boolean(transmissionType),
    hasConflict,
    conflictCount,
    conflictingFamilies: families.length > 1 ? families : [],
    conflictingTransmissionTypes: transmissionTypes.length > 1 ? transmissionTypes : [],
    closureSource: family || transmissionType ? 'candidate_consensus' : null,
  };
}

export function getGearboxSemanticProfile(rawCodeOrLabel, candidates = [], options = {}) {
  const normalizedInput = normalize(rawCodeOrLabel);
  const directCode = isCodeLike(normalizedInput) ? normalizedInput : null;
  const masterSemantic = directCode ? getMasterSemanticForCode(directCode) : null;
  const candidateConsensus = deriveCandidateConsensus(candidates);

  const allowLabelInference = options?.allowLabelInference === true;
  const labelTransmissionType = allowLabelInference
    ? normalizeTransmissionType(
        normalizedInput.includes('DSG')
          ? 'DSG'
          : normalizedInput.includes('MANUAL')
          ? 'manual'
          : normalizedInput.includes('AUTOMAT')
          ? 'automatic'
          : null
      )
    : null;
  const labelFamily = allowLabelInference ? familyFromLabelHint(normalizedInput) : null;
  const explicitLabelClosure = Boolean(labelTransmissionType || labelFamily);

  const transmissionType =
    masterSemantic?.transmissionType || candidateConsensus.transmissionType || labelTransmissionType || null;

  const family = masterSemantic?.family || candidateConsensus.family || labelFamily || null;
  const drivetrain = masterSemantic?.drivetrain || candidateConsensus.drivetrain || null;
  const serviceRequired = Boolean(masterSemantic?.serviceRequired);
  const maintenanceGroup =
    masterSemantic?.maintenanceGroup ||
    candidateConsensus.maintenanceGroup ||
    (transmissionType === 'dsg' && family === 'DQ200'
      ? 'DSG_DQ200_NO_SERVICE'
      : transmissionType === 'dsg' && serviceRequired
      ? 'DSG_WET_SERVICEABLE'
      : transmissionType === 'manual' && ['MQ200', 'MQ250'].includes(family)
      ? 'MANUAL_5'
      : transmissionType === 'manual' && family
      ? 'MANUAL_6'
      : transmissionType === 'automatic'
      ? 'TORQUE_CONVERTER_AUTO'
      : null);

  const displayLabel =
    displayLabelForTransmissionType(transmissionType, masterSemantic?.code || rawCodeOrLabel || null) || null;

  const directMasterClosure = Boolean(masterSemantic?.closureSource && (masterSemantic?.family || masterSemantic?.transmissionType));
  const suppressCandidateConflict = explicitLabelClosure || directMasterClosure;

  return {
    code: directCode || null,
    type: transmissionType,
    transmissionType,
    family,
    drivetrain,
    serviceRequired,
    maintenanceGroup,
    groupLabel: masterSemantic?.groupLabel || null,
    displayLabel,
    candidateCount: Array.isArray(candidates) ? candidates.filter(Boolean).length : 0,
    candidateConsensus,
    familyClosed: Boolean(family),
    transmissionTypeClosed: Boolean(transmissionType),
    hasConflict: suppressCandidateConflict ? false : Boolean(candidateConsensus.hasConflict),
    conflictCount: suppressCandidateConflict ? 0 : Number(candidateConsensus.conflictCount || 0),
    conflictingFamilies: suppressCandidateConflict ? [] : candidateConsensus.conflictingFamilies || [],
    conflictingTransmissionTypes: suppressCandidateConflict ? [] : candidateConsensus.conflictingTransmissionTypes || [],
    closureSource:
      masterSemantic?.closureSource ||
      candidateConsensus.closureSource ||
      (explicitLabelClosure ? 'label_hint' : null),
  };
}

export function inferConsensusGearboxType(codes = []) {
  return deriveCandidateConsensus(codes).transmissionType || null;
}

export function inferConsensusGearboxFamily(codes = []) {
  return deriveCandidateConsensus(codes).family || null;
}

export function deriveGearboxClosure({ code = null, candidates = [], allowLabelInference = false } = {}) {
  return getGearboxSemanticProfile(code, candidates, { allowLabelInference });
}

export default getGearboxSemanticProfile;
