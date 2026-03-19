export const VEHICLE_PRESENTATION = {
  scala: {
    label: "Škoda Scala I",
    image:
      "https://upload.wikimedia.org/wikipedia/commons/thumb/8/84/2019_Skoda_Scala_SE_L_1.5_Front.jpg/640px-2019_Skoda_Scala_SE_L_1.5_Front.jpg",
  },
  octavia: {
    label: "Škoda Octavia IV",
    image:
      "https://upload.wikimedia.org/wikipedia/commons/thumb/0/05/2020_Skoda_Octavia_SE_L_TSi_MHEV_1.0_Front.jpg/640px-2020_Skoda_Octavia_SE_L_TSi_MHEV_1.0_Front.jpg",
  },
  superb: {
    label: "Škoda Superb III",
    image:
      "https://upload.wikimedia.org/wikipedia/commons/thumb/2/22/2019_Skoda_Superb_SE_L_Executive_TDi_S-A_2.0_Front.jpg/640px-2019_Skoda_Superb_SE_L_Executive_TDi_S-A_2.0_Front.jpg",
  },
  kodiaq: {
    label: "Škoda Kodiaq II",
    image:
      "https://upload.wikimedia.org/wikipedia/commons/thumb/6/67/2024_Skoda_Kodiaq_Sportline_TDi_S-A_2.0_Front.jpg/640px-2024_Skoda_Kodiaq_Sportline_TDi_S-A_2.0_Front.jpg",
  },
  karoq: {
    label: "Škoda Karoq",
    image:
      "https://upload.wikimedia.org/wikipedia/commons/thumb/7/79/2018_Skoda_Karoq_SE_L_TSi_1.5_Front.jpg/640px-2018_Skoda_Karoq_SE_L_TSi_1.5_Front.jpg",
  },
  kamiq: {
    label: "Škoda Kamiq",
    image:
      "https://upload.wikimedia.org/wikipedia/commons/thumb/4/40/2019_Skoda_Kamiq_SE_L_TSi_1.0_Front.jpg/640px-2019_Skoda_Kamiq_SE_L_TSi_1.0_Front.jpg",
  },
};

export function getVehiclePresentation(decoded) {
  const rawModel = String(decoded?.model || "").toLowerCase();

  if (rawModel.includes("scala")) return VEHICLE_PRESENTATION.scala;
  if (rawModel.includes("octavia")) return VEHICLE_PRESENTATION.octavia;
  if (rawModel.includes("superb")) return VEHICLE_PRESENTATION.superb;
  if (rawModel.includes("kodiaq")) return VEHICLE_PRESENTATION.kodiaq;
  if (rawModel.includes("karoq")) return VEHICLE_PRESENTATION.karoq;
  if (rawModel.includes("kamiq")) return VEHICLE_PRESENTATION.kamiq;

  return {
    label: `${decoded?.marka || "Škoda"} ${decoded?.model || "-"}`,
    image: "",
  };
}