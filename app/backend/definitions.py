"""Metric definitions, bands, assumptions and citations shown on the Definitions page (spec 13.2)."""

GEDWIN_BUSY, GEDWIN_CROWDED = 2.06, 2.34


def band(gedwin: float | None) -> str:
    if gedwin is None:
        return "Active"  # census 0
    return "Active" if gedwin < GEDWIN_BUSY else "Busy" if gedwin <= GEDWIN_CROWDED else "Crowded"


DEFINITIONS = {
    "bands": {
        "gedwin": {"busy": GEDWIN_BUSY, "crowded": GEDWIN_CROWDED},
        "edwin": {"busy": 1.5, "crowded": 2.0},
        "nedocs": {"levels": [20, 60, 100, 140, 180],
                   "labels": ["Not busy", "Busy", "Extremely busy", "Overcrowded", "Severely overcrowded", "Dangerously overcrowded"]},
    },
    "formulas": {
        "occupancy": "OR = census / ED beds; OR_c = OR clamped to [0.01, 0.99]",
        "acuity": "AR = sum(n_i * t_i) / census, t_i = 6 - ESI, untriaged = 3",
        "edwin": "EDWIN = (1 / attendings on duty) * OR_c / (1 - OR_c) * AR",
        "gedwin": "gEDWIN = ln(OR_c / (1 - OR_c)) + ln(AR)",
        "nedocs": "NEDOCS = -20 + 85.8*(census/ED beds) + 600*(boarders/inpatient beds) + 13.4*ventilators "
                  "+ 0.93*longest boarder hours + 5.64*last door-to-bed hours, clamped 0-200",
        "door_to_provider": "Minutes from arrival to provider greet, over patients greeted in the last 60 minutes",
    },
    "assumptions": [
        "gEDWIN uses the natural log (the paper does not state the base; its analysis used R, where log() is natural).",
        "Untriaged patients are weighted as ESI 3 (t = 3).",
        "Occupancy is floored at 0.01 (the paper only caps at 0.99).",
        "Door-to-provider uses a 1-hour window; LWBS, length of stay and boarding use 24-hour windows.",
        "All facilities share one time zone; timestamps are stored in UTC.",
        "Door-to-provider statistics are marked low-sample when fewer than 5 patients were greeted.",
        "AI-suggested ESI is decision support for the demo, not clinical advice. All data is synthetic.",
    ],
    "citations": [
        "gEDWIN: a simple and practical index for real-time monitoring of emergency department crowding. BMC Emergency Medicine, 2025.",
        "Weiss SJ et al. Estimating the degree of emergency department overcrowding in academic medical centers (NEDOCS). Acad Emerg Med, 2004.",
    ],
}
