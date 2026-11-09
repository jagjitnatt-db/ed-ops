"""Triage-note templates. Each archetype has one template per acuity bucket; every template embeds PHI
(name plus at least one of DOB, MRN, phone, street) so ai_mask has something real to find (Plan 2)."""

from __future__ import annotations

from datetime import datetime

import numpy as np

V = "HR {hr}, BP {sbp}/{dbp}, RR {rr}, SpO2 {spo2}%, T {temp}C"

ARCHETYPES: dict[str, dict] = {
    "chest_pain": {
        "category": "cardiac", "weight": 0.10,
        "complaints": ["chest pain", "chest tightness", "CP radiating to arm"],
        "high": "{name} ({age}{sex_short}), DOB {dob}, {mrn}. BIBA c/o crushing substernal chest pain radiating to L arm x {onset}, diaphoretic, nauseated. " + V + ". Pain {pain}/10. Callback {phone}.",
        "mid": "Pt {name}, {age}yo {sex_word}, {mrn}, reports intermittent chest tightness x {onset}, worse with exertion. " + V + ", pain {pain}/10. Hx HTN. Contact {phone}.",
        "low": "{name} DOB {dob} c/o sharp L chest wall pain after lifting boxes x {onset}, reproducible on palpation. " + V + ", pain {pain}/10. {mrn}.",
    },
    "shortness_of_breath": {
        "category": "respiratory", "weight": 0.10,
        "complaints": ["shortness of breath", "SOB", "difficulty breathing"],
        "high": "BIBA {name}, {age}{sex_short}, {mrn}: severe SOB x {onset}, 2-3 word sentences, accessory muscle use, cyanotic lips. " + V + " on NRB. Lives at {street}, {city}.",
        "mid": "{name} ({mrn}) c/o SOB and productive cough x {onset}, mild wheeze. " + V + ", pain {pain}/10. DOB {dob}.",
        "low": "Pt {name}, DOB {dob}, mild SOB with seasonal allergies x {onset}, full sentences, no distress. " + V + ". Phone {phone}.",
    },
    "stroke_symptoms": {
        "category": "neurological", "weight": 0.04,
        "complaints": ["weakness one side", "slurred speech", "facial droop"],
        "high": "STROKE ALERT. {name}, {age}{sex_short}, {mrn}, sudden R-sided weakness and slurred speech, LKW {onset} ago, facial droop. " + V + ". Daughter callback {phone}.",
        "mid": "{name} DOB {dob} reports transient L hand numbness x {onset}, now resolved, no weakness. " + V + ". {mrn}.",
        "low": "Pt {name} ({mrn}) c/o dizziness when standing x {onset}, no focal deficits, ambulatory. " + V + ", pain {pain}/10.",
    },
    "headache": {
        "category": "neurological", "weight": 0.07,
        "complaints": ["headache", "migraine", "head pain"],
        "high": "{name}, {age}{sex_short}, {mrn}: worst headache of life, thunderclap onset {onset} ago, vomiting, neck stiffness. " + V + ", pain {pain}/10. DOB {dob}.",
        "mid": "Pt {name} c/o migraine x {onset} unrelieved by home meds, photophobia. " + V + ", pain {pain}/10. {mrn}, ph {phone}.",
        "low": "{name} DOB {dob} mild tension-type headache x {onset}, alert, ambulatory. " + V + ", pain {pain}/10.",
    },
    "abdominal_pain": {
        "category": "gastrointestinal", "weight": 0.14,
        "complaints": ["abdominal pain", "stomach pain", "abd pain with vomiting"],
        "high": "{name}, {age}{sex_short}, {mrn}, severe diffuse abd pain x {onset}, rigid abdomen, pale and clammy. " + V + ", pain {pain}/10. Lives {street}, {city}.",
        "mid": "Pt {name} (DOB {dob}) RLQ pain x {onset} with nausea, anorexia. " + V + ", pain {pain}/10. {mrn}.",
        "low": "{name} c/o mild epigastric discomfort after meals x {onset}, tolerating PO. " + V + ", pain {pain}/10. Callback {phone}.",
    },
    "fever_infection": {
        "category": "infectious", "weight": 0.10,
        "complaints": ["fever", "fever and chills", "possible infection"],
        "high": "{name}, {age}{sex_short}, {mrn}: fever, rigors and confusion x {onset}, mottled skin, suspected sepsis. " + V + ". Family ph {phone}.",
        "mid": "Pt {name} DOB {dob} fever and dysuria x {onset}, flank pain. " + V + ", pain {pain}/10. {mrn}.",
        "low": "{name} ({mrn}) sore throat and low-grade fever x {onset}, eating and drinking. " + V + ", pain {pain}/10.",
    },
    "fall_injury": {
        "category": "trauma", "weight": 0.10,
        "complaints": ["fall", "injury after fall", "hip pain after fall"],
        "high": "BIBA {name}, {age}{sex_short}, {mrn}, fall from ladder ~12 ft {onset} ago, GCS 13, obvious L femur deformity. " + V + ", pain {pain}/10. DOB {dob}.",
        "mid": "{name} DOB {dob} mechanical fall at home {onset} ago, R wrist swelling, no LOC. " + V + ", pain {pain}/10. {mrn}, lives {street}.",
        "low": "Pt {name} rolled R ankle {onset} ago, ambulatory with limp. " + V + ", pain {pain}/10. Ph {phone}.",
    },
    "laceration": {
        "category": "trauma", "weight": 0.07,
        "complaints": ["laceration", "cut", "bleeding wound"],
        "high": "{name}, {age}{sex_short}, {mrn}: deep L forearm laceration with pulsatile bleeding x {onset}, tourniquet applied by EMS. " + V + ", pain {pain}/10.",
        "mid": "{name} DOB {dob} 4 cm scalp laceration after fall {onset} ago, bleeding controlled. " + V + ", pain {pain}/10. {mrn}.",
        "low": "Pt {name} ({mrn}) small finger laceration from kitchen knife {onset} ago, bleeding controlled. " + V + ", pain {pain}/10. Callback {phone}.",
    },
    "back_pain": {
        "category": "musculoskeletal", "weight": 0.09,
        "complaints": ["back pain", "low back pain", "back injury"],
        "high": "{name}, {age}{sex_short}, {mrn}: sudden tearing back pain radiating to abdomen x {onset}, diaphoretic. " + V + ", pain {pain}/10. DOB {dob}.",
        "mid": "Pt {name} DOB {dob} low back pain with new leg numbness x {onset}. " + V + ", pain {pain}/10. {mrn}.",
        "low": "{name} ({mrn}) chronic low back pain flare x {onset}, no red flags, ambulatory. " + V + ", pain {pain}/10. Ph {phone}.",
    },
    "behavioral_crisis": {
        "category": "psychiatric", "weight": 0.06,
        "complaints": ["anxiety", "depression", "psychiatric evaluation"],
        "high": "{name}, {age}{sex_short}, {mrn}, brought by police, active suicidal ideation with plan, agitated, escalating x {onset}. " + V + ". Emergency contact {phone}.",
        "mid": "Pt {name} DOB {dob} anxiety and panic attacks x {onset}, passive SI without plan. " + V + ". {mrn}.",
        "low": "{name} ({mrn}) requesting medication refill, insomnia x {onset}, calm and cooperative. " + V + ".",
    },
    "pregnancy_concern": {
        "category": "obstetric", "weight": 0.03,
        "complaints": ["pregnancy bleeding", "pregnant with abdominal pain", "pregnancy concern"],
        "high": "{name}, {age}F, {mrn}, 34 wks pregnant, heavy vaginal bleeding and severe abd pain x {onset}. " + V + ", pain {pain}/10. Partner {phone}.",
        "mid": "Pt {name} DOB {dob}, 10 wks pregnant, light spotting and cramping x {onset}. " + V + ", pain {pain}/10. {mrn}.",
        "low": "{name} ({mrn}), 20 wks pregnant, mild nausea x {onset}, tolerating fluids. " + V + ", pain {pain}/10.",
    },
    "allergic_reaction": {
        "category": "other", "weight": 0.10,
        "complaints": ["allergic reaction", "rash", "hives"],
        "high": "{name}, {age}{sex_short}, {mrn}: throat swelling and stridor after peanut exposure {onset} ago, diffuse hives, EpiPen x1. " + V + ". DOB {dob}.",
        "mid": "Pt {name} DOB {dob} facial swelling and hives after new medication x {onset}, no stridor. " + V + ". {mrn}.",
        "low": "{name} ({mrn}) localized rash on arm x {onset}, itchy, no swelling. " + V + ", pain {pain}/10. Ph {phone}.",
    },
}

VITALS = {
    "high": {"hr": (110, 140), "sbp": (78, 100), "dbp": (45, 62), "rr": (24, 32), "spo2": (82, 90), "temp": (36.0, 39.8), "pain": (8, 10)},
    "mid": {"hr": (90, 110), "sbp": (105, 150), "dbp": (65, 95), "rr": (18, 22), "spo2": (93, 96), "temp": (36.5, 38.6), "pain": (5, 7)},
    "low": {"hr": (64, 92), "sbp": (112, 138), "dbp": (68, 88), "rr": (12, 18), "spo2": (97, 100), "temp": (36.4, 37.4), "pain": (1, 4)},
}
COMPLAINT_PHI_P = 0.3
_NAMES = tuple(ARCHETYPES)


def acuity_bucket(esi: int) -> str:
    return "high" if esi <= 2 else "mid" if esi == 3 else "low"


def pick_archetype(rng: np.random.Generator, sex: str) -> str:
    names = [n for n in _NAMES if not (sex == "M" and ARCHETYPES[n]["category"] == "obstetric")]
    weights = np.array([ARCHETYPES[n]["weight"] for n in names])
    return names[int(rng.choice(len(names), p=weights / weights.sum()))]


def _onset(rng: np.random.Generator, bucket: str) -> str:
    if bucket == "high":
        return f"{int(rng.integers(10, 90))} min"
    if bucket == "mid":
        return f"{int(rng.integers(2, 24))} hours"
    return f"{int(rng.integers(1, 7))} days"


def render_note(rng: np.random.Generator, archetype: str, true_esi: int, patient: dict, arrival: datetime) -> tuple[str, str]:
    bucket = acuity_bucket(true_esi)
    v = VITALS[bucket]
    dob = patient["dob"]
    age = arrival.year - dob.year - ((arrival.month, arrival.day) < (dob.month, dob.day))
    slots = {
        "name": f"{patient['first_name']} {patient['last_name']}",
        "age": age,
        "sex_short": patient["sex"],
        "sex_word": "female" if patient["sex"] == "F" else "male",
        "dob": dob.strftime("%m/%d/%Y"),
        "mrn": patient["mrn"],
        "phone": patient["phone"],
        "street": patient["street"],
        "city": patient["city"],
        "hr": int(rng.integers(v["hr"][0], v["hr"][1] + 1)),
        "sbp": int(rng.integers(v["sbp"][0], v["sbp"][1] + 1)),
        "dbp": int(rng.integers(v["dbp"][0], v["dbp"][1] + 1)),
        "rr": int(rng.integers(v["rr"][0], v["rr"][1] + 1)),
        "spo2": int(rng.integers(v["spo2"][0], v["spo2"][1] + 1)),
        "temp": f"{rng.uniform(*v['temp']):.1f}",
        "pain": int(rng.integers(v["pain"][0], v["pain"][1] + 1)),
        "onset": _onset(rng, bucket),
    }
    note = ARCHETYPES[archetype][bucket].format(**slots)
    complaints = ARCHETYPES[archetype]["complaints"]
    complaint = complaints[int(rng.integers(0, len(complaints)))]
    if rng.random() < COMPLAINT_PHI_P:
        complaint = f"{complaint} - pt {slots['name']}, cb {patient['phone']}"
    return note, complaint
