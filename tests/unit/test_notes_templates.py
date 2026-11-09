import re
from datetime import date, datetime

import numpy as np
import pytest

from ed_ops.generator.notes_templates import ARCHETYPES, acuity_bucket, pick_archetype, render_note

PATIENT = {
    "first_name": "Ava", "last_name": "Carter", "dob": date(1958, 3, 14), "sex": "F", "phone": "312-555-0147",
    "street": "42 Elm St", "zip": "62704", "mrn": "MRN-7000042", "city": "Springfield", "state": "IL",
}
PHI_SLOTS = ("{dob}", "{mrn}", "{phone}", "{street}")


def test_archetype_catalogue_complete():
    assert len(ARCHETYPES) == 12
    assert sum(a["weight"] for a in ARCHETYPES.values()) == pytest.approx(1.0)
    for name, a in ARCHETYPES.items():
        assert a["category"] in {"cardiac", "respiratory", "neurological", "trauma", "gastrointestinal",
                                 "infectious", "psychiatric", "obstetric", "musculoskeletal", "other"}
        assert a["complaints"]
        for bucket in ("high", "mid", "low"):
            template = a[bucket]
            assert "{name}" in template, (name, bucket)
            assert any(slot in template for slot in PHI_SLOTS), (name, bucket)


def test_acuity_bucket():
    assert [acuity_bucket(e) for e in (1, 2, 3, 4, 5)] == ["high", "high", "mid", "low", "low"]


def test_render_high_acuity_note_contains_phi_and_abnormal_vitals():
    rng = np.random.default_rng(1)
    note, complaint = render_note(rng, "chest_pain", 1, PATIENT, datetime(2026, 11, 9, 12))
    assert "Ava Carter" in note
    assert "{" not in note and "}" not in note
    hr = int(re.search(r"HR (\d+)", note).group(1))
    spo2 = int(re.search(r"SpO2 (\d+)%", note).group(1))
    assert hr >= 110 and spo2 <= 90
    assert complaint


def test_render_low_acuity_note_has_normal_vitals():
    rng = np.random.default_rng(2)
    note, _ = render_note(rng, "back_pain", 5, PATIENT, datetime(2026, 11, 9, 12))
    assert int(re.search(r"SpO2 (\d+)%", note).group(1)) >= 97


def test_male_patients_never_get_pregnancy_archetype():
    rng = np.random.default_rng(3)
    assert "pregnancy_concern" not in {pick_archetype(rng, "M") for _ in range(3000)}
    assert "pregnancy_concern" in {pick_archetype(rng, "F") for _ in range(3000)}


def test_render_is_deterministic():
    a = render_note(np.random.default_rng(9), "fever_infection", 3, PATIENT, datetime(2026, 11, 9, 12))
    b = render_note(np.random.default_rng(9), "fever_infection", 3, PATIENT, datetime(2026, 11, 9, 12))
    assert a == b
