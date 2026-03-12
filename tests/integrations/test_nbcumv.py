from fractions import Fraction

from trr_backend.integrations import nbcumv


def test_find_show_image_by_filename_uses_show_index(monkeypatch) -> None:
    monkeypatch.setattr(
        nbcumv,
        "list_show_images",
        lambda show_id, session=None, limit=None: [
            {"lbx_filename": "NUP_209430_00480.jpg", "lbx_id": "70075355"},
            {"lbx_filename": "NUP_209430_00178.JPG", "lbx_id": "70075342"},
        ],
    )

    image = nbcumv.find_show_image_by_filename("show-1", "NUP_209430_00178.jpg")

    assert image is not None
    assert image["lbx_id"] == "70075342"


def test_json_safe_value_normalizes_fraction_like_values() -> None:
    payload = {
        "fraction": Fraction(3, 2),
        "items": [Fraction(2, 1)],
        "text": "abc\u0000def\u001bghi",
    }

    result = nbcumv._json_safe_value(payload)

    assert result == {
        "fraction": 1.5,
        "items": [2],
        "text": "abcdefghi",
    }
