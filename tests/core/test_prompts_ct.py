import pytest

from neocortex.prompts.base import load_prompt_template


def test_load_prompt_template_requires_string_system(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "neocortex.prompts.base._load_prompt_document",
        lambda _name: {"dependencies": [], "system": ["bad"], "user": "ok"},
    )

    with pytest.raises(ValueError, match="'system' must be a string"):
        load_prompt_template("dummy.yaml")


def test_load_prompt_template_requires_string_user(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "neocortex.prompts.base._load_prompt_document",
        lambda _name: {"dependencies": [], "system": "ok", "user": {"bad": "shape"}},
    )

    with pytest.raises(ValueError, match="'user' must be a string"):
        load_prompt_template("dummy.yaml")
