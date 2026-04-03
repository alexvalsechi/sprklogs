from backend.services.llm_prompt_builder import build_analysis_prompt


def test_build_analysis_prompt_includes_sparklens_guidance_and_json():
    prompt, py_files_provided = build_analysis_prompt(
        reduced_report="## Reduced report",
        summary=None,
        sparklens_context={
            "app": {"driver_idle_pct": 38.4},
            "cluster": {"cluster_utilization_pct": 42.1},
        },
        language="pt",
    )

    assert py_files_provided is False
    assert "## Regras Deterministicas do Sparklens" in prompt
    assert "cluster_utilization_pct" in prompt
    assert '"driver_idle_pct": 38.4' in prompt
    assert "Todos os campos textuais narrativos do JSON DEVEM ser escritos em Portugues do Brasil." in prompt


def test_build_analysis_prompt_omits_sparklens_section_when_not_provided():
    prompt, _ = build_analysis_prompt(
        reduced_report="## Reduced report",
        summary=None,
        sparklens_context=None,
        language="en",
    )

    assert "## Deterministic Sparklens Rules" not in prompt
    assert "## Deterministic Sparklens Metrics" not in prompt
    assert "All narrative string fields in the JSON MUST be written in English." in prompt
