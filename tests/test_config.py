from __future__ import annotations

import sys

from config.job_args import configure_from_args
from config.project_config import load_config


def test_national_performance_defaults(monkeypatch) -> None:
    for name in ("CMS_PAGE_SIZE", "CMS_WRITE_BATCH_SIZE", "MAX_OPENFDA_PRODUCTS", "MEDICAID_STATES", "CMS_MODE"):
        monkeypatch.delenv(name, raising=False)

    config = load_config()

    assert config.catalog == "workspace"
    assert config.states == ()
    assert config.national_scope is True
    assert config.cms_mode == "bulk_csv"
    assert config.cms_page_size == 5000
    assert config.cms_write_batch_size == 50000
    assert config.max_openfda_products == 20


def test_runtime_scope_arguments(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["pipeline.py", "--medicaid-states", "CA", "--max-openfda-products", "10"],
    )

    configure_from_args()

    assert load_config().states == ("CA",)
    assert load_config().max_openfda_products == 10
