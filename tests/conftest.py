"""Shared test fixtures for migration tests."""

import json
import os
import sys

import pytest
from unittest.mock import MagicMock

# Ensure migration_utils is importable from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def valid_config():
    """Return a complete, valid configuration dict."""
    return {
        "source": {
            "server_url": "https://test-server.com",
            "site_content_url": "test-site",
            "access_token_name": "test-token-name",
            "access_token": "test-token-secret"
        },
        "destination": {
            "pod_url": "https://10ax.online.tableau.com",
            "site_content_url": "cloud-site",
            "access_token_name": "cloud-token-name",
            "access_token": "cloud-token-secret"
        },
        "default_content_owner": "admin@test.com",
        "default_email_domain": "@test.com",
        "migration_scope": {
            "data_sources": True,
            "workbooks": True
        }
    }


@pytest.fixture
def config_without_scope():
    """Config without migration_scope (backward compatibility test)."""
    return {
        "source": {
            "server_url": "https://test-server.com",
            "site_content_url": "test-site",
            "access_token_name": "test-token-name",
            "access_token": "test-token-secret"
        },
        "destination": {
            "pod_url": "https://10ax.online.tableau.com",
            "site_content_url": "cloud-site",
            "access_token_name": "cloud-token-name",
            "access_token": "cloud-token-secret"
        },
        "default_content_owner": "admin@test.com",
        "default_email_domain": "@test.com"
    }


@pytest.fixture
def tmp_config_file(tmp_path, valid_config):
    """Write a valid config to a temp file and return its path."""
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(valid_config))
    return str(config_path)


@pytest.fixture
def mock_mapping_context():
    """
    Factory fixture that creates a mock mapping context for a given username.

    The SDK context has:
      - ctx.content_item.name  (the username string)
      - ctx.mapped_location.parent()  (returns a domain object)
      - ctx.map_to(location)  (returns mapped context)
    """
    def _make_context(username):
        ctx = MagicMock()
        ctx.content_item.name = username

        domain = MagicMock()
        ctx.mapped_location.parent.return_value = domain

        # map_to returns the context itself (for chaining)
        ctx.map_to.return_value = ctx
        return ctx

    return _make_context
