"""Tests for configuration loading and validation."""

import json

import pytest

from migration_utils import load_config, validate_config


class TestLoadConfig:
    """Tests for load_config function."""

    def test_load_valid_config(self, tmp_config_file, valid_config):
        """Loading a valid config.json returns a dict with expected keys."""
        config = load_config(config_path=tmp_config_file)
        assert config is not None
        assert config == valid_config

    def test_load_missing_config(self, tmp_path):
        """Loading a nonexistent file returns None."""
        config = load_config(config_path=str(tmp_path / "nonexistent.json"))
        assert config is None

    def test_load_invalid_json(self, tmp_path):
        """Loading a file with invalid JSON raises an error."""
        bad_file = tmp_path / "config.json"
        bad_file.write_text("not valid json {{{")
        with pytest.raises(json.JSONDecodeError):
            load_config(config_path=str(bad_file))


class TestValidateConfig:
    """Tests for validate_config function."""

    def test_valid_config_passes(self, valid_config):
        """A complete config passes validation."""
        assert validate_config(valid_config) is True

    def test_none_config_fails(self):
        """None config fails validation."""
        assert validate_config(None) is False

    def test_empty_config_fails(self):
        """Empty dict fails validation."""
        assert validate_config({}) is False

    def test_missing_source_section_fails(self, valid_config):
        """Missing 'source' section fails."""
        del valid_config['source']
        assert validate_config(valid_config) is False

    def test_missing_destination_section_fails(self, valid_config):
        """Missing 'destination' section fails."""
        del valid_config['destination']
        assert validate_config(valid_config) is False

    def test_missing_default_content_owner_fails(self, valid_config):
        """Missing default_content_owner fails."""
        del valid_config['default_content_owner']
        assert validate_config(valid_config) is False

    def test_missing_default_email_domain_fails(self, valid_config):
        """Missing default_email_domain fails."""
        del valid_config['default_email_domain']
        assert validate_config(valid_config) is False

    def test_empty_access_token_fails(self, valid_config):
        """Empty access_token in source fails."""
        valid_config['source']['access_token'] = ''
        assert validate_config(valid_config) is False

    def test_missing_single_source_field_fails(self, valid_config):
        """Missing a single required source field fails."""
        del valid_config['source']['server_url']
        assert validate_config(valid_config) is False

    def test_empty_site_content_url_is_valid(self, valid_config):
        """Empty string for site_content_url is valid (means default site)."""
        valid_config['source']['site_content_url'] = ''
        assert validate_config(valid_config) is True

    def test_missing_site_content_url_fails(self, valid_config):
        """Missing site_content_url key entirely still fails."""
        del valid_config['source']['site_content_url']
        assert validate_config(valid_config) is False
