"""Tests for migration_scope configuration parsing."""

import pytest

from migration_utils import get_migration_scope, DEFAULT_MIGRATION_SCOPE


class TestGetMigrationScope:
    """Tests for the get_migration_scope function."""

    def test_full_scope_config(self, valid_config):
        """Returns exact values from config when all keys present."""
        scope = get_migration_scope(valid_config)
        assert scope['data_sources'] is True
        assert scope['workbooks'] is True

    def test_scope_with_exclusions(self, valid_config):
        """Respects False values in config."""
        valid_config['migration_scope']['data_sources'] = False
        scope = get_migration_scope(valid_config)
        assert scope['data_sources'] is False
        assert scope['workbooks'] is True

    def test_missing_scope_defaults_to_all_true(self, config_without_scope):
        """Missing migration_scope section defaults to all True."""
        scope = get_migration_scope(config_without_scope)
        assert scope['data_sources'] is True
        assert scope['workbooks'] is True

    def test_partial_scope_fills_defaults(self, valid_config):
        """If only one key is provided, the other defaults to True."""
        valid_config['migration_scope'] = {'data_sources': False}
        scope = get_migration_scope(valid_config)
        assert scope['data_sources'] is False
        assert scope['workbooks'] is True

    def test_empty_scope_object_defaults_to_all_true(self, valid_config):
        """Empty migration_scope object defaults to all True."""
        valid_config['migration_scope'] = {}
        scope = get_migration_scope(valid_config)
        assert scope == DEFAULT_MIGRATION_SCOPE

    def test_all_false(self, valid_config):
        """User can disable everything."""
        valid_config['migration_scope'] = {'data_sources': False, 'workbooks': False}
        scope = get_migration_scope(valid_config)
        assert scope['data_sources'] is False
        assert scope['workbooks'] is False
