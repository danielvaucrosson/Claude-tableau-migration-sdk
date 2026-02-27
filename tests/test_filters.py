"""Tests for content filter classes."""

import pytest
from unittest.mock import MagicMock

from migration_utils import (
    SkipUserMigration,
    SkipGroupMigration,
    SkipProjectMigration,
    SkipDataSourceMigration,
    SkipWorkbookMigration,
)


class TestAlwaysSkipFilters:
    """All skip filters should always return False."""

    @pytest.mark.parametrize("filter_class", [
        SkipUserMigration,
        SkipGroupMigration,
        SkipProjectMigration,
        SkipDataSourceMigration,
        SkipWorkbookMigration,
    ])
    def test_should_migrate_returns_false(self, filter_class):
        """Every skip filter returns False for any item."""
        filter_instance = filter_class()
        mock_item = MagicMock()
        assert filter_instance.should_migrate(mock_item) is False
