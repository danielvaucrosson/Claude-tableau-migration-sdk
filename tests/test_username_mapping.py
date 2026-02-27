"""Tests for username/email mapping logic."""

import pytest

from migration_utils import EmailDomainMapping


class TestEmailDomainMapping:
    """Tests for the EmailDomainMapping class."""

    def test_username_gets_domain_appended(self, mock_mapping_context):
        """A plain username gets the email domain appended."""
        mapping = EmailDomainMapping(email_domain="@test.com")
        ctx = mock_mapping_context("jsmith")

        mapping.map(ctx)

        domain = ctx.mapped_location.parent.return_value
        domain.append.assert_called_once_with("jsmith@test.com")
        ctx.map_to.assert_called_once()

    def test_email_username_passes_through(self, mock_mapping_context):
        """A username that already contains '@' is used as-is."""
        mapping = EmailDomainMapping(email_domain="@test.com")
        ctx = mock_mapping_context("jsmith@existing.com")

        mapping.map(ctx)

        domain = ctx.mapped_location.parent.return_value
        domain.append.assert_called_once_with("jsmith@existing.com")

    def test_different_domain(self, mock_mapping_context):
        """Different email domains are correctly appended."""
        mapping = EmailDomainMapping(email_domain="@bigcorp.org")
        ctx = mock_mapping_context("alice")

        mapping.map(ctx)

        domain = ctx.mapped_location.parent.return_value
        domain.append.assert_called_once_with("alice@bigcorp.org")

    def test_domain_without_at_sign(self, mock_mapping_context):
        """If email_domain lacks leading '@', one is prepended."""
        mapping = EmailDomainMapping(email_domain="example.com")
        ctx = mock_mapping_context("bob")

        mapping.map(ctx)

        domain = ctx.mapped_location.parent.return_value
        domain.append.assert_called_once_with("bob@example.com")

    def test_domain_with_at_sign(self, mock_mapping_context):
        """If email_domain already has '@', it is not doubled."""
        mapping = EmailDomainMapping(email_domain="@example.com")
        ctx = mock_mapping_context("bob")

        mapping.map(ctx)

        domain = ctx.mapped_location.parent.return_value
        domain.append.assert_called_once_with("bob@example.com")

    def test_default_owner_stored(self):
        """The default_owner parameter is stored on the instance."""
        mapping = EmailDomainMapping(
            email_domain="@test.com",
            default_owner="fallback@test.com"
        )
        assert mapping.default_owner == "fallback@test.com"

    def test_default_owner_is_optional(self):
        """The default_owner parameter defaults to None."""
        mapping = EmailDomainMapping(email_domain="@test.com")
        assert mapping.default_owner is None
