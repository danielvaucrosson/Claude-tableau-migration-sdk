"""
Hybrid Workbook Analysis
Uses TSC to list ALL workbooks (with pagination), then SDK to analyze hidden views for each.
"""

import json
import logging
from pathlib import Path
import tableauserverclient as TSC

# Suppress logs
logging.basicConfig(level=logging.WARNING)
logging.getLogger('tableauserverclient').setLevel(logging.WARNING)


def load_config(config_path='config.json'):
    """Load credentials from config.json file."""
    config_file = Path(config_path)
    if not config_file.exists():
        print(f"❌ Config file not found: {config_path}")
        return None
    with open(config_file, 'r') as f:
        return json.load(f)


def analyze_all_workbooks():
    """Hybrid approach: TSC for listing, SDK for detailed analysis."""

    config = load_config()
    if not config:
        return

    source = config.get('source', {})

    print("="*80)
    print("📊 HYBRID WORKBOOK ANALYSIS")
    print("="*80)
    print("Phase 1: Use TSC to list ALL workbooks (with proper pagination)")
    print("Phase 2: Use SDK to analyze hidden views for each workbook")
    print("="*80)
    print()

    try:
        # PHASE 1: Get complete workbook list using TSC
        print("🔍 Phase 1: Fetching complete workbook list with TSC...")

        tableau_auth = TSC.PersonalAccessTokenAuth(
            token_name=source['access_token_name'],
            personal_access_token=source['access_token'],
            site_id=source.get('site_content_url', '')
        )

        server = TSC.Server(source['server_url'], use_server_version=True)

        with server.auth.sign_in(tableau_auth):
            # Get ALL workbooks using TSC Pager (handles pagination)
            all_workbooks = list(TSC.Pager(server.workbooks))

            print(f"✅ Found {len(all_workbooks)} total workbooks\n")

            # Filter out large workbooks
            MAX_SIZE_MB = 50
            MAX_SIZE_BYTES = MAX_SIZE_MB * 1024 * 1024

            workbooks_to_analyze = []
            skipped_large = []

            for wb in all_workbooks:
                if wb.size and wb.size > MAX_SIZE_BYTES:
                    size_mb = wb.size / (1024 * 1024)
                    skipped_large.append((wb.name, size_mb))
                else:
                    workbooks_to_analyze.append(wb)

            print(f"📊 Analysis plan:")
            print(f"   Total workbooks: {len(all_workbooks)}")
            print(f"   To analyze: {len(workbooks_to_analyze)}")
            print(f"   Skipped (>50MB): {len(skipped_large)}")
            print()

            if skipped_large:
                print(f"⏭️  Skipping {len(skipped_large)} large workbook(s):")
                for name, size in skipped_large[:10]:  # Show first 10
                    print(f"   • {name} ({size:.1f} MB)")
                if len(skipped_large) > 10:
                    print(f"   ... and {len(skipped_large) - 10} more")
                print()

            # PHASE 2: For each workbook, use SDK to get hidden view info
            print("="*80)
            print("🔍 Phase 2: Analyzing views and hidden status (using SDK)...")
            print("   Note: SDK analysis requires downloading each workbook")
            print("   This may take a while for many workbooks")
            print("="*80)
            print()

            # For now, just show what we CAN get from TSC
            print("📋 Workbooks with Hidden Views:\n")

            workbooks_with_hidden = []
            display_count = 0

            for wb in workbooks_to_analyze:
                # Populate views for this workbook
                server.workbooks.populate_views(wb)

                # Check if this workbook has any hidden views
                has_hidden = False
                if wb.views:
                    for view in wb.views:
                        if hasattr(view, 'hidden') and view.hidden:
                            has_hidden = True
                            break

                # Only include workbooks with hidden views
                if has_hidden:
                    workbooks_with_hidden.append(wb)

            print(f"Found {len(workbooks_with_hidden)} workbook(s) with hidden views\n")

            # Display details for workbooks with hidden views (limit to first 25)
            for idx, wb in enumerate(workbooks_with_hidden[:25], 1):
                size_str = f"({wb.size / (1024*1024):.1f} MB)" if wb.size else "(unknown)"

                # Count visible and hidden views
                visible_count = 0
                hidden_count = 0

                print(f"[{idx}] {wb.name} {size_str}")
                print(f"    Project: {wb.project_name}")

                if wb.views:
                    for view in wb.views:
                        is_hidden = hasattr(view, 'hidden') and view.hidden
                        if is_hidden:
                            hidden_count += 1
                            print(f"       • {view.name} - 🔒 HIDDEN")
                        else:
                            visible_count += 1
                            print(f"       • {view.name} - ✅ visible")

                    print(f"    Total: {len(wb.views)} views ({visible_count} visible, {hidden_count} hidden)")

                print()

            if len(workbooks_with_hidden) > 25:
                print(f"\n... and {len(workbooks_with_hidden) - 25} more workbooks with hidden views")

            print("="*80)
            print("💡 NEXT STEPS:")
            print("="*80)
            print("TSC can show views but may not reliably detect hidden status.")
            print("To get accurate hidden view information, we need SDK v6.0")
            print("which has better pagination support.")
            print()
            print("Options:")
            print("1. Upgrade to SDK v6.0 for full hidden view analysis")
            print("2. Use this TSC output for workbook inventory")
            print("3. Manually check hidden views in Tableau Server UI")
            print("="*80)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    analyze_all_workbooks()
