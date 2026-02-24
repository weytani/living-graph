# ABOUTME: Tests for the validation scanner.
# ABOUTME: Creates intentionally invalid Test/ pages and verifies the scanner catches issues.

import time
from living_graph.validation import ValidationScanner, Issue


def test_missing_required_attrs(roam, test_namespace):
    roam.create_page("Test/Validation Person")
    time.sleep(2)
    scanner = ValidationScanner(roam)
    issues = scanner.validate_page("Test/Validation Person", type_name="Person")
    attr_issues = [i for i in issues if i.kind == "missing_attr"]
    assert len(attr_issues) >= 2
    attr_names = {i.detail for i in attr_issues}
    assert "Role" in attr_names
    assert "Related" in attr_names


def test_valid_page_no_issues(roam, test_namespace):
    roam.create_page("Test/Validation Good Person")
    time.sleep(2)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Validation Good Person"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]
    roam.create_block(page_uid, "Role:: Engineer", 0)
    roam.create_block(page_uid, "Related:: [[Test/Something]]", 1)
    time.sleep(2)
    scanner = ValidationScanner(roam)
    issues = scanner.validate_page("Test/Validation Good Person", type_name="Person")
    attr_issues = [i for i in issues if i.kind == "missing_attr"]
    assert len(attr_issues) == 0


def test_invalid_status(roam, test_namespace):
    roam.create_page("Test/Validation Bad Project")
    time.sleep(2)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Validation Bad Project"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]
    roam.create_block(page_uid, "Track:: Professional", 0)
    roam.create_block(page_uid, "Status:: banana", 1)
    roam.create_block(page_uid, "Related:: [[Test/Something]]", 2)
    time.sleep(2)
    scanner = ValidationScanner(roam)
    issues = scanner.validate_page("Test/Validation Bad Project", type_name="Project")
    status_issues = [i for i in issues if i.kind == "invalid_status"]
    assert len(status_issues) == 1
    assert "banana" in status_issues[0].detail


def test_scan_namespace(roam, test_namespace):
    roam.create_page("Test/ScanNS Person A")
    roam.create_page("Test/ScanNS Person B")
    time.sleep(2)
    scanner = ValidationScanner(roam)
    report = scanner.scan_namespace("Test/ScanNS ", type_name="Person")
    assert len(report) == 2
    for title, issues in report.items():
        assert any(i.kind == "missing_attr" for i in issues)


def test_stub_detection(roam, test_namespace):
    roam.create_page("Test/Stub Page")
    time.sleep(2)
    scanner = ValidationScanner(roam)
    issues = scanner.validate_page("Test/Stub Page", type_name="Person")
    stub_issues = [i for i in issues if i.kind == "stub"]
    assert len(stub_issues) == 1


def test_issue_severity(roam, test_namespace):
    roam.create_page("Test/Severity Check")
    time.sleep(2)
    scanner = ValidationScanner(roam)
    issues = scanner.validate_page("Test/Severity Check", type_name="Person")
    for issue in issues:
        assert issue.severity in ("critical", "warning", "info")


def test_broken_link_detection(roam, test_namespace):
    """Should detect Related:: links pointing to deleted pages."""
    # Create both pages, link them, then delete the target
    roam.create_page("Test/Broken Link Page")
    roam.create_page("Test/Link Will Be Deleted")
    time.sleep(2)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Broken Link Page"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]
    roam.create_block(page_uid, "Role:: Engineer", 0)
    roam.create_block(page_uid, "Related:: [[Test/Link Will Be Deleted]]", 1)
    time.sleep(2)

    # Now delete the target page, creating a broken link
    target = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Link Will Be Deleted"] [?p :block/uid ?uid]]'
    )
    roam.delete_page(target[0][0])
    time.sleep(3)

    scanner = ValidationScanner(roam)
    issues = scanner.validate_page("Test/Broken Link Page", type_name="Person")
    broken = [i for i in issues if i.kind == "broken_link"]
    assert len(broken) == 1
    assert "bare text" in broken[0].detail


def test_broken_link_ignores_valid_links(roam, test_namespace):
    """Should not flag Related:: links that point to real pages."""
    roam.create_page("Test/Link Target")
    roam.create_page("Test/Link Source")
    time.sleep(2)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Link Source"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]
    roam.create_block(page_uid, "Role:: Tester", 0)
    roam.create_block(page_uid, "Related:: [[Test/Link Target]]", 1)
    time.sleep(2)

    scanner = ValidationScanner(roam)
    issues = scanner.validate_page("Test/Link Source", type_name="Person")
    broken = [i for i in issues if i.kind == "broken_link"]
    assert len(broken) == 0


def test_orphan_detection(roam, test_namespace):
    """Should detect pages with no incoming references from other pages."""
    roam.create_page("Test/Orphan Page")
    time.sleep(2)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Orphan Page"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]
    roam.create_block(page_uid, "Role:: Nobody", 0)
    roam.create_block(page_uid, "Related:: [[Test/Something]]", 1)
    time.sleep(2)

    scanner = ValidationScanner(roam)
    issues = scanner.validate_page("Test/Orphan Page", type_name="Person")
    orphan_issues = [i for i in issues if i.kind == "orphan"]
    assert len(orphan_issues) == 1
