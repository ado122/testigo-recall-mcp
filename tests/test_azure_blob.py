"""Comprehensive tests for Azure Blob Storage backend and source collection.

Tests cover:
- _sync_db_from_azure_blob: downloading .db files from Azure Blob Storage
- _collect_sources: collecting DBs from local, GitHub, Azure, and mixed sources
- Edge cases: network failures, invalid SAS, empty containers, cache fallback
- Existing GitHub backend: verify it still works unchanged
"""

from __future__ import annotations

import json
import os
import sqlite3
import textwrap
from pathlib import Path
from unittest import mock
from urllib.error import HTTPError, URLError

import pytest

from testigo_recall_mcp.server import (
    _collect_sources,
    _get_azure_bearer_token,
    _sync_db_from_azure_blob,
    _sync_db_from_github,
)
from testigo_recall_mcp.storage.db import _SCHEMA


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_test_db(path: Path) -> Path:
    """Create a minimal valid testigo-recall .db file at the given path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.executescript(_SCHEMA)
    conn.execute(
        "INSERT INTO pr_analyses (pr_id, repo, timestamp, files, kinds) "
        "VALUES (?, ?, ?, ?, ?)",
        ("SCAN:test", "test-repo", "2025-01-01T00:00:00Z", "[]", "[]"),
    )
    conn.execute(
        "INSERT INTO facts (pr_id, repo, category, summary, detail, confidence, source, source_files, symbols) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("SCAN:test", "test-repo", "behavior", "test fact summary", "test fact detail", 0.95, "scan", "[]", "[]"),
    )
    conn.commit()
    conn.close()
    return path


def _azure_list_xml(*blob_names: str) -> bytes:
    """Build Azure Blob Storage List Blobs XML response."""
    blobs_xml = ""
    for name in blob_names:
        blobs_xml += f"<Blob><Name>{name}</Name><Properties><Content-Length>4096</Content-Length></Properties></Blob>"
    return textwrap.dedent(f"""\
        <?xml version="1.0" encoding="utf-8"?>
        <EnumerationResults ServiceEndpoint="https://testacct.blob.core.windows.net/" ContainerName="knowledge-base">
          <Blobs>{blobs_xml}</Blobs>
          <NextMarker/>
        </EnumerationResults>""").encode("utf-8")


def _github_release_json(*asset_names: str) -> bytes:
    """Build GitHub release API response with given asset names."""
    assets = []
    for i, name in enumerate(asset_names):
        assets.append({
            "name": name,
            "url": f"https://api.github.com/repos/owner/repo/releases/assets/{1000 + i}",
            "size": 4096,
        })
    return json.dumps({"tag_name": "knowledge-base", "assets": assets}).encode("utf-8")


class MockResponse:
    """Mock urllib response with read() and context manager support."""

    def __init__(self, data: bytes, status: int = 200):
        self._data = data
        self.status = status

    def read(self) -> bytes:
        return self._data

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


# ---------------------------------------------------------------------------
# _sync_db_from_azure_blob tests
# ---------------------------------------------------------------------------

class TestSyncDbFromAzureBlob:
    """Tests for _sync_db_from_azure_blob()."""

    def test_downloads_single_db(self, tmp_path: Path):
        """Successfully downloads a single .db blob."""
        db_content = _make_test_db(tmp_path / "source.db").read_bytes()
        list_xml = _azure_list_xml("my-repo.db")

        call_count = 0
        def mock_urlopen(req, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # List blobs request
                assert "restype=container&comp=list" in req.full_url
                return MockResponse(list_xml)
            else:
                # Download blob request
                assert "my-repo.db" in req.full_url
                return MockResponse(db_content)

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            result = _sync_db_from_azure_blob(
                "https://testacct.blob.core.windows.net/knowledge-base",
                "sv=2023-01-03&se=2030-01-01&sp=rl&sig=abc123",
                cache_dir,
            )

        assert len(result) == 1
        assert result[0].name == "my-repo.db"
        assert result[0].exists()
        # Verify it's a valid SQLite DB
        conn = sqlite3.connect(str(result[0]))
        count = conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
        conn.close()
        assert count > 0

    def test_downloads_multiple_dbs(self, tmp_path: Path):
        """Successfully downloads multiple .db blobs from one container."""
        db1_content = _make_test_db(tmp_path / "src1.db").read_bytes()
        db2_content = _make_test_db(tmp_path / "src2.db").read_bytes()
        list_xml = _azure_list_xml("repo-a.db", "repo-b.db")

        call_count = 0
        def mock_urlopen(req, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return MockResponse(list_xml)
            elif call_count == 2:
                return MockResponse(db1_content)
            else:
                return MockResponse(db2_content)

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            result = _sync_db_from_azure_blob(
                "https://testacct.blob.core.windows.net/knowledge-base",
                "sv=2023&sp=rl&sig=xyz",
                cache_dir,
            )

        assert len(result) == 2
        assert {r.name for r in result} == {"repo-a.db", "repo-b.db"}

    def test_filters_non_db_blobs(self, tmp_path: Path):
        """Only downloads .db files, ignores other blobs."""
        db_content = _make_test_db(tmp_path / "source.db").read_bytes()
        list_xml = _azure_list_xml("readme.md", "data.csv", "my-repo.db", "notes.txt")

        call_count = 0
        def mock_urlopen(req, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return MockResponse(list_xml)
            else:
                return MockResponse(db_content)

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            result = _sync_db_from_azure_blob(
                "https://testacct.blob.core.windows.net/kb",
                "sv=2023&sp=rl&sig=xyz",
                cache_dir,
            )

        assert len(result) == 1
        assert result[0].name == "my-repo.db"

    def test_empty_container(self, tmp_path: Path):
        """Returns empty list when container has no .db blobs."""
        list_xml = _azure_list_xml()  # no blobs

        def mock_urlopen(req, timeout=None):
            return MockResponse(list_xml)

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            result = _sync_db_from_azure_blob(
                "https://testacct.blob.core.windows.net/kb",
                "sv=2023&sp=rl&sig=xyz",
                cache_dir,
            )

        assert result == []

    def test_container_only_non_db_files(self, tmp_path: Path):
        """Returns empty list when container has files but none are .db."""
        list_xml = _azure_list_xml("readme.md", "config.json")

        def mock_urlopen(req, timeout=None):
            return MockResponse(list_xml)

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            result = _sync_db_from_azure_blob(
                "https://testacct.blob.core.windows.net/kb",
                "sv=2023&sp=rl&sig=xyz",
                cache_dir,
            )

        assert result == []

    def test_http_403_forbidden(self, tmp_path: Path):
        """Returns empty list on 403 (bad SAS token or expired)."""
        def mock_urlopen(req, timeout=None):
            raise HTTPError(req.full_url, 403, "Forbidden", {}, None)

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            result = _sync_db_from_azure_blob(
                "https://testacct.blob.core.windows.net/kb",
                "sv=2023&sp=rl&sig=EXPIRED",
                cache_dir,
            )

        assert result == []

    def test_http_404_not_found(self, tmp_path: Path):
        """Returns empty list on 404 (container doesn't exist)."""
        def mock_urlopen(req, timeout=None):
            raise HTTPError(req.full_url, 404, "Not Found", {}, None)

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            result = _sync_db_from_azure_blob(
                "https://testacct.blob.core.windows.net/nonexistent",
                "sv=2023&sp=rl&sig=xyz",
                cache_dir,
            )

        assert result == []

    def test_network_timeout(self, tmp_path: Path):
        """Returns empty list on network timeout."""
        def mock_urlopen(req, timeout=None):
            raise TimeoutError("Connection timed out")

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            result = _sync_db_from_azure_blob(
                "https://testacct.blob.core.windows.net/kb",
                "sv=2023&sp=rl&sig=xyz",
                cache_dir,
            )

        assert result == []

    def test_url_error_dns_failure(self, tmp_path: Path):
        """Returns empty list on DNS resolution failure."""
        def mock_urlopen(req, timeout=None):
            raise URLError("Name or service not known")

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            result = _sync_db_from_azure_blob(
                "https://nonexistent-account.blob.core.windows.net/kb",
                "sv=2023&sp=rl&sig=xyz",
                cache_dir,
            )

        assert result == []

    def test_invalid_xml_response(self, tmp_path: Path):
        """Returns empty list when Azure returns invalid XML."""
        def mock_urlopen(req, timeout=None):
            return MockResponse(b"<html><body>Not XML</body>")

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            result = _sync_db_from_azure_blob(
                "https://testacct.blob.core.windows.net/kb",
                "sv=2023&sp=rl&sig=xyz",
                cache_dir,
            )

        # Should not crash — returns empty or whatever it could parse
        assert isinstance(result, list)

    def test_partial_download_failure(self, tmp_path: Path):
        """Downloads what it can, skips failed blobs."""
        db_content = _make_test_db(tmp_path / "source.db").read_bytes()
        list_xml = _azure_list_xml("good.db", "bad.db", "also-good.db")

        call_count = 0
        def mock_urlopen(req, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return MockResponse(list_xml)  # list response
            elif call_count == 2:
                return MockResponse(db_content)  # good.db
            elif call_count == 3:
                raise HTTPError(req.full_url, 500, "Server Error", {}, None)  # bad.db
            else:
                return MockResponse(db_content)  # also-good.db

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            result = _sync_db_from_azure_blob(
                "https://testacct.blob.core.windows.net/kb",
                "sv=2023&sp=rl&sig=xyz",
                cache_dir,
            )

        assert len(result) == 2
        assert {r.name for r in result} == {"good.db", "also-good.db"}

    def test_sas_token_with_leading_question_mark(self, tmp_path: Path):
        """Handles SAS token with or without leading '?'."""
        db_content = _make_test_db(tmp_path / "source.db").read_bytes()
        list_xml = _azure_list_xml("test.db")

        urls_seen = []
        call_count = 0
        def mock_urlopen(req, timeout=None):
            nonlocal call_count
            urls_seen.append(req.full_url)
            call_count += 1
            if call_count == 1:
                return MockResponse(list_xml)
            else:
                return MockResponse(db_content)

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            _sync_db_from_azure_blob(
                "https://testacct.blob.core.windows.net/kb",
                "?sv=2023&sp=rl&sig=xyz",  # leading '?' should be stripped
                cache_dir,
            )

        # Verify no double '?' in URLs
        for url in urls_seen:
            assert "??" not in url

    def test_url_with_trailing_slash(self, tmp_path: Path):
        """Handles container URL with trailing slash."""
        db_content = _make_test_db(tmp_path / "source.db").read_bytes()
        list_xml = _azure_list_xml("test.db")

        urls_seen = []
        call_count = 0
        def mock_urlopen(req, timeout=None):
            nonlocal call_count
            urls_seen.append(req.full_url)
            call_count += 1
            if call_count == 1:
                return MockResponse(list_xml)
            else:
                return MockResponse(db_content)

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            _sync_db_from_azure_blob(
                "https://testacct.blob.core.windows.net/kb/",  # trailing slash
                "sv=2023&sp=rl&sig=xyz",
                cache_dir,
            )

        # Download URL should be properly formed (no double slash before blob name)
        download_url = urls_seen[1]
        assert "//test.db" not in download_url
        assert "/test.db?" in download_url


# ---------------------------------------------------------------------------
# _get_azure_bearer_token tests
# ---------------------------------------------------------------------------

class TestGetAzureBearerToken:
    """Tests for _get_azure_bearer_token() — az CLI auth."""

    def test_returns_token_on_success(self):
        """Returns bearer token when az CLI is logged in."""
        az_output = json.dumps({
            "accessToken": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.fake_token",
            "expiresOn": "2030-01-01 00:00:00",
            "tokenType": "Bearer",
        })
        mock_result = mock.Mock(returncode=0, stdout=az_output, stderr="")

        with mock.patch("testigo_recall_mcp.server.subprocess.run", return_value=mock_result) as mock_run:
            token = _get_azure_bearer_token()

        assert token == "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.fake_token"
        # Verify correct az CLI command
        args = mock_run.call_args[0][0]
        assert "az" in args
        assert "get-access-token" in args
        assert "https://storage.azure.com" in args

    def test_returns_none_when_not_logged_in(self):
        """Returns None when az login session expired or not logged in."""
        mock_result = mock.Mock(returncode=1, stdout="", stderr="Please run 'az login'")

        with mock.patch("testigo_recall_mcp.server.subprocess.run", return_value=mock_result):
            token = _get_azure_bearer_token()

        assert token is None

    def test_returns_none_when_az_not_installed(self):
        """Returns None when az CLI is not installed."""
        with mock.patch("testigo_recall_mcp.server.subprocess.run", side_effect=FileNotFoundError):
            token = _get_azure_bearer_token()

        assert token is None

    def test_returns_none_on_timeout(self):
        """Returns None when az CLI hangs."""
        import subprocess as sp
        with mock.patch("testigo_recall_mcp.server.subprocess.run", side_effect=sp.TimeoutExpired("az", 15)):
            token = _get_azure_bearer_token()

        assert token is None

    def test_returns_none_on_invalid_json(self):
        """Returns None when az CLI returns garbage."""
        mock_result = mock.Mock(returncode=0, stdout="not json", stderr="")

        with mock.patch("testigo_recall_mcp.server.subprocess.run", return_value=mock_result):
            token = _get_azure_bearer_token()

        assert token is None


class TestAzureBlobAuthPriority:
    """Tests for auth priority: SAS > az CLI bearer > none."""

    def test_sas_token_takes_priority_over_cli(self, tmp_path: Path):
        """When SAS token is provided, az CLI is NOT called."""
        db_content = _make_test_db(tmp_path / "source.db").read_bytes()
        list_xml = _azure_list_xml("test.db")

        call_count = 0
        def mock_urlopen(req, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # List request should have SAS in URL, no Bearer header
                assert "sv=2023" in req.full_url
                assert req.get_header("Authorization") is None
                return MockResponse(list_xml)
            else:
                return MockResponse(db_content)

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            with mock.patch("testigo_recall_mcp.server._get_azure_bearer_token") as mock_bearer:
                result = _sync_db_from_azure_blob(
                    "https://testacct.blob.core.windows.net/kb",
                    "sv=2023&sp=rl&sig=xyz",
                    cache_dir,
                )

        assert len(result) == 1
        # Bearer token should NOT have been requested
        mock_bearer.assert_not_called()

    def test_falls_back_to_cli_when_no_sas(self, tmp_path: Path):
        """When no SAS token, uses pre-fetched az CLI bearer token."""
        db_content = _make_test_db(tmp_path / "source.db").read_bytes()
        list_xml = _azure_list_xml("test.db")

        call_count = 0
        def mock_urlopen(req, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Should have Bearer header, no SAS in URL
                assert req.get_header("Authorization") == "Bearer fake-bearer-token"
                assert req.get_header("X-ms-version") == "2020-10-02"
                return MockResponse(list_xml)
            else:
                assert req.get_header("Authorization") == "Bearer fake-bearer-token"
                return MockResponse(db_content)

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            result = _sync_db_from_azure_blob(
                "https://testacct.blob.core.windows.net/kb",
                "",  # no SAS
                cache_dir,
                bearer_token="fake-bearer-token",  # pre-fetched by _collect_sources
            )

        assert len(result) == 1

    def test_no_auth_when_no_sas_and_no_cli(self, tmp_path: Path):
        """When no SAS and no az CLI, makes unauthenticated requests (public container)."""
        db_content = _make_test_db(tmp_path / "source.db").read_bytes()
        list_xml = _azure_list_xml("test.db")

        call_count = 0
        def mock_urlopen(req, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # No auth at all
                assert req.get_header("Authorization") is None
                return MockResponse(list_xml)
            else:
                return MockResponse(db_content)

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            with mock.patch("testigo_recall_mcp.server._get_azure_bearer_token", return_value=None):
                result = _sync_db_from_azure_blob(
                    "https://testacct.blob.core.windows.net/public-kb",
                    "",  # no SAS
                    cache_dir,
                )

        assert len(result) == 1


# ---------------------------------------------------------------------------
# _sync_db_from_github tests (existing backend — verify no regression)
# ---------------------------------------------------------------------------

class TestSyncDbFromGitHub:
    """Tests for _sync_db_from_github() — verify existing backend still works."""

    def test_downloads_single_db(self, tmp_path: Path):
        """Successfully downloads a single .db asset from GitHub release."""
        db_content = _make_test_db(tmp_path / "source.db").read_bytes()
        release_json = _github_release_json("my-repo.db")

        call_count = 0
        def mock_urlopen(req, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return MockResponse(release_json)
            else:
                return MockResponse(db_content)

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            result = _sync_db_from_github("owner/repo", cache_dir)

        assert len(result) == 1
        assert result[0].name == "my-repo.db"
        assert result[0].exists()

    def test_downloads_multiple_assets(self, tmp_path: Path):
        """Downloads all .db assets from a release."""
        db_content = _make_test_db(tmp_path / "source.db").read_bytes()
        release_json = _github_release_json("repo-a.db", "repo-b.db")

        call_count = 0
        def mock_urlopen(req, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return MockResponse(release_json)
            else:
                return MockResponse(db_content)

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            result = _sync_db_from_github("owner/repo", cache_dir)

        assert len(result) == 2

    def test_filters_non_db_assets(self, tmp_path: Path):
        """Only downloads .db assets, ignores others."""
        db_content = _make_test_db(tmp_path / "source.db").read_bytes()
        release_json = _github_release_json("readme.md", "my-repo.db", "notes.txt")

        call_count = 0
        def mock_urlopen(req, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return MockResponse(release_json)
            else:
                return MockResponse(db_content)

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            result = _sync_db_from_github("owner/repo", cache_dir)

        assert len(result) == 1
        assert result[0].name == "my-repo.db"

    def test_http_error_returns_empty(self, tmp_path: Path):
        """Returns empty list on HTTP error (404, 403, etc.)."""
        def mock_urlopen(req, timeout=None):
            raise HTTPError(req.full_url, 404, "Not Found", {}, None)

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            result = _sync_db_from_github("owner/repo", cache_dir)

        assert result == []

    def test_uses_github_token(self, tmp_path: Path):
        """Uses GITHUB_TOKEN env var for auth header."""
        release_json = _github_release_json()

        headers_seen = {}
        def mock_urlopen(req, timeout=None):
            headers_seen.update(dict(req.headers))
            return MockResponse(release_json)

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            with mock.patch.dict(os.environ, {"GITHUB_TOKEN": "ghp_test123"}, clear=False):
                _sync_db_from_github("owner/repo", cache_dir)

        assert headers_seen.get("Authorization") == "token ghp_test123"

    def test_uses_gh_token_fallback(self, tmp_path: Path):
        """Falls back to GH_TOKEN if GITHUB_TOKEN not set."""
        release_json = _github_release_json()

        headers_seen = {}
        def mock_urlopen(req, timeout=None):
            headers_seen.update(dict(req.headers))
            return MockResponse(release_json)

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        env = {"GH_TOKEN": "ghp_fallback456"}
        # Remove GITHUB_TOKEN if it exists
        env_clean = {k: v for k, v in os.environ.items() if k not in ("GITHUB_TOKEN", "GH_TOKEN")}
        env_clean["GH_TOKEN"] = "ghp_fallback456"

        with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
            with mock.patch.dict(os.environ, env_clean, clear=True):
                _sync_db_from_github("owner/repo", cache_dir)

        assert headers_seen.get("Authorization") == "token ghp_fallback456"


# ---------------------------------------------------------------------------
# _collect_sources tests
# ---------------------------------------------------------------------------

class TestCollectSources:
    """Tests for _collect_sources() — all backends and combinations."""

    def _clean_env(self):
        """Return env dict with all testigo-recall vars removed."""
        return {k: v for k, v in os.environ.items()
                if k not in (
                    "TESTIGO_RECALL_DB_PATH", "PR_IMPACT_DB_PATH",
                    "TESTIGO_RECALL_REPO", "PR_IMPACT_REPO",
                    "TESTIGO_RECALL_AZURE_URL", "TESTIGO_RECALL_AZURE_SAS",
                    "GITHUB_TOKEN", "GH_TOKEN",
                )}

    def test_local_db_path(self, tmp_path: Path):
        """Picks up local DB from TESTIGO_RECALL_DB_PATH."""
        db_path = _make_test_db(tmp_path / "local.db")
        env = self._clean_env()
        env["TESTIGO_RECALL_DB_PATH"] = str(db_path)

        with mock.patch.dict(os.environ, env, clear=True):
            sources = _collect_sources()

        assert len(sources) == 1
        assert sources[0] == db_path

    def test_multiple_local_paths(self, tmp_path: Path):
        """Handles comma-separated local DB paths."""
        db1 = _make_test_db(tmp_path / "a.db")
        db2 = _make_test_db(tmp_path / "b.db")
        env = self._clean_env()
        env["TESTIGO_RECALL_DB_PATH"] = f"{db1},{db2}"

        with mock.patch.dict(os.environ, env, clear=True):
            sources = _collect_sources()

        assert len(sources) == 2

    def test_local_path_missing_file(self, tmp_path: Path):
        """Skips missing local paths with warning (doesn't crash)."""
        db_path = _make_test_db(tmp_path / "exists.db")
        env = self._clean_env()
        env["TESTIGO_RECALL_DB_PATH"] = f"{db_path},{tmp_path / 'missing.db'}"

        with mock.patch.dict(os.environ, env, clear=True):
            sources = _collect_sources()

        assert len(sources) == 1
        assert sources[0] == db_path

    def test_legacy_pr_impact_db_path(self, tmp_path: Path):
        """Supports legacy PR_IMPACT_DB_PATH env var."""
        db_path = _make_test_db(tmp_path / "legacy.db")
        env = self._clean_env()
        env["PR_IMPACT_DB_PATH"] = str(db_path)

        with mock.patch.dict(os.environ, env, clear=True):
            sources = _collect_sources()

        assert len(sources) == 1

    def test_azure_blob_source(self, tmp_path: Path):
        """Collects DBs from Azure Blob Storage."""
        db_content = _make_test_db(tmp_path / "source.db").read_bytes()
        list_xml = _azure_list_xml("drmax.db")

        call_count = 0
        def mock_urlopen(req, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return MockResponse(list_xml)
            else:
                return MockResponse(db_content)

        env = self._clean_env()
        env["TESTIGO_RECALL_AZURE_URL"] = "https://testacct.blob.core.windows.net/knowledge-base"
        env["TESTIGO_RECALL_AZURE_SAS"] = "sv=2023&sp=rl&sig=xyz"

        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
                sources = _collect_sources()

        assert len(sources) == 1
        assert sources[0].name == "drmax.db"

    def test_azure_cache_fallback(self, tmp_path: Path):
        """Falls back to cached Azure DBs when download fails."""
        # Pre-populate cache
        cache_dir = tmp_path / ".testigo-recall" / "azure--testacct--knowledge-base"
        _make_test_db(cache_dir / "cached-repo.db")

        def mock_urlopen(req, timeout=None):
            raise URLError("Network unreachable")

        env = self._clean_env()
        env["TESTIGO_RECALL_AZURE_URL"] = "https://testacct.blob.core.windows.net/knowledge-base"
        env["TESTIGO_RECALL_AZURE_SAS"] = "sv=2023&sp=rl&sig=xyz"

        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
                with mock.patch("pathlib.Path.home", return_value=tmp_path):
                    sources = _collect_sources()

        assert len(sources) == 1
        assert sources[0].name == "cached-repo.db"

    def test_azure_no_cache_no_network(self, tmp_path: Path):
        """Returns empty when Azure fails and no cache exists."""
        def mock_urlopen(req, timeout=None):
            raise URLError("Network unreachable")

        env = self._clean_env()
        env["TESTIGO_RECALL_AZURE_URL"] = "https://testacct.blob.core.windows.net/kb"
        env["TESTIGO_RECALL_AZURE_SAS"] = "sv=2023&sp=rl&sig=xyz"

        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
                with mock.patch("pathlib.Path.home", return_value=tmp_path):
                    sources = _collect_sources()

        assert sources == []

    def test_azure_multiple_containers(self, tmp_path: Path):
        """Handles comma-separated Azure container URLs."""
        db_content = _make_test_db(tmp_path / "source.db").read_bytes()
        list_xml_1 = _azure_list_xml("repo-a.db")
        list_xml_2 = _azure_list_xml("repo-b.db")

        call_count = 0
        def mock_urlopen(req, timeout=None):
            nonlocal call_count
            call_count += 1
            url = req.full_url
            if "restype=container&comp=list" in url:
                if "container1" in url:
                    return MockResponse(list_xml_1)
                else:
                    return MockResponse(list_xml_2)
            else:
                return MockResponse(db_content)

        env = self._clean_env()
        env["TESTIGO_RECALL_AZURE_URL"] = (
            "https://testacct.blob.core.windows.net/container1,"
            "https://testacct.blob.core.windows.net/container2"
        )
        env["TESTIGO_RECALL_AZURE_SAS"] = "sv=2023&sp=rl&sig=xyz"

        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
                with mock.patch("pathlib.Path.home", return_value=tmp_path):
                    sources = _collect_sources()

        assert len(sources) == 2

    def test_mixed_local_and_azure(self, tmp_path: Path):
        """Combines local DB paths with Azure Blob sources."""
        local_db = _make_test_db(tmp_path / "local.db")
        azure_db_content = _make_test_db(tmp_path / "azure-source.db").read_bytes()
        list_xml = _azure_list_xml("cloud-repo.db")

        call_count = 0
        def mock_urlopen(req, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return MockResponse(list_xml)
            else:
                return MockResponse(azure_db_content)

        env = self._clean_env()
        env["TESTIGO_RECALL_DB_PATH"] = str(local_db)
        env["TESTIGO_RECALL_AZURE_URL"] = "https://testacct.blob.core.windows.net/kb"
        env["TESTIGO_RECALL_AZURE_SAS"] = "sv=2023&sp=rl&sig=xyz"

        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
                with mock.patch("pathlib.Path.home", return_value=tmp_path):
                    sources = _collect_sources()

        assert len(sources) == 2
        names = {s.name for s in sources}
        assert "local.db" in names
        assert "cloud-repo.db" in names

    def test_mixed_github_and_azure(self, tmp_path: Path):
        """Combines GitHub and Azure sources simultaneously."""
        db_content = _make_test_db(tmp_path / "source.db").read_bytes()
        github_json = _github_release_json("github-repo.db")
        azure_xml = _azure_list_xml("azure-repo.db")

        call_count = 0
        def mock_urlopen(req, timeout=None):
            nonlocal call_count
            call_count += 1
            url = req.full_url
            if "api.github.com" in url:
                return MockResponse(github_json)
            elif "restype=container&comp=list" in url:
                return MockResponse(azure_xml)
            else:
                return MockResponse(db_content)

        env = self._clean_env()
        env["TESTIGO_RECALL_REPO"] = "owner/repo"
        env["TESTIGO_RECALL_AZURE_URL"] = "https://testacct.blob.core.windows.net/kb"
        env["TESTIGO_RECALL_AZURE_SAS"] = "sv=2023&sp=rl&sig=xyz"

        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
                with mock.patch("pathlib.Path.home", return_value=tmp_path):
                    sources = _collect_sources()

        assert len(sources) == 2
        names = {s.name for s in sources}
        assert "github-repo.db" in names
        assert "azure-repo.db" in names

    def test_no_env_vars_set(self, tmp_path: Path):
        """Returns empty when no env vars are configured."""
        env = self._clean_env()

        with mock.patch.dict(os.environ, env, clear=True):
            sources = _collect_sources()

        assert sources == []

    def test_azure_without_sas_token(self, tmp_path: Path):
        """Works with empty SAS token (falls back to az CLI or public container)."""
        db_content = _make_test_db(tmp_path / "source.db").read_bytes()
        list_xml = _azure_list_xml("public-repo.db")

        call_count = 0
        def mock_urlopen(req, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # No SAS params in URL
                assert "sig=" not in req.full_url
                return MockResponse(list_xml)
            else:
                return MockResponse(db_content)

        env = self._clean_env()
        env["TESTIGO_RECALL_AZURE_URL"] = "https://testacct.blob.core.windows.net/public-kb"
        # No TESTIGO_RECALL_AZURE_SAS set

        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
                with mock.patch("testigo_recall_mcp.server._get_azure_bearer_token", return_value=None):
                    with mock.patch("pathlib.Path.home", return_value=tmp_path):
                        sources = _collect_sources()

        assert len(sources) == 1

    def test_azure_cache_dir_naming(self, tmp_path: Path):
        """Verify Azure cache dir is derived from URL (account + container)."""
        list_xml = _azure_list_xml()  # empty container

        def mock_urlopen(req, timeout=None):
            return MockResponse(list_xml)

        env = self._clean_env()
        env["TESTIGO_RECALL_AZURE_URL"] = "https://drmaxstorage.blob.core.windows.net/testigo-kb"
        env["TESTIGO_RECALL_AZURE_SAS"] = "sv=2023&sp=rl&sig=xyz"

        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch("testigo_recall_mcp.server.urllib.request.urlopen", side_effect=mock_urlopen):
                with mock.patch("pathlib.Path.home", return_value=tmp_path):
                    _collect_sources()

        # Cache dir should exist with expected name
        expected_cache = tmp_path / ".testigo-recall" / "azure--drmaxstorage--testigo-kb"
        assert expected_cache.exists()


# ---------------------------------------------------------------------------
# Integration: Azure .db files work with Database class
# ---------------------------------------------------------------------------

class TestAzureDbIntegration:
    """Verify that .db files downloaded from Azure work with the Database class."""

    def test_downloaded_db_is_queryable(self, tmp_path: Path):
        """A .db downloaded via Azure can be opened and queried."""
        from testigo_recall_mcp.storage.db import Database

        db_path = _make_test_db(tmp_path / "azure-downloaded.db")
        db = Database(db_path)

        results = db.search("test fact")
        assert len(results) > 0
        assert results[0]["summary"] == "test fact summary"

    def test_downloaded_db_fts_works(self, tmp_path: Path):
        """FTS5 full-text search works on downloaded DBs."""
        from testigo_recall_mcp.storage.db import Database

        db_path = _make_test_db(tmp_path / "fts-test.db")
        db = Database(db_path)

        # FTS should find by keyword
        results = db.search("summary")
        assert len(results) > 0

    def test_multiple_azure_dbs_merge(self, tmp_path: Path):
        """Multiple Azure DBs can be merged without errors."""
        from testigo_recall_mcp.server import _merge_into, _migrate_connection

        db1 = _make_test_db(tmp_path / "repo1.db")
        db2_path = tmp_path / "repo2.db"
        conn2 = sqlite3.connect(str(db2_path))
        conn2.executescript(_SCHEMA)
        conn2.execute(
            "INSERT INTO pr_analyses (pr_id, repo, timestamp, files, kinds) "
            "VALUES (?, ?, ?, ?, ?)",
            ("SCAN:other", "other-repo", "2025-02-01T00:00:00Z", "[]", "[]"),
        )
        conn2.execute(
            "INSERT INTO facts (pr_id, repo, category, summary, detail, confidence, source, source_files, symbols) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("SCAN:other", "other-repo", "design", "other fact", "other detail", 0.9, "scan", "[]", "[]"),
        )
        conn2.commit()
        conn2.close()

        # Merge db2 into db1
        target_path = tmp_path / "merged.db"
        import shutil
        shutil.copy2(db1, target_path)

        conn = sqlite3.connect(str(target_path))
        conn.row_factory = sqlite3.Row
        _migrate_connection(conn)
        conn.commit()
        count = _merge_into(conn, db2_path)
        conn.close()

        assert count == 1  # one fact from db2

        # Verify merged DB has facts from both repos
        from testigo_recall_mcp.storage.db import Database
        merged_db = Database(target_path)
        r1 = merged_db.search("test fact")
        r2 = merged_db.search("other fact")
        assert len(r1) > 0
        assert len(r2) > 0
