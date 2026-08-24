package client

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sep() string { return string(os.PathListSeparator) }

func TestSplitPathList(t *testing.T) {
	assert.Equal(t, []string{"a", "b"}, splitPathList(strings.Join([]string{"a", "", "b"}, sep())))
	assert.Nil(t, splitPathList(""))
}

func TestRemovePathDir(t *testing.T) {
	assert.Equal(t, []string{"a", "c"}, removePathDir([]string{"a", "b", "c", "b"}, "b"))
	// The input slice must not be mutated in place.
	in := []string{"x", "y"}
	_ = removePathDir(in, "x")
	assert.Equal(t, []string{"x", "y"}, in)
}

func TestPrependPathDir(t *testing.T) {
	// A later duplicate is dropped so the prepended dir wins.
	assert.Equal(t, []string{"a", "b", "c"}, prependPathDir([]string{"b", "a", "c"}, "a"))
	assert.Equal(t, []string{"new", "a", "b"}, prependPathDir([]string{"a", "b"}, "new"))
}

func TestFindInPath(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "tool")
	require.NoError(t, os.WriteFile(exe, []byte("#!/bin/sh\n"), 0o755))
	nonExe := filepath.Join(dir, "data")
	require.NoError(t, os.WriteFile(nonExe, []byte("x"), 0o644))

	assert.Equal(t, exe, findInPath([]string{"/nonexistent", dir}, "tool"))
	assert.Empty(t, findInPath([]string{dir}, "missing"))
	// A non-executable file is not a match.
	assert.Empty(t, findInPath([]string{dir}, "data"))
}

func TestReplaceEnvPath(t *testing.T) {
	got := replaceEnvPath([]string{"FOO=bar", "PATH=/old", "BAZ=qux"}, "/new")
	assert.Equal(t, []string{"FOO=bar", "PATH=/new", "BAZ=qux"}, got)
	// PATH is appended when the environment has none.
	assert.Equal(t, []string{"FOO=bar", "PATH=/new"}, replaceEnvPath([]string{"FOO=bar"}, "/new"))
}

func TestNodeDownloadArch(t *testing.T) {
	assert.Equal(t, "x64", nodeDownloadArch("amd64"))
	assert.Equal(t, "arm64", nodeDownloadArch("arm64"))
	assert.Empty(t, nodeDownloadArch("mips"))
}

func TestLatestNodeTarball(t *testing.T) {
	body := "abc123  node-v24.1.0-linux-x64.tar.gz\n" +
		"def456  node-v24.1.0-linux-x64.tar.xz\n" +
		"ghi789  node-v24.1.0-linux-arm64.tar.xz\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	name, err := latestNodeTarball(t.Context(), srv.URL, "x64")
	require.NoError(t, err)
	assert.Equal(t, "node-v24.1.0-linux-x64.tar.xz", name)

	_, err = latestNodeTarball(t.Context(), srv.URL, "ppc64le")
	assert.Error(t, err)
}

func TestClaudeSystemContext(t *testing.T) {
	assert.Contains(t, claudeSystemContext("/Workspace/Users/me@example.com"), "working directory is /Workspace/Users/me@example.com")
	// Falls back to a generic phrase when the workspace home is unknown.
	assert.Contains(t, claudeSystemContext(""), "working directory is the user's Databricks workspace home directory")
}

// gatewayServer serves the Anthropic model listing with the given status/body and
// 404s every other path (e.g. the SDK's /.well-known/databricks-config discovery,
// which then falls back to the static PAT config).
func gatewayServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ai-gateway/anthropic/v1/models" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

func newProbeClient(t *testing.T, host string) *databricks.WorkspaceClient {
	t.Helper()
	w, err := databricks.NewWorkspaceClient((*databricks.Config)(&config.Config{Host: host, Token: "test-token"}))
	require.NoError(t, err)
	return w
}

func TestProbeAIGateway(t *testing.T) {
	t.Run("enabled with a claude model", func(t *testing.T) {
		srv := gatewayServer(t, http.StatusOK, `{"data":[{"id":"databricks-claude-sonnet-4"}]}`)
		defer srv.Close()
		assert.NoError(t, probeAIGateway(t.Context(), newProbeClient(t, srv.URL)))
	})

	t.Run("disabled returns an actionable error", func(t *testing.T) {
		srv := gatewayServer(t, http.StatusForbidden, "")
		defer srv.Close()
		err := probeAIGateway(t.Context(), newProbeClient(t, srv.URL))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not enabled on this workspace")
		assert.Contains(t, err.Error(), "HTTP 403")
	})

	t.Run("enabled but no claude models", func(t *testing.T) {
		srv := gatewayServer(t, http.StatusOK, `{"data":[{"id":"databricks-gpt-4o"}]}`)
		defer srv.Close()
		err := probeAIGateway(t.Context(), newProbeClient(t, srv.URL))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exposes no Claude models")
	})
}
