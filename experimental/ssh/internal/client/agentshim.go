package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/databricks/cli/libs/cmdio"
	"github.com/databricks/databricks-sdk-go"
)

const (
	// ucodeShimDirName is the (home-relative) directory the interactive session
	// drops the `claude` wrapper into. It must match remoteShimDir, which the
	// wrapper preamble expands as "$HOME/.ucode-shim". It is deliberately excluded
	// from the PATH we run ucode under (see bootstrapAndLaunchClaude) so ucode's
	// "is claude installed?" check and its execvp("claude") resolve the real
	// binary rather than looping back into the wrapper.
	ucodeShimDirName = ".ucode-shim"

	// ucodeGitSource is the published ucode build installed when --ucode-source
	// isn't supplied.
	ucodeGitSource = "git+https://github.com/anton-107/ucode@remote-env-token-auth"

	// ucodeSourceEnv, when set in the wrapper's environment (via --ucode-source),
	// is the /Workspace path of a locally-built ucode sdist to force-reinstall
	// from instead of ucodeGitSource.
	ucodeSourceEnv = "DATABRICKS_UCODE_SOURCE"

	// workspaceHomeEnv carries the user's workspace home (/Workspace/Users/<email>)
	// resolved client-side, so the shim can weave it into the agent context without
	// an extra API round trip.
	workspaceHomeEnv = "DATABRICKS_WORKSPACE_HOME"

	// nodeDistDirName holds the Node/npm runtime the shim fetches when the image
	// ships none; it is off PATH by default so a global npm install can't shadow it.
	nodeDistDirName = ".ucode-node"

	// contextFileName is where the shim writes the agent system context that it
	// passes to Claude Code via --append-system-prompt-file.
	contextFileName = ".ucode-claude-context.md"

	// readyFileName marks that first-run setup completed; it stores the resolved
	// PATH so later launches skip the bootstrap and configure steps entirely.
	readyFileName = ".ready"
)

// RunClaudeAgentShim is the entry point for `databricks ssh agent-shim claude`,
// invoked on the serverless driver by the tiny `claude` wrapper that
// `databricks ssh connect` installs on PATH. It probes the workspace's Unity AI
// Gateway first (erroring out early if it isn't enabled), then bootstraps ucode +
// Claude Code (once per session) and launches ucode-configured Claude Code,
// forwarding claudeArgs.
func RunClaudeAgentShim(ctx context.Context, client *databricks.WorkspaceClient, claudeArgs []string) error {
	// Probe at the very beginning: fail with an actionable error before spending
	// time on the (slow, first-run) toolchain bootstrap if the gateway is off.
	if err := probeAIGateway(ctx, client); err != nil {
		return err
	}
	return bootstrapAndLaunchClaude(ctx, claudeArgs)
}

// claudeGatewayModelPrefix identifies a Claude serving endpoint on the AI Gateway.
// ucode discovers Claude families by this same `databricks-claude-*` convention.
const claudeGatewayModelPrefix = "databricks-claude-"

// probeAIGateway checks that this workspace's Unity AI Gateway is enabled and
// exposes at least one Claude model, mirroring ucode's own availability check
// (a GET of the gateway's Anthropic model listing). It returns an actionable
// error otherwise so the user isn't left to decode a later ucode failure.
func probeAIGateway(ctx context.Context, client *databricks.WorkspaceClient) error {
	host := strings.TrimRight(client.Config.Host, "/")
	url := host + "/ai-gateway/anthropic/v1/models"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to build AI Gateway probe request: %w", err)
	}
	if err := client.Config.Authenticate(req); err != nil {
		return fmt.Errorf("failed to authenticate AI Gateway probe: %w", err)
	}

	httpClient := &http.Client{Transport: client.Config.HTTPTransport}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to reach the Unity AI Gateway at %s: %w", host, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("failed to read the Unity AI Gateway response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("the Unity AI Gateway is not enabled on this workspace (%s): its model listing returned HTTP %d. Enable the AI Gateway (Foundation Model APIs) for this workspace and try again", host, resp.StatusCode)
	}

	var payload struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return fmt.Errorf("failed to parse the Unity AI Gateway model listing: %w", err)
	}
	for _, m := range payload.Data {
		if strings.Contains(m.ID, claudeGatewayModelPrefix) {
			return nil
		}
	}
	return fmt.Errorf("the Unity AI Gateway on %s exposes no Claude models (expected a %q serving endpoint); ask your workspace admin to enable Anthropic models on the gateway", host, claudeGatewayModelPrefix+"*")
}

// bootstrapAndLaunchClaude installs the toolchain (first run only) and launches
// ucode-configured Claude Code, replacing the current process. On repeat launches
// it restores the PATH recorded by the first run and skips straight to the launch.
func bootstrapAndLaunchClaude(ctx context.Context, claudeArgs []string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to resolve home directory: %w", err)
	}
	shimDir := filepath.Join(home, ucodeShimDirName)
	readyFile := filepath.Join(shimDir, readyFileName)
	contextFile := filepath.Join(home, contextFileName)

	// Fast path: first-run setup recorded the resolved PATH; restore it and launch.
	if data, err := os.ReadFile(readyFile); err == nil {
		return launchUcodeClaude(strings.TrimSpace(string(data)), contextFile, claudeArgs)
	}

	// Build the PATH we run tooling (and ultimately ucode) under: the session PATH
	// with the shim dir removed and ucode's install bin prepended. Removing the
	// shim dir is what lets ucode install and exec the real claude instead of
	// finding this wrapper — no recursion guard needed.
	pathDirs := removePathDir(splitPathList(os.Getenv("PATH")), shimDir)
	pathDirs = prependPathDir(pathDirs, filepath.Join(home, ".local", "bin"))

	// 1. uv (installs into ~/.local/bin).
	if findInPath(pathDirs, "uv") == "" {
		cmdio.LogString(ctx, "Installing uv...")
		if err := runShell(ctx, pathDirs, "curl -LsSf https://astral.sh/uv/install.sh | sh"); err != nil {
			return fmt.Errorf("failed to install uv: %w", err)
		}
	}

	// 2. ucode. --ucode-source force-reinstalls from the uploaded sdist so a local
	// branch takes effect; otherwise install the published build once.
	if source := os.Getenv(ucodeSourceEnv); source != "" {
		cmdio.LogString(ctx, "Installing ucode from "+source+"...")
		if err := runCommand(ctx, pathDirs, "uv", "tool", "install", "--reinstall", source); err != nil {
			return fmt.Errorf("failed to install ucode from %s: %w", source, err)
		}
	} else if findInPath(pathDirs, "ucode") == "" {
		cmdio.LogString(ctx, "Installing ucode...")
		if err := runCommand(ctx, pathDirs, "uv", "tool", "install", ucodeGitSource); err != nil {
			return fmt.Errorf("failed to install ucode: %w", err)
		}
	}

	// 3. Node/npm. Claude Code needs it and serverless images don't ship it.
	if findInPath(pathDirs, "npm") == "" {
		cmdio.LogString(ctx, "Installing Node.js...")
		nodeBin, err := ensureNode(ctx, home, pathDirs)
		if err != nil {
			return err
		}
		pathDirs = prependPathDir(pathDirs, nodeBin)
	}

	// 4. Put npm's global bin on PATH so the npm-installed claude resolves after
	// ucode installs it.
	if prefix := npmGlobalPrefix(ctx, pathDirs); prefix != "" {
		pathDirs = prependPathDir(pathDirs, filepath.Join(prefix, "bin"))
	}

	// 5. Configure Claude Code against the session's workspace, then record the
	// resolved PATH so subsequent launches take the fast path above.
	if err := os.WriteFile(contextFile, []byte(claudeSystemContext(os.Getenv(workspaceHomeEnv))), 0o644); err != nil {
		return fmt.Errorf("failed to write agent context file: %w", err)
	}
	if err := runCommand(ctx, pathDirs, "ucode", "configure", "--agent", "claude", "--enable-databricks-ai-tools", "--skip-validate"); err != nil {
		return fmt.Errorf("ucode configure failed: %w", err)
	}
	pathEnv := strings.Join(pathDirs, string(os.PathListSeparator))
	if err := os.MkdirAll(shimDir, 0o755); err != nil {
		return fmt.Errorf("failed to create %s: %w", shimDir, err)
	}
	if err := os.WriteFile(readyFile, []byte(pathEnv), 0o644); err != nil {
		return fmt.Errorf("failed to record setup completion: %w", err)
	}

	return launchUcodeClaude(pathEnv, contextFile, claudeArgs)
}

// launchUcodeClaude replaces the current process with `ucode claude`, pinning the
// resolved PATH and pointing Claude Code at the agent context file.
func launchUcodeClaude(pathEnv, contextFile string, claudeArgs []string) error {
	ucodePath := findInPath(splitPathList(pathEnv), "ucode")
	if ucodePath == "" {
		return errors.New("ucode was not found on PATH after setup")
	}
	argv := append([]string{"ucode", "claude", "--append-system-prompt-file", contextFile}, claudeArgs...)
	return execProcess(ucodePath, argv, replaceEnvPath(os.Environ(), pathEnv))
}

// ensureNode downloads the latest Krypton LTS Node build into ~/.ucode-node (once)
// and returns its bin directory. The image is always Linux; runtime.GOARCH matches
// the driver because the uploaded CLI binary is arch-matched.
func ensureNode(ctx context.Context, home string, pathDirs []string) (string, error) {
	nodeDir := filepath.Join(home, nodeDistDirName)
	nodeBin := filepath.Join(nodeDir, "bin")
	if isExecutable(filepath.Join(nodeBin, "npm")) {
		return nodeBin, nil
	}

	arch := nodeDownloadArch(runtime.GOARCH)
	if arch == "" {
		return "", fmt.Errorf("unsupported architecture for Node download: %s", runtime.GOARCH)
	}
	const base = "https://nodejs.org/dist/latest-krypton"
	tarName, err := latestNodeTarball(ctx, base+"/SHASUMS256.txt", arch)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(nodeDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create %s: %w", nodeDir, err)
	}
	// Node ships .tar.xz; xz decompression isn't in the Go stdlib, so stream the
	// download straight through the system tar.
	script := fmt.Sprintf("curl -fsSL %s | tar -xJ --strip-components=1 -C %s -f -",
		shellSingleQuote(base+"/"+tarName), shellSingleQuote(nodeDir))
	if err := runShell(ctx, pathDirs, script); err != nil {
		return "", fmt.Errorf("failed to download Node.js: %w", err)
	}
	return nodeBin, nil
}

// nodeDownloadArch maps a Go arch to Node's release arch token, or "" if unsupported.
func nodeDownloadArch(goarch string) string {
	switch goarch {
	case "amd64":
		return "x64"
	case "arm64":
		return "arm64"
	default:
		return ""
	}
}

// latestNodeTarball parses the SHASUMS256.txt listing to find the linux tarball
// name for arch (e.g. node-v24.1.0-linux-x64.tar.xz).
func latestNodeTarball(ctx context.Context, shasumsURL, arch string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, shasumsURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to fetch Node checksums: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("failed to read Node checksums: %w", err)
	}
	re := regexp.MustCompile(`node-v[0-9.]+-linux-` + regexp.QuoteMeta(arch) + `\.tar\.xz`)
	if name := re.FindString(string(body)); name != "" {
		return name, nil
	}
	return "", fmt.Errorf("no linux-%s Node tarball found in %s", arch, shasumsURL)
}

// npmGlobalPrefix returns `npm prefix -g`, or "" if npm isn't runnable.
func npmGlobalPrefix(ctx context.Context, pathDirs []string) string {
	npm := findInPath(pathDirs, "npm")
	if npm == "" {
		return ""
	}
	cmd := exec.CommandContext(ctx, npm, "prefix", "-g")
	cmd.Env = replaceEnvPath(os.Environ(), strings.Join(pathDirs, string(os.PathListSeparator)))
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// runCommand runs name (resolved against pathDirs) with the given args, inheriting
// stdio and running under a PATH built from pathDirs so the tool's own subprocess
// lookups resolve correctly.
func runCommand(ctx context.Context, pathDirs []string, name string, args ...string) error {
	bin := findInPath(pathDirs, name)
	if bin == "" {
		return fmt.Errorf("%s not found on PATH", name)
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = replaceEnvPath(os.Environ(), strings.Join(pathDirs, string(os.PathListSeparator)))
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// runShell runs a shell snippet under a PATH built from pathDirs. Used for the
// pipelines (curl | sh, curl | tar) that are awkward to express without a shell.
func runShell(ctx context.Context, pathDirs []string, script string) error {
	sh := findInPath(pathDirs, "sh")
	if sh == "" {
		return errors.New("sh not found on PATH")
	}
	cmd := exec.CommandContext(ctx, sh, "-c", script)
	cmd.Env = replaceEnvPath(os.Environ(), strings.Join(pathDirs, string(os.PathListSeparator)))
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// splitPathList splits a PATH-list string into directories, dropping empties.
func splitPathList(path string) []string {
	var dirs []string
	for _, d := range filepath.SplitList(path) {
		if d != "" {
			dirs = append(dirs, d)
		}
	}
	return dirs
}

// removePathDir returns dirs with every occurrence of dir removed.
func removePathDir(dirs []string, dir string) []string {
	out := dirs[:0:0]
	for _, d := range dirs {
		if d != dir {
			out = append(out, d)
		}
	}
	return out
}

// prependPathDir puts dir at the front of dirs, de-duplicating any later copy.
func prependPathDir(dirs []string, dir string) []string {
	return append([]string{dir}, removePathDir(dirs, dir)...)
}

// findInPath returns the absolute path of an executable named name in one of dirs,
// or "" if none is found.
func findInPath(dirs []string, name string) string {
	for _, dir := range dirs {
		candidate := filepath.Join(dir, name)
		if isExecutable(candidate) {
			return candidate
		}
	}
	return ""
}

// isExecutable reports whether path is a regular file with an execute bit set.
func isExecutable(path string) bool {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return false
	}
	return info.Mode()&0o111 != 0
}

// replaceEnvPath returns env with its PATH entry replaced by pathEnv (adding one
// if absent), leaving every other variable untouched.
func replaceEnvPath(env []string, pathEnv string) []string {
	out := make([]string, 0, len(env)+1)
	replaced := false
	for _, kv := range env {
		if strings.HasPrefix(kv, "PATH=") {
			out = append(out, "PATH="+pathEnv)
			replaced = true
			continue
		}
		out = append(out, kv)
	}
	if !replaced {
		out = append(out, "PATH="+pathEnv)
	}
	return out
}

// claudeSystemContext returns the agent system context Claude Code is launched
// with. wsHome, when set, is the user's workspace home; it's woven in so the
// agent knows its working directory.
func claudeSystemContext(wsHome string) string {
	cwd := "the user's Databricks workspace home directory"
	if wsHome != "" {
		cwd = wsHome
	}
	return fmt.Sprintf(`You are running inside a "databricks ssh connect" session on the driver node of a
Databricks serverless cluster.
- The "databricks" CLI is installed and already authenticated: DATABRICKS_HOST and
  DATABRICKS_TOKEN are set in the environment, so "databricks ..." commands work with no
  "databricks auth login". The same token governs Unity Catalog and serving-endpoint access.
- This container is ephemeral; only paths under /Workspace persist across sessions. Your
  working directory is %s.
- DATABRICKS_TOKEN is a static session token that may expire during a long session; if
  "databricks" calls start failing with auth errors, the session likely needs reconnecting.
- You can run shell commands and use the "databricks" CLI to explore the workspace
  (clusters, jobs, Unity Catalog, DBFS, etc.).
- If the user asks to set up a new project, suggest scaffolding it with "databricks bundle
  init" (Databricks Asset Bundles / DABs). If the project needs any resources deployed
  (jobs, pipelines, serving endpoints, etc.), define them in the bundle and deploy via
  "databricks bundle deploy" rather than creating them ad hoc.`, cwd)
}
