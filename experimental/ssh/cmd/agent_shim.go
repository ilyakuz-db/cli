package ssh

import (
	"github.com/databricks/cli/cmd/root"
	"github.com/databricks/cli/experimental/ssh/internal/client"
	"github.com/databricks/cli/libs/cmdctx"
	"github.com/spf13/cobra"
)

// newAgentShimCommand builds the hidden `ssh agent-shim` command tree. Its
// subcommands are the `claude` (and future agent) launchers that
// `databricks ssh connect` installs on the remote PATH: a tiny wrapper on the
// driver invokes `databricks ssh agent-shim claude ...`, which probes the AI
// Gateway, bootstraps the toolchain, and launches the agent.
func newAgentShimCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "agent-shim",
		Short: "Launch a ucode-configured coding agent (invoked on the remote)",
		// Internal command spawned by the on-remote `claude` wrapper, not meant to
		// be run by users directly.
		Hidden: true,
	}
	cmd.AddCommand(newAgentShimClaudeCommand())
	return cmd
}

func newAgentShimClaudeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "claude",
		Short: "Launch ucode-configured Claude Code",
		// Everything after `claude` is forwarded verbatim to Claude Code, which has
		// its own flags (e.g. -p, --dangerously-skip-permissions); disable flag
		// parsing here so cobra doesn't try to interpret them.
		DisableFlagParsing: true,
	}

	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		// Runs on the serverless driver where DATABRICKS_HOST/DATABRICKS_TOKEN are
		// injected into the environment; there is no bundle and no interactive prompt.
		cmd.SetContext(root.SkipLoadBundle(cmd.Context()))
		cmd.SetContext(root.SkipPrompt(cmd.Context()))
		return root.MustWorkspaceClient(cmd, args)
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return client.RunClaudeAgentShim(ctx, cmdctx.WorkspaceClient(ctx), args)
	}

	return cmd
}
