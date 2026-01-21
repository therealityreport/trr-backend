"""Unit tests for verify_credits_parity script argument parsing."""

from __future__ import annotations

from scripts.verify_credits_parity import parse_args


class TestParseArgs:
    """Test argument parsing for verify_credits_parity script."""

    def test_default_args(self) -> None:
        """Test default argument values."""
        args = parse_args([])

        assert args.verbose is False
        assert args.show_id is None
        assert args.limit is None
        assert args.spot_check == 10

    def test_verbose_flag(self) -> None:
        """Test --verbose flag."""
        args = parse_args(["--verbose"])
        assert args.verbose is True

        args = parse_args(["-v"])
        assert args.verbose is True

    def test_show_id_arg(self) -> None:
        """Test --show-id argument."""
        show_id = "4f2bee5e-c791-4e2e-b461-c56f2d77bb5a"
        args = parse_args(["--show-id", show_id])
        assert args.show_id == show_id

    def test_limit_arg(self) -> None:
        """Test --limit argument."""
        args = parse_args(["--limit", "10"])
        assert args.limit == 10

    def test_spot_check_arg(self) -> None:
        """Test --spot-check argument."""
        args = parse_args(["--spot-check", "20"])
        assert args.spot_check == 20

    def test_combined_args(self) -> None:
        """Test multiple arguments together."""
        args = parse_args(
            [
                "--verbose",
                "--limit",
                "5",
                "--spot-check",
                "15",
            ]
        )

        assert args.verbose is True
        assert args.limit == 5
        assert args.spot_check == 15
        assert args.show_id is None

    def test_show_id_overrides_limit(self) -> None:
        """Test that --show-id can coexist with --limit (show_id takes priority in script logic)."""
        args = parse_args(
            [
                "--show-id",
                "test-uuid",
                "--limit",
                "10",
            ]
        )

        assert args.show_id == "test-uuid"
        assert args.limit == 10  # Both are parsed, script logic handles precedence
