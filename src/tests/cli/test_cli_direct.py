"""
Tests for kmt.cli

Unlike src/tests/cli/test_cli.py (which exercises the built container image's
entrypoint), these call kmt.cli.main() directly in-process so they can run
without a docker build.
"""

import pytest

import kmt.cli


def _run_main(monkeypatch, argv, capsys):
    monkeypatch.setattr("sys.argv", ["kmt"] + argv)

    with pytest.raises(SystemExit) as excinfo:
        kmt.cli.main()

    return excinfo.value.code, capsys.readouterr()


class TestCliMain:
    """
    Tests for kmt.cli.main
    """

    def test_help_exits_zero(self, monkeypatch, capsys):
        """--help is handled by argparse itself and exits 0"""
        monkeypatch.setattr("sys.argv", ["kmt", "--help"])

        with pytest.raises(SystemExit) as excinfo:
            kmt.cli.main()

        assert excinfo.value.code == 0

    def test_missing_path_argument_is_a_clean_usage_error(self, monkeypatch, capsys):
        """
        Regression test: with exit_on_error=False, a missing required argument used to
        raise an uncaught argparse.ArgumentError (a raw traceback) instead of the usual
        clean "usage: ..." error and exit(2).
        """
        code, captured = _run_main(monkeypatch, [], capsys)

        assert code == 2
        assert "usage:" in captured.err
        assert "Traceback" not in captured.err

    def test_successful_run_prints_manifests_and_exits_zero(self, tmp_path, monkeypatch, capsys):
        """A pipeline that runs cleanly prints its manifests and exits 0"""
        (tmp_path / "config.yaml").write_text(
            """
            pipeline:
              - import:
                  files:
                    - "manifest.yaml"
            """,
            encoding="utf-8",
        )
        (tmp_path / "manifest.yaml").write_text(
            "kind: ConfigMap\nmetadata:\n  name: cm1\n",
            encoding="utf-8",
        )

        code, captured = _run_main(monkeypatch, [str(tmp_path)], capsys)

        assert code == 0
        assert "cm1" in captured.out

    def test_missing_config_yaml_exits_nonzero(self, tmp_path, monkeypatch, capsys):
        """A pipeline directory error is caught and reported as a clean exit(1), not a traceback"""
        code, captured = _run_main(monkeypatch, [str(tmp_path)], capsys)

        assert code == 1
        assert captured.out == ""

    def test_debug_flag_logs_full_traceback_on_error(self, tmp_path, monkeypatch, caplog):
        """With -d, the error is logged with a full traceback instead of just the message"""
        monkeypatch.setattr("sys.argv", ["kmt", "-d", str(tmp_path)])

        with pytest.raises(SystemExit) as excinfo:
            kmt.cli.main()

        assert excinfo.value.code == 1
        assert "Traceback" in caplog.text
