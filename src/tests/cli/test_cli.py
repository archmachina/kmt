
import sys
import subprocess
import pytest
import kmt

class TestCli:
    def test_1(self):
        sys.argv = ["kmt", "--help"]

        with pytest.raises(SystemExit):
            res = kmt.cli.main()

    def test_2(self):
        # Test running the entrypoint
        ret = subprocess.call(["/work/bin/entrypoint", "--help"])

        assert ret == 0

