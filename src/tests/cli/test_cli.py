
import sys
import kmt
import pytest

class TestCli:
    def test_1(self):
        sys.argv = ["kmt", "--help"]

        with pytest.raises(SystemExit):
            res = kmt.cli.main()

