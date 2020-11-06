import unittest
from pveBarque.common import config_reader as cfg


class TestConfigRead(unittest.TestCase):
    def test_lxc_config_read(self):
        cfg.lxc_config_read(".")
