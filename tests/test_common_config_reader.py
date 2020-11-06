import pytest
import mock
import pveBarque.common.config_reader as cfg


# Test reading barque application configuration
mock_open_etc = mock.mock_open()


def test_open_file_exists():
    with mock.patch('__builtin__.open', mock_open_etc):
        config = cfg.barque_config_read()
