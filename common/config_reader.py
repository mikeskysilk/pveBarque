# pveBarque/common/config_reader.py
import toml


def barque_config_read(filePath='/etc/barque/barque.conf'):
    return toml.load(filePath)


def lxc_config_read(filePath):
    pass


def lxc_config_write(filePath):
    pass


def qemu_config_read(filePath):
    pass


def qemu_config_write(filePath):
    pass
