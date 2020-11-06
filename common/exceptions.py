class WorkerError(Exception):
    """Exceptions raised during job processing

    Attributes:
        message -- explaination of the error
    """

    def __init__(self, message):
        self.message = message


class CephErrorNonFatal(Exception):
    """Exceptions raised during ceph operations but have been handled, used for
    logging error messages.

    Attributes:
        message -- explaination of the error
    """

    def __init__(self, message):
        self.message = message


class CephErrorFatal(Exception):
    """Exceptions raised during ceph operations and could not be handled,
    should cause the calling function to break execution.

    Attributes:
        message -- explaination of the error
    """

    def __init__(self, message):
        self.message = message

class PveErrorNonFatal(Exception):
    """Exceptions raised during Proxmox operations but have been handled, used
    for logging error messages.

    Attributes:
        message -- explaination of the error
    """

    def __init__(self, message):
        self.message = message


class PveErrorFatal(Exception):
    """Exceptions raised during Proxmox operations and could not be handled,
    should cause the calling function to break execution.

    Attributes:
        message -- explaination of the error
    """

    def __init__(self, message):
        self.message = message
