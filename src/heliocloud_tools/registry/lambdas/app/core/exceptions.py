"""
Exceptions that can be thrown by various HelioCloud Registry processes.
"""


class RegistryException(Exception):
    """
    Exception that can be thrown by Registry processes.
    """

    def __init__(self, message="There was an error in the Registry module") -> None:
        self.message = message
        super().__init__(self.message)


class IngesterException(Exception):
    """
    Specific exception type that can be raised by the Ingester process.
    """

    def __init__(self, message="There was an error in the ingest process"):
        self.message = message
        super().__init__(self.message)
