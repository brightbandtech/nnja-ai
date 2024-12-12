class NNJAError(Exception):
    """Base class for all NNJA exceptions."""

    pass


class InvalidPartitionKeyError(NNJAError):
    """Exception raised for invalid partition keys in file paths."""

    def __init__(self, key, message="Invalid partition key found in file path"):
        self.key = key
        self.message = f"{message}: {key}"
        super().__init__(self.message)


class ManifestNotFoundError(NNJAError):
    """Exception raised when the manifest is not loaded."""

    def __init__(self, message="Manifest not loaded"):
        self.message = message
        super().__init__(self.message)


class EmptyTimeSubsetError(NNJAError):
    """Exception raised when the time subset results in an empty DataFrame."""

    def __init__(
        self,
        selection,
        min_time,
        max_time,
        message="Time subset resulted in an empty DataFrame",
    ):
        self.selection = selection
        self.min_time = min_time
        self.max_time = max_time
        self.message = f"{message}. Selection: {selection}, Min time: {min_time}, Max time: {max_time}"
        super().__init__(self.message)
