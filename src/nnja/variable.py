class NNJAVariable:
    def __init__(self, variable_metadata: dict, full_id: str):
        """
        Initialize an NNJAVariable object.

        Args:
            variable_metadata (dict): Metadata for the variable.
            full_id (str): The fully expanded variable ID (e.g., 'brightness_temp_00007').
        """
        self.id = full_id
        self.description = variable_metadata["description"]
        self.category = variable_metadata["category"]
        self.dimension = variable_metadata.get("dimension")
        self.extra_metadata = variable_metadata.get("extra_metadata", {})

    def info(self) -> str:
        """Provide a summary of the variable."""
        return (
            f"Variable '{self.id}': {self.description}\n"
            f"Category: {self.category}\n"
            f"Extra Metadata: {self.extra_metadata}"
        )
