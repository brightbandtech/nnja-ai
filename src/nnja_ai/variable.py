from typing import Optional, Union


class NNJAVariable:
    """A class to represent a variable in a NNJADataset.

    Many datasets in the NNJA archive have a large number of variables,
    and the parquet metadata doesn't provide enough flexibility to organize
    and describe them. We've organized variables into four categories, referenced
    by the 'category' attribute:
        - "primary data": The main data variables in the dataset that most users will use
            (e.g. brightness temperature, precipitation, radiance).
        - "primary descriptors": key descriptor variables that are useful for most users
            (e.g., time, latitude, longitude, satellite ID).
        - "secondary data": Additional data variables that are included for completeness,
            but contain little useful information for most users (e.g., data quality flags,
            variables that are null for most observations, etc.).
        - "secondary descriptors": Additional descriptor variables that are included for
            completeness, but contain little useful information for most users (e.g. processing
            station, scan number, etc.).

    Additionally some variables have a 'dimension' attribute, which is used to represent additional
    information about the variable that can be used to subset the data (e.g., 'channel' for a satellite
    with many channels, or pressure level for some soundings). Because the data is based on parquet files,
    we can provide some additional subsetting features by using the 'dimension' attribute.

    Attributes:
        id (str): The fully expanded variable ID, corresponding to the parquet column name.
        base_id (str): The original variable ID; same as id unless the variable has a dimension
        description (str): Description of the variable.
        category (str): Category of the variable.
        dimension (optional): Dimension of the variable, if available.
        extra_metadata (dict): Additional metadata for the variable.
    """

    def __init__(
        self,
        variable_metadata: dict,
        full_id: str,
        dim_val: Optional[Union[float, int]] = None,
    ):
        """
        Initialize an NNJAVariable object.

        Args:
            variable_metadata: Metadata for the variable.
            full_id: The fully expanded variable ID (e.g., 'brightness_temp_00007', or "lat").
            dim_val: The value of the dimension for this variable, if applicable.

        """
        self.id = full_id
        self.base_id = variable_metadata["id"]
        self.dim_val = dim_val
        self.description = variable_metadata["description"]
        self.category = variable_metadata["category"]
        self.dimension = variable_metadata.get("dimension")
        self.extra_metadata = variable_metadata.get("extra_metadata", {})

    def __repr__(self) -> str:
        """Provide a string representation of the variable."""
        table_str = ""
        if "code table" in self.extra_metadata:
            table_str = f' [code table: {self.extra_metadata["code table"]}]'
        elif "flag table" in self.extra_metadata:
            table_str = f' [flag table: {self.extra_metadata["flag table"]}]'
        return f'NNJAVariable("{self.id}" ({self.category}), {self.description}){table_str}'

    def info(self) -> str:
        """Provide a summary of the variable."""
        info_str = (
            f"Variable '{self.id}': {self.description}\n"
            f"Category: {self.category}\n"
            f"Extra Metadata: {self.extra_metadata}"
        )
        if self.dimension is not None:
            info_str += f"\nDimension: {self.dimension}"
        # Add code/flag table info if present
        if "code table" in self.extra_metadata:
            info_str += f"\nCode Table: {self.extra_metadata['code table']}"
            if "code table link" in self.extra_metadata:
                info_str += (
                    f"\nCode Table Link: {self.extra_metadata['code table link']}"
                )
        if "flag table" in self.extra_metadata:
            info_str += f"\nFlag Table: {self.extra_metadata['flag table']}"
            if "flag table link" in self.extra_metadata:
                info_str += (
                    f"\nFlag Table Link: {self.extra_metadata['flag table link']}"
                )
        return info_str

    @property
    def is_code_or_flag_table(self) -> bool:
        """Return True if the variable is associated with a code or flag table."""
        return ("code table" in self.extra_metadata) or (
            "flag table" in self.extra_metadata
        )
