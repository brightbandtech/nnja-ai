from .variable import NNJAVariable
from .io import read_json


class NNJADataset:
    def __repr__(self):
        """Return a concise string representation of the dataset."""
        return (
            f"<NNJADataset(name='{self.name}', "
            f"description='{self.description[:50]}...', "
            f"tags={self.tags}, "
            f"files={len(self.manifest)}, "
            f"variables={len(self.variables)})>"
        )

    def __init__(self, json_uri: str):
        """
        Initialize an NNJADataset object from a JSON file or URI.

        Args:
            json_uri (str): Path or URI to the dataset's JSON metadata.
        """
        self.json_uri = json_uri
        dataset_metadata = read_json(json_uri)  # Load JSON metadata
        self.name = dataset_metadata["name"]
        self.description = dataset_metadata["description"]
        self.tags = dataset_metadata["tags"]
        self.manifest = dataset_metadata["manifest"]
        self.dimensions = self._parse_dimensions(dataset_metadata.get("dimensions", []))
        self.variables = self._expand_variables(dataset_metadata["variables"])

    def _parse_dimensions(self, dimensions_metadata: list) -> dict:
        """
        Parse dimensions from metadata.

        Args:
            dimensions_metadata (list): List of dimension definitions.

        Returns:
            dict: Dictionary of dimensions.
        """
        dimensions = {}
        for dim in dimensions_metadata:
            for name, metadata in dim.items():
                dimensions[name] = metadata
        return dimensions

    def _expand_variables(self, variables_metadata: list) -> list:
        """
        Expand variables tied to dimensions into individual NNJAVariable objects.

        Args:
            variables_metadata (list): List of variable definitions.

        Returns:
            list: List of NNJAVariable objects.
        """
        variables = []
        for var_metadata in variables_metadata:
            if var_metadata.get("dimension"):
                dim_name = var_metadata["dimension"]
                dim = self.dimensions.get(dim_name)
                if dim:
                    for value in dim["values"]:
                        formatted_value = f"{value:{dim['format_str']}}"
                        full_id = f"{var_metadata['id']}_{formatted_value}"
                        variables.append(NNJAVariable(var_metadata, full_id))
            else:
                variables.append(NNJAVariable(var_metadata, var_metadata["id"]))
        return variables

    def info(self) -> str:
        """Provide a summary of the dataset."""
        return (
            f"Dataset '{self.name}': {self.description}\n"
            f"Tags: {', '.join(self.tags)}\n"
            f"Files: {len(self.manifest)} files in manifest\n"
            f"Variables: {len(self.variables)}"
        )

    def list_variables(self) -> list:
        """List all variables with their descriptions."""
        return [var.info() for var in self.variables]

    def get_variable(self, variable_id: str) -> NNJAVariable:
        """
        Fetch a specific variable by ID.

        Args:
            variable_id (str): The ID of the variable.

        Returns:
            NNJAVariable: The requested variable, or None if not found.
        """
        for var in self.variables:
            if var.id == variable_id:
                return var
        return None

    def load_dataset(self, library="pandas"):
        """
        Load the dataset into a DataFrame using the specified library.

        Args:
            library (str): The library to use for loading the dataset ('pandas', 'polars', etc.).

        Returns:
            DataFrame: The loaded dataset.
        """
        if library == "pandas":
            import pandas as pd
            return pd.read_parquet(self.manifest)
        elif library == "polars":
            import polars as pl
            return pl.scan_parquet(self.manifest)
        elif library == "dask":
            import dask.dataframe as dd
            return dd.read_parquet(self.manifest)
        else:
            raise ValueError(f"Unsupported library: {library}")

