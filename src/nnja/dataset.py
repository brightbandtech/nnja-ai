from nnja import io
from nnja.variable import NNJAVariable
from typing import Dict, List, Union
import copy


class NNJADataset:
    """NNJADataset class for handling dataset metadata and loading data.

    The NNJADataset class is primarily meant to aid in navigating dataset metadata and loading data,
    with some support for data subsetting. The intent is that this class is used to find the
    appropriate dataset and variable(s) of interest, and then load the data into whichever library
    (e.g., pandas, polars, dask) is most appropriate for the user's needs.

    Attributes:
        name (str): Name of the dataset.
        description (str): Description of the dataset.
        tags (list): List of tags associated with the dataset.
        manifest (list): List of files in the dataset's manifest.
        dimensions (dict): Dict of dimensions parsed from metadata.
        variables (dict): Dict of NNJAVariable objects representing the dataset's variables.
    """

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
            json_uri: Path or URI to the dataset's JSON metadata.
        """
        dataset_metadata = io.read_json(json_uri)
        self.json_uri = json_uri
        self.name: str = dataset_metadata["name"]
        self.description: str = dataset_metadata["description"]
        self.tags: List[str] = dataset_metadata["tags"]
        self.manifest: List[str] = dataset_metadata["manifest"]
        self.dimensions: Dict[str, Dict] = self._parse_dimensions(
            dataset_metadata.get("dimensions", [])
        )
        self.variables: Dict[str, NNJAVariable] = self._expand_variables(
            dataset_metadata["variables"]
        )

    def __getitem__(
        self, key: Union[str, List[str]]
    ) -> Union[NNJAVariable, "NNJADataset"]:
        """
        Fetch a specific variable by ID or subset the dataset by a list of variable names.

        Args:
            key: The ID of the variable to fetch or a list of variable names to subset.

        Returns:
            NNJAVariable or NNJADataset: The variable object if a single ID is provided,
                                       or a DataFrame with the subsetted data if a list of variable names is provided.
        """
        if isinstance(key, str):
            # Single variable access
            return self.variables[key]
        elif isinstance(key, list):
            return self._select_columns(key)
        else:
            raise TypeError("Key must be a string or a list of strings")

    def _select_columns(self, columns: List[str]) -> "NNJADataset":
        """
        Subset the dataset by a list of variable names.

        Args:
            columns: List of variable names to subset the dataset.

        Returns:
            NNJADataset: A new dataset object with only the specified variables.
        """
        for col in columns:
            if col not in self.variables:
                raise ValueError(f"Variable '{col}' not found in dataset.")
        new_dataset = copy.deepcopy(self)
        new_dataset.variables = {
            k: v for k, v in self.variables.items() if k in columns
        }
        needed_dims = set([v.dimension for v in new_dataset.variables.values()])
        new_dataset.dimensions = {
            k: v for k, v in self.dimensions.items() if k in needed_dims
        }
        return new_dataset

    def _parse_dimensions(self, dimensions_metadata: list) -> dict:
        """
        Parse dimensions from metadata.

        Args:
            dimensions_metadata: List of dimension definitions.

        Returns:
            dict: Dictionary of dimensions.
        """
        dimensions = {}
        for dim in dimensions_metadata:
            for name, metadata in dim.items():
                dimensions[name] = metadata
        return dimensions

    def _expand_variables(self, variables_metadata: list) -> Dict[str, NNJAVariable]:
        """
        Expand variables from the dataset metadata into NNJAVariable objects.

        This is only nontrivial since we've packed variables tied to dimensions into a single
        variable definition in the metadata to avoid redundancy. Set as dict to allow for easy
        retrieval by variable ID.

        Args:
            variables_metadata: List of variable definitions.

        Returns:
            Dict of NNJAVariable objects.
        """
        variables = {}
        for var_metadata in variables_metadata:
            if var_metadata.get("dimension"):
                dim_name = var_metadata["dimension"]
                dim = self.dimensions.get(dim_name)
                if dim:
                    for value in dim["values"]:
                        formatted_value = f"{value:{dim['format_str']}}"
                        full_id = f"{var_metadata['id']}_{formatted_value}"
                        # variables.append(NNJAVariable(var_metadata, full_id))
                        variables[full_id] = NNJAVariable(var_metadata, full_id)
            else:
                # variables.append(NNJAVariable(var_metadata, var_metadata["id"]))
                variables[var_metadata["id"]] = NNJAVariable(
                    var_metadata, var_metadata["id"]
                )
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
        return [var.info() for var in self.variables.values()]

    def load_dataset(self, backend: io.Backend = "pandas", **backend_kwargs):
        """
        Load the dataset into a DataFrame using the specified library.

        Args:
            backend: The library to use for loading the dataset ('pandas', 'polars', etc.).
            **backend_kwargs: Additional keyword arguments to pass to the backend loader.

        Returns:
            DataFrame: The loaded dataset.
        """
        files = self.manifest
        columns = [var.id for var in self.variables.values()]
        return io.load_parquet(files, columns, backend, **backend_kwargs)

    def sel(self, **kwargs):
        """
        Select data based on the provided keywords.

        Args:
            **kwargs: Keywords for subsetting. Valid keywords are 'variables', 'columns', 'time',
                    and any extra dimensions in self.dimensions.

        Returns:
            NNJADataset: A new dataset object with the subsetted data.
        """
        if "variables" in kwargs and "columns" in kwargs:
            raise ValueError(
                "Cannot provide both 'variables' and 'columns' in selection"
            )
        new_dataset = copy.deepcopy(self)
        for key, value in kwargs.items():
            if key in ["variables", "columns"]:
                new_dataset = new_dataset._select_columns(value)
            elif key == "time":
                new_dataset = new_dataset._select_time(value)
            elif key in self.dimensions:
                new_dataset = new_dataset._select_extra_dimension(key, value)
            else:
                raise ValueError(f"Invalid selection keyword: {key}")
        return new_dataset

    def _select_time(self, time_range: List[str]) -> "NNJADataset":
        raise NotImplementedError

    def _select_extra_dimension(
        self, dim_name: str, value: Union[str, List[str]]
    ) -> "NNJADataset":
        raise NotImplementedError
