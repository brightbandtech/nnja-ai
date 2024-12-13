from nnja import io
from nnja.variable import NNJAVariable
from nnja.exceptions import ManifestNotFoundError
from typing import Dict, List, Union
import copy
import pandas as pd
import logging
import warnings
from nnja.exceptions import EmptyTimeSubsetError

# Define the valid types for time selection
DatetimeIndexKey = Union[
    str,
    pd.Timestamp,
    List[str],
    List[pd.Timestamp],
    slice,
]

logger = logging.getLogger(__name__)


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
        oarquet_root_path (str): Directory containing the dataset's parquet files.
        manifest (DataFrame): DataFrame containing the dataset's manifest of parquet partitions.
        dimensions (dict): Dict of dimensions parsed from metadata.
        variables (dict): Dict of NNJAVariable objects representing the dataset's variables.
    """

    def __repr__(self):
        """Return a concise string representation of the dataset."""
        return (
            f"<NNJADataset(name='{self.name}', "
            f"description='{self.description[:50]}...', "
            f"tags={self.tags}, "
            f"parquet_root_path='{self.parquet_root_path}', "
            f"files={len(self.manifest)}, "
            f"variables={len(self.variables)})>"
        )

    def __init__(self, json_uri: str, skip_manifest: bool = False):
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
        self.parquet_root_path: str = dataset_metadata["parquet_root_path"]
        self.manifest: pd.DataFrame = pd.DataFrame()
        if not skip_manifest:
            self.manifest = io.load_manifest(self.parquet_root_path)
        self.dimensions: Dict[str, Dict] = self._parse_dimensions(
            dataset_metadata.get("dimensions", [])
        )
        self.variables: Dict[str, NNJAVariable] = self._expand_variables(
            dataset_metadata["variables"]
        )

    def load_manifest(self):
        """Load the dataset's manifest of parquet partitions.

        Returns:
            NNJADataset: The dataset object with the manifest loaded.
        """

        self.manifest: pd.DataFrame = io.load_manifest(self.parquet_root_path)
        return self

    def __getitem__(
        self, key: Union[str, List[str]]
    ) -> Union[NNJAVariable, "NNJADataset"]:
        """Fetch a specific variable by ID, or subset the dataset by a list of variable names.

        If a single variable ID is provided, return the variable object.
        If a list of variable names is provided, return a new dataset object with only the specified variables.

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
        """Subset the dataset by a list of variable names.

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
        """Expand variables from the dataset metadata into NNJAVariable objects.

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
        """Load the dataset into a DataFrame using the specified library.

        Args:
            backend: The library to use for loading the dataset ('pandas', 'polars', etc.).
            **backend_kwargs: Additional keyword arguments to pass to the backend loader.

        Returns:
            DataFrame: The loaded dataset.
        """
        if self.manifest.empty:
            raise ManifestNotFoundError(
                "Manifest is empty. Load the manifest first with load_manifest()."
            )

        files = self.manifest["file"].tolist()
        columns = [var.id for var in self.variables.values()]
        return io.load_parquet(files, columns, backend, **backend_kwargs)

    def sel(self, **kwargs):
        """Select data based on the provided keywords.

        Allows for three types of selection:
            - 'variables' or 'columns': Subset the dataset by a list of variable names.
            - 'time': Subset the dataset by a time range.
            - Any extra dimensions in self.dimensions: Subset the dataset by a specific value of the dimension.
        Multiple keywords can be provided to perform multiple selections.

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

    def _select_time(self, selection: DatetimeIndexKey) -> "NNJADataset":
        """
        Subset the dataset by a time range.

        Args:
            selection: A single timestamp, a string that can be cast to a timestamp, a slice of timestamps,
                       or a list of timestamps or strings that can be cast to timestamps.

        Returns:
            NNJADataset: A new dataset object with the subsetted data.
        """
        manifest_df = self.manifest

        def localize_to_utc(dt):
            if dt.tzinfo is None:
                warnings.warn(f"Naive datetime {dt} assumed to be in UTC", UserWarning)
                return dt.tz_localize("UTC")
            if str(dt.tzinfo) != "UTC":
                warnings.warn(
                    f"Non-UTC timezone {dt.tzinfo} converted to UTC", UserWarning
                )
                dt = dt.tz_convert("UTC")
            return dt

        match selection:
            case str():
                try:
                    selection = pd.to_datetime(selection)
                    selection = localize_to_utc(selection)
                    subset_df = manifest_df.loc[[selection]]
                except ValueError:
                    raise TypeError("Selection must be a valid timestamp string")
            case pd.Timestamp():
                selection = localize_to_utc(selection)
                subset_df = manifest_df.loc[[selection]]
            case slice():
                start = (
                    localize_to_utc(pd.to_datetime(selection.start))
                    if selection.start
                    else None
                )
                stop = (
                    localize_to_utc(pd.to_datetime(selection.stop))
                    if selection.stop
                    else None
                )
                subset_df = manifest_df.loc[start:stop]
            case list():
                try:
                    selection = [
                        localize_to_utc(pd.to_datetime(item))
                        if isinstance(item, str)
                        else localize_to_utc(item)
                        for item in selection
                    ]
                    subset_df = manifest_df.loc[selection]
                except ValueError:
                    raise TypeError(
                        "All items in the list must be valid timestamps or timestamp strings"
                    )
            case _:
                raise TypeError(
                    "Selection must be a pd.Timestamp, valid timestamp string, "
                    "slice, or list of pd.Timestamps or valid timestamp strings"
                )
        if subset_df.empty:
            min_time = manifest_df.index.min()
            max_time = manifest_df.index.max()
            raise EmptyTimeSubsetError(
                f"Time subset resulted in an empty DataFrame. "
                f"Selection: {selection}, Min time: {min_time}, Max time: {max_time}"
            )

        # Create a new dataset with the subsetted manifest
        new_dataset = copy.deepcopy(self)
        new_dataset.manifest = subset_df
        return new_dataset

    def _select_extra_dimension(
        self, dim_name: str, value: Union[str, List[str]]
    ) -> "NNJADataset":
        raise NotImplementedError
