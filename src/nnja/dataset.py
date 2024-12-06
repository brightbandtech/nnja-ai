class NNJADataset:
    def __init__(self, metadata: dict):
        """
        Initialize the NNJADataset object.
        Args:
            metadata (dict): Metadata for this specific dataset.
        """
        self.name = metadata.get("name")
        self.description = metadata.get("description")
        self.tags = metadata.get("tags", [])
        self.json_path = metadata.get("json")
        self.metadata = None

    def info(self) -> str:
        """Return a summary of the dataset."""
        return f"{self.name}: {self.description}\nTags: {', '.join(self.tags)}"

    def load_metadata(self):
        """Load the metadata JSON for this dataset."""
        if not self.metadata:
            from .io import read_json  # Utility function for reading JSON
            self.metadata = read_json(self.json_path)

    def subset(self, **kwargs):
        """Return a subset of the dataset (to be implemented based on use case)."""
        # Implement lazy subsetting logic here.
        pass

    def to_pandas(self):
        """Load the dataset as a pandas DataFrame."""
        import pandas as pd
        # Use self.json_path or other logic to load the actual dataset.
        self.load_metadata()
        return pd.read_parquet(self.metadata["data_path"])  # Example path in metadata

    def to_polars(self):
        """Load the dataset as a Polars DataFrame."""
        import polars as pl
        self.load_metadata()
        return pl.scan_parquet(self.metadata["data_path"])  # Example path in metadata

    def to_dask(self):
        """Load the dataset as a Dask DataFrame."""
        import dask.dataframe as dd
        self.load_metadata()
        return dd.read_parquet(self.metadata["data_path"])  # Example path in metadata
