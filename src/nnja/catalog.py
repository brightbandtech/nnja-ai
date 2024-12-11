from nnja import io
from nnja.dataset import NNJADataset
import logging
import os
from typing import Dict, Any

logger = logging.getLogger(__name__)

# Configuration parameters
STRICT_LOAD = os.getenv("STRICT_LOAD", default=False)


class DataCatalog:
    """DataCatalog class for finding and loading NNJA datasets.

    The DataCatalog represents a collection of NNJADataset objects,
    and provides some basic search/list functionality.

    Attributes:
        json_uri (str): Path to the JSON file (local or cloud storage).
        catalog_metadata (dict): Metadata of the catalog, loaded from the JSON file.
        datasets (dict): Dictionary of dataset instances or subtypes.
    """

    def __init__(self, json_uri: str):
        """
        Initialize the DataCatalog from a JSON metadata file.

        Args:
            json_uri: Path to the JSON file (local or cloud storage).
        """
        self.json_uri = json_uri
        self.catalog_metadata: Dict[str, Dict[str, Any]] = io.read_json(json_uri)
        self.datasets: Dict[str, NNJADataset] = self._parse_datasets()

    def __getitem__(self, dataset_name: str) -> NNJADataset:
        """
        Fetch a specific dataset by name.

        Args:
            dataset_name: The name of the dataset to fetch.

        Returns:
            NNJADataset: The dataset object.
        """
        return self.datasets[dataset_name]

    def _parse_datasets(self) -> dict:
        """
        Parse datasets from the catalog metadata and initialize NNJADataset instances.


        Returns:
            dict: A dictionary of dataset instances or subtypes if multiple exist.
        """
        datasets = {}
        for group, group_metadata in self.catalog_metadata.items():
            message_types = group_metadata.get("datasets", {})
            for msg_type, msg_metadata in message_types.items():
                # If there are multiple messages in a group, use message type name in the key.
                key = (
                    group
                    if len(message_types) == 1
                    else group + "_" + msg_metadata["name"]
                )
                try:
                    datasets[key] = NNJADataset(msg_metadata["json"])
                except Exception as e:
                    if STRICT_LOAD:
                        raise RuntimeError(
                            f"Failed to load dataset for group '{group}', message type '{msg_type}': {e}"
                        ) from e
                    else:
                        logger.warning(
                            f"Could not load dataset for group '{group}', message type '{msg_type}': {e}"
                        )
        return datasets

    def info(self) -> str:
        """Provide information about the catalog."""
        return "\n".join(
            f"{group}: {group_metadata['description']}"
            for group, group_metadata in self.catalog_metadata.items()
        )

    def list_datasets(self) -> list:
        """List all dataset groups."""
        return list(self.datasets.keys())

    def search(self, query_term: str) -> list:
        """
        Search datasets by name, tags, description, or variables.

        Args:
            query_term: The term to search for.

        Returns:
            list: A list of NNJADataset objects matching the search term.
        """
        results = []
        for dataset in self.datasets.values():
            for field in ["name", "description", "tags"]:
                result = getattr(dataset, field)
                if isinstance(result, list):
                    if query_term.lower() in [x.lower() for x in result]:
                        results.append(dataset)
                        break
                elif isinstance(result, str):
                    if query_term.lower() in result.lower():
                        results.append(dataset)
                        break
            for variable in dataset.variables.values():
                for field in ["id", "description"]:
                    result = getattr(variable, field)
                    if isinstance(result, str):
                        if query_term.lower() in result.lower():
                            results.append(dataset)
                            break
                if dataset in results:
                    break
        return results


def generate_catalog():
    # Right now I've hardcoded the catalog json; this should probably be generated
    # by parsing over the dataset jsons

    pass
