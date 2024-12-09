from .io import read_json
import logging
from .dataset import NNJADataset

logger = logging.getLogger(__name__)

# Configuration parameters
STRICT_LOAD = False  # If True, raise error if can't find dataset jsons


class DataCatalog:
    def __init__(self, json_uri: str):
        """
        Initialize the DataCatalog from a JSON metadata file.

        Args:
            json_uri (str): Path to the JSON file (local or cloud storage).
        """
        self.catalog_metadata = read_json(json_uri)
        self.datasets = self._parse_datasets()

    def _parse_datasets(self) -> dict:
        """
        Parse datasets from the catalog metadata and initialize NNJADataset instances.

        Returns:
            dict: A dictionary of dataset instances or subtypes if multiple exist.
        """
        datasets = {}
        for group, group_metadata in self.catalog_metadata.items():
            message_types = group_metadata.get("datasets", {})
            if len(message_types) == 1:
                single_type, single_metadata = next(iter(message_types.items()))
                try:
                    datasets[group] = NNJADataset(single_metadata["json"])
                except Exception as e:
                    if STRICT_LOAD:
                        raise RuntimeError(
                            f"Failed to load dataset for group '{group}': {e}"
                        ) from e
                    else:
                        logger.warning(
                            f"Could not load dataset for group '{group}': {e}"
                        )
            else:
                group_datasets = {}
                for msg_type, msg_metadata in message_types.items():
                    try:
                        group_datasets[msg_type] = NNJADataset(msg_metadata["json"])
                    except Exception as e:
                        if STRICT_LOAD:
                            raise RuntimeError(
                                f"Failed to load dataset for group '{group}', message type '{msg_type}': {e}"
                            ) from e
                        else:
                            logger.warning(
                                f"Could not load dataset for group '{group}', message type '{msg_type}': {e}"
                            )
                datasets[group] = group_datasets
        return datasets

    def info(self) -> str:
        """Provide information about the catalog."""
        return "\n".join(
            f"{group}: {group_metadata['description']}"
            for group, group_metadata in self.catalog_metadata.items()
        )

    def list_datasets(self) -> list:
        """List all dataset groups."""
        return list(self.catalog_metadata.keys())

    def search(self, queryterm: str) -> list:
        """
        Search datasets by tags, description, or name.

        Args:
            queryterm (str): The term to search for.

        Returns:
            list: A list of NNJADataset objects matching the search term.
        """
        results = []
        for group, group_metadata in self.catalog_metadata.items():
            datasets = group_metadata.get("datasets", {})
            for message_type, dataset_metadata in datasets.items():
                if any(queryterm.lower() in str(value).lower() for value in dataset_metadata.values()):
                    json_uri = dataset_metadata["json"]
                    try:
                        dataset = NNJADataset(json_uri)
                        results.append(dataset)
                    except Exception as e:
                        logger.warning(
                            f"Could not load dataset for group '{group}', message type '{message_type}': {e}"
                        )
        return results


def generate_catalog():
    # Right now I've hardcoded the catalog json; this should probably be generated
    # by parsing over the dataset jsons

    pass
