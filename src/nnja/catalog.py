from .io import read_json
from .dataset import NNJADataset


class DataCatalog:
    def __init__(self, json_uri: str):
        """
        Initialize the DataCatalog from a JSON metadata file.

        Args:
            json_uri (str): Path to the JSON file (local or cloud storage).
        """
        from .io import read_json  # Assume this is your utility function for JSON loading
        self.catalog_metadata = read_json(json_uri)
        self.datasets = self._parse_datasets()

    def _parse_datasets(self) -> dict:
        """
        Parse the datasets from the catalog metadata.

        Returns:
            dict: A dictionary of NNJADataset objects.
        """
        datasets = {}
        for group, group_metadata in self.catalog_metadata.items():
            message_types = group_metadata.get("datasets", {})
            if len(message_types) == 1:
                # Auto-select the single message_type if only one exists
                single_type, single_metadata = next(iter(message_types.items()))
                datasets[group] = NNJADataset(single_metadata)
            else:
                # Retain message_type structure if there are multiple
                datasets[group] = {
                    msg_type: NNJADataset(msg_metadata)
                    for msg_type, msg_metadata in message_types.items()
                }
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
            list: A list of tuples identifying matching datasets.
                  Each tuple contains (group, message_type, dataset_name).
                  If a group has only one message_type, message_type is None.
        """
        results = []
        for group, group_metadata in self.catalog_metadata.items():
            datasets = group_metadata.get("datasets", {})
            for message_type, dataset in datasets.items():
                if any(queryterm.lower() in str(value).lower() for value in dataset.values()):
                    if len(datasets) == 1:  # Auto-select single message_type
                        results.append((group, None, dataset["name"]))
                    else:
                        results.append((group, message_type, dataset["name"]))
        return results

    def get_dataset(self, result):
        """
        Fetch a specific NNJADataset object based on a search result.

        Args:
            result (tuple): A result tuple from `search()`, e.g., (group, message_type, name).

        Returns:
            NNJADataset: The requested dataset object, or None if not found.
        """
        group, message_type, _ = result
        if message_type is None:  # Auto-select for single message_type
            return self.datasets.get(group)
        return self.datasets.get(group, {}).get(message_type)
