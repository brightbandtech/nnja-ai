from importlib.metadata import version

__version__ = version("nnja-ai")

from nnja_ai.catalog import DataCatalog
from nnja_ai.dataset import NNJADataset
from nnja_ai.variable import NNJAVariable

__all__ = ["DataCatalog", "NNJADataset", "NNJAVariable"]
