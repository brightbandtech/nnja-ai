from importlib.metadata import version

__version__ = version("nnja")

from nnja.catalog import DataCatalog
from nnja.dataset import NNJADataset
from nnja.variable import NNJAVariable

__all__ = ["DataCatalog", "NNJADataset", "NNJAVariable"]
