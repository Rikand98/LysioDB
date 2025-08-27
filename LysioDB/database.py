from LysioDB.calculations import Calculations
from LysioDB.category import Category
from LysioDB.config import Config
from LysioDB.dashboard import Dashboard
from LysioDB.export import Export
from LysioDB.geo import Location
from LysioDB.identify import Identify
from LysioDB.metadata import Metadata
from LysioDB.power import Power
from LysioDB.transform import Transform
import polars as pl
import polars_readstat as pr
import pyreadstat as pystat


class Database:
    def __init__(self, sav_file: str, config: Config = None):
        """Initialize the processor by loading data from a .sav file."""
        self.df: pl.DataFrame
        self.lf: pl.LazyFrame
        self.meta: pystat.metadata_container
        _, self.meta = pystat.read_sav(sav_file)
        self.lf = pr.scan_readstat(sav_file)
        self.df = self.lf.collect()
        self.config = config or Config()

        self.question_df = pl.DataFrame()
        self.categories = pl.DataFrame()
        self.percentage_df = pl.DataFrame()
        self.ranked_dfs = dict(pl.DataFrame())
        self.index_df = pl.DataFrame()
        self.eni_df = pl.DataFrame()
        self.eni_percentage_df = pl.DataFrame()
        self.correlate_df = pl.DataFrame()
        self.open_text_df = pl.DataFrame()

        self.matrix = []

        self.calculations = Calculations(self)
        self.category = Category(self)
        self.dashboard = Dashboard(self)
        self.export = Export(self)
        self.geo = Location(self)
        self.identify = Identify(self)
        self.metadata = Metadata(self)
        self.power = Power(self)
        self.transform = Transform(self)
