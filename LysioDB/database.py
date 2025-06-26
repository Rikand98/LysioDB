from LysioDB.identify import Identify
from LysioDB.transform import Transform
from LysioDB.category import Category
from LysioDB.calculations import Calculations
from LysioDB.export import Export
from LysioDB.config import Config
from LysioDB.metadata import Metadata
from LysioDB.power import Power
import polars as pl
import pyreadstat as pystat


class Database:
    def __init__(self, sav_file: str, config: Config = None):
        """Initialize the processor by loading data from a .sav file."""
        self.df: pl.DataFrame
        self.meta: pystat.metadata_container
        dataframe, self.meta = pystat.read_sav(sav_file)
        self.df = pl.DataFrame(dataframe)
        self.config = config or Config()

        self.question_df = pl.DataFrame()
        self.categories = pl.Series()
        self.percentage_df = pl.DataFrame()
        self.ranked_df = pl.DataFrame()
        self.index_df = pl.DataFrame()
        self.eni_df = pl.DataFrame()
        self.eni_percentage_df = pl.DataFrame()
        self.correlate_df = pl.DataFrame()
        self.open_text_df = pl.DataFrame()

        self.matrix = []

        self.metadata = Metadata(self)
        self.identify = Identify(self)
        self.category = Category(self)
        self.calculations = Calculations(self)
        self.transform = Transform(self)
        self.export = Export(self)
        self.power = Power(self)
