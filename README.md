# LysioDB

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![GitHub Repo](https://img.shields.io/badge/GitHub-Repo-black?logo=github)](https://github.com/Rikardp98/LysioDB)

LysioDB is a modular Python-based data analysis pipeline designed for processing survey data, particularly from SPSS `.sav` files. It leverages [Polars](https://pola.rs/) for high-performance data manipulation, integrates tools for statistical calculations, text analysis, visualizations, and automated reporting. The core entry point is the `Database` class, which orchestrates data loading, processing, and export functionalities through specialized classes like `Identify`, `Transform`, `Category`, `Calculations`, `Export`, `Config`, `Metadata`, `Power`, `Geo`, and `Dashboard`.

This package is ideal for survey analysis workflows, including weighting, correlation analysis, ENI (Employee Net Promoter Index) calculations, geospatial processing, and generating reports in Excel or PowerPoint.

## Key Features

- **Data Loading & Initialization**: Load `.sav` files into Polars DataFrames with metadata handling using `pyreadstat`.
- **Question Identification & Categorization**: Automatically detect question types (e.g., single-choice, multi-response, grid, open-text) and create categories for segmentation.
- **Data Transformations**: Merge background data, map schemas across datasets, and standardize columns using fuzzy matching (`thefuzz`).
- **Calculations**:
  - Iterative Proportional Fitting (IPF) for weighting using `ipfn`.
  - Percentage calculations, correlations, ranking, and ENI metrics.
  - Open-text extraction and processing.
- **Text Analysis**: NLP tasks with `spacy` and fuzzy matching for responses.
- **Geospatial Tools**: Address geocoding (using `geopy` with services like Nominatim, Photon, TomTom) and distance calculations.
- **Visualizations**: Generate charts (pie, bar, Likert, Sankey) using Plotly and word clouds with `wordcloud` and `matplotlib`.
- **Reporting & Export**:
  - Export to Excel (`xlsxwriter`) in wide or long formats.
  - Automate PowerPoint presentations (`python-pptx`) with dynamic tables, charts, and placeholders.
- **Modularity**: All classes are reusable and configurable via a `Config` object.

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/Rikardp98/LysioDB.git
   cd LysioDB
   ```

2. Install as a local editable pip package:
   ```
   pip install -e .
   ```


## Usage

### Basic Example
   ```
from LysioDB import Config, Database, Dashboard, Transform

Transform.add_background_data(
    database_path="current_db.sav",
    database_token="ResPIN",
    background_path="background_data.xlsx",
    background_token="Token",
    path="new_db.sav",
)
nan = {999: None}
category_data = {
    "Department": ["unique", "Department"],
    "Area_1": ["single", 'pl.col("Area").is_in([1,2,3])'],
    "Area_2": ["single", 'pl.col("Area").is_in([4,5,6])'],
    "gender": ["column", "gender"],
    "<30": ["single", f"pl.col('age) < {30}"],
    "30-39": ["single", "pl.col('age').is_between(30,39)"],
    "40-49": ["single", "pl.col('age').is_between(40,49)"],
    "50-59": ["single", "pl.col('age').is_between(50,59)"],
    "60-69": ["single", "pl.col('age').is_between(60,69)"],
    "70+": ["single", f"pl.col('age') >= {70}"],
}
area_map = {
    "NMI": ["Q1", "Q2"],
}
question_prefixes = []
question_map = {}

config = Config(
    category_data=category_data,
    question_prefixes=question_prefixes,
    question_map=question_map,
    nan_values=nan,
    area_map=area_map,
)
db = Database("database.sav", config=config)

db.identify.identify_questions()
db.category.create_categories()
db.calculations.index(correlate="NMI")
db.calculations.percentages()
db.calculations.open_text()
db.export.excel()

   ```
### Advanced Usage

- **Custom Transformations**: Use `db.transform.add_background_data(...)` to merge additional datasets or `db.transform.map(...)` for schema alignment across years.
- **Correlations & Indexing**: Call `db.calculations.correlate(...)` or `db.calculations.index(...)` for advanced stats.
- **Dashboard**: Save multiple figures with `db.dashboard.save(figures=[fig1, fig2], format=["html", "pdf"])`.
- **PowerPoint Automation**: Use placeholders like `{area_1:year_1}` in templates for dynamic updates.

For detailed method signatures and examples, refer to the docstrings in the source files (e.g., `calculations.py`, `power.py`).

## Dependencies

- Core: `numpy`, `polars`, `pyreadstat`
- Analysis: `ipfn` (weighting), `thefuzz` (fuzzy matching), `spacy` (NLP)
- Visualization: `matplotlib`, `wordcloud`, `plotly`
- Export: `xlsxwriter`, `python-pptx`
- Geospatial: `geopy`

## Contributing

Contributions are welcome! Please fork the repo, create a feature branch, and submit a pull request. Ensure code is well-documented and follows PEP8.

1. Fork the repository.
2. Create a branch: `git checkout -b feature/your-feature`.
3. Commit changes: `git commit -m 'Add your feature'`.
4. Push: `git push origin feature/your-feature`.
5. Open a pull request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

For questions or issues, open a GitHub issue or contact the maintainer at [rikard@rikand.com](mailto:rikard@rikand.com).
