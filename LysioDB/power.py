import os
from typing_extensions import dataclass_transform
import pptx
import re
import numpy as np
import polars as pl
from pptx.enum.shapes import MSO_SHAPE_TYPE
from pptx.enum.chart import XL_CHART_TYPE
from pptx.chart.chart import Chart
from pptx.chart.data import ChartData, XyChartData


class Power:
    def __init__(self, database):
        """
        Initializes the Builder with necessary data.

        Args:
            database (database): An instance of the database class providing access to data.
        """
        self.database = database
        self.matrix_counter = 0
        self.min_corr = None
        self.max_corr = None
        self.min_index = None
        self.max_index = None

    def update_pptx(self, category, template_path, output_dir="output_ppts"):
        """Generate a PowerPoint for each unique category."""

        print("\n--- Start generating Powerpoints ---")
        os.makedirs(output_dir, exist_ok=True)

        prs = pptx.Presentation(template_path)

        area_mapping = {
            f"area_{i}": area
            for i, area in enumerate(self.database.config.area_map.keys())
        }
        year_mapping = {
            f"year_{i}": year for i, year in enumerate(self.database.config.year_map)
        }
        self.matrix_counter = 0
        self.min_corr = None
        self.max_corr = None
        self.min_index = None
        self.max_index = None

        for slide in prs.slides:
            for shape in slide.shapes:
                if shape.has_text_frame:
                    self._parse_text_frame(
                        shape.text_frame, area_mapping, year_mapping, category
                    )

                elif shape.has_table:
                    self._update_table(
                        shape.table, area_mapping, year_mapping, category
                    )

                elif shape.has_chart:
                    self._update_chart(shape.chart, category)

        output_path = f"{output_dir}/powerpoint_{category}.pptx"
        prs.save(output_path)

        print(f"Generated PowerPoints in {output_dir}")

    def _parse_text_frame(self, text_frame, area_mapping, year_mapping, category):
        """Replace simple placeholders in text frames."""

        for paragraph in text_frame.paragraphs:
            for run in paragraph.runs:
                text = run.text
                placeholders = self._extract_placeholders(text)

                for placeholder in placeholders:
                    cat_match = re.match(r"category", placeholder)
                    freq_match = re.match(r"frequency", placeholder)
                    area_year_match = re.match(r"area_(\d+):year_(\d+)", placeholder)

                    if area_year_match:
                        year = area_year_match.group(0).split(":")[-1]
                        area = area_year_match.group(0).split(":")[0]
                        year_value = year_mapping.get(year)
                        area_name = area_mapping.get(area)
                        category_label = f"{year_value}:{category}"
                        if category_label in self.database.index_df["Category"]:
                            try:
                                result_value = (
                                    self.database.index_df.loc[
                                        self.database.index_df["Category"]
                                        == category_label,
                                        (slice(None), area_name),
                                    ]
                                    .values[0][0]
                                    .round(2)
                                )
                                result_value = self._format_number_swedish(result_value)

                                text = str(result_value)

                            except (IndexError, KeyError) as e:
                                print(
                                    f"Warning: Could not find data for {placeholder} in category '{category}' ({e})"
                                )

                    if cat_match:
                        text = category

                    if freq_match:
                        text = "test"
                # Replace placeholders using the replace_simple_placeholders function
                run.text = text

    def _update_table(self, table, area_mapping, year_mapping, category):
        """Update data in existing tables based on placeholders."""
        for row_idx, row in enumerate(table.rows):
            for col_idx, cell in enumerate(row.cells):
                text = cell.text
                placeholders = self._extract_placeholders(text)
                for placeholder in placeholders:
                    question_pattern = "|".join(
                        rf"{prefix}(.*)"
                        for prefix in self.database.config.QUESTION_PREFIXES
                    )
                    area_year_match = re.match(r"area_(\d+):year_(\d+)", placeholder)
                    area_match = re.match(r"area_(\d+)", placeholder)
                    year_match = re.match(r"year_(\d+)", placeholder)
                    question_match = re.search(
                        question_pattern,
                        placeholder,
                    )
                    nan_percentage_match = re.search("nan:(.*)", placeholder)
                    count_match = re.search(r"count:(.*)", placeholder)

                    if area_match:
                        area = area_match.group(0)
                        area_name = area_mapping.get(area)
                        self._update_cell_text(cell, str(area_name))

                    if year_match:
                        year_index = int(year_match.group(1))
                        year = year_match.group(0)
                        if 0 <= year_index < len(self.database.config.year_map):
                            year_value = year_mapping.get(year)
                            self._update_cell_text(cell, str(year_value))

                    if question_match:
                        year_match = re.search(":", question_match.group(0))
                        if year_match:
                            question = question_match.group(0).split(":")[0]
                            year = question_match.group(0).split(":")[-1]
                            year_value = year_mapping.get(year)
                            category_label = f"{year_value}:{category}"
                            if category_label in self.database.index_df["Category"]:
                                try:
                                    result_value = (
                                        self.database.index_df.loc[
                                            self.database.index_df["Category"]
                                            == category_label,
                                            (slice(None), question),
                                        ]
                                        .values[0][0]
                                        .round(2)
                                    )
                                    result_value = self._format_number_swedish(
                                        result_value
                                    )
                                    if result_value == "nan":
                                        result_value = "-"
                                    self._update_cell_text(cell, str(result_value))
                                except (IndexError, KeyError) as e:
                                    print(
                                        f"Warning: Could not find data for {placeholder} in category '{category}' ({e})"
                                    )
                        else:
                            question_part = question_match.group(0)
                            base_question = self._get_base_question(question_part)
                            question_label = self._get_column_label_for_sub_question(
                                base_question, question_part
                            )

                            self._update_cell_text(cell, str(question_label))

                    if area_year_match:
                        year = area_year_match.group(0).split(":")[-1]
                        area = area_year_match.group(0).split(":")[0]
                        year_value = year_mapping.get(year)
                        area_name = area_mapping.get(area)
                        category_label = f"{year_value}:{category}"
                        if category_label in self.database.index_df["Category"]:
                            try:
                                result_value = (
                                    self.database.index_df.loc[
                                        self.database.index_df["Category"]
                                        == category_label,
                                        (slice(None), area_name),
                                    ]
                                    .values[0][0]
                                    .round(2)
                                )
                                result_value = self._format_number_swedish(result_value)
                                if result_value == "nan":
                                    result_value = "-"

                                self._update_cell_text(cell, str(result_value))
                            except (IndexError, KeyError) as e:
                                print(
                                    f"Warning: Could not find data for {placeholder} in category '{category}' ({e})"
                                )

                    if nan_percentage_match:
                        question_part = nan_percentage_match.group(0).split(":")[-1]
                        year = year_mapping.get("year_0")
                        base_question = self._get_base_question(question_part)

                        try:
                            nan = (
                                self.database.percentage_df.filter(
                                    (pl.col("question") == question)
                                    & (pl.col("metric_type") == "percentage")
                                    & (
                                        pl.col("answer_value").is_in(
                                            self.database.config.NAN_VALUES.keys()
                                        )
                                    )
                                )
                                .select(pl.col(f"{year}:{category}"))
                                .item(0, 0)
                            )

                            if nan is not None:
                                nan_percentage_value = nan.values[0] * 100
                                self._update_cell_text(
                                    cell, f"{int(nan_percentage_value)}%"
                                )
                            else:
                                self._update_cell_text(cell, "0%")

                        except (KeyError, IndexError) as e:
                            print(
                                f"Warning: Could not find nan_percentage for {placeholder} in category '{category}' ({e})"
                            )
                            self._update_cell_text(cell, "N/A")

                    if count_match:
                        question_part = count_match.group(0).split(":")[-1]
                        year = year_mapping.get("year_0")
                        base_question = self._get_base_question(question_part)
                        try:
                            count_value = (
                                self.database.percentage_df.filter(
                                    (pl.col("question") == question)
                                    & (pl.col("metric_type") == "count")
                                    & (
                                        pl.col("answer_value").is_in(
                                            ~self.database.config.NAN_VALUES.keys()
                                        )
                                    )
                                )
                                .select(pl.col(f"{year}:{category}"))
                                .sum()
                            )

                            self._update_cell_text(cell, str(count_value))
                        except (KeyError, IndexError) as e:
                            print(
                                f"Warning: Could not find count for {placeholder} in category '{category}' ({e})"
                            )
                            self._update_cell_text(cell, "N/A")

    def _update_cell_text(self, cell, new_text):
        """Updates the text of a cell, preserving format of the first run."""
        text_frame = cell.text_frame
        if len(text_frame.paragraphs) > 0 and len(text_frame.paragraphs[0].runs) > 0:
            text_frame.paragraphs[0].runs[0].text = str(new_text)
        else:
            cell.text = str(new_text)

    def _update_chart(self, chart, category):
        chart_data = ChartData()
        year = self.database.config.year_map[0]
        category = f"{year}:{category}"

        if isinstance(chart, Chart):
            if chart.chart_type in [
                XL_CHART_TYPE.PIE,
                XL_CHART_TYPE.LINE,
                XL_CHART_TYPE.LINE_MARKERS,
                XL_CHART_TYPE.PIE_EXPLODED,
            ]:
                template_var_names = chart.plots[0].categories.flattened_labels
                var_names = tuple(label[0] for label in template_var_names)
                template_val_name_suffixes = tuple(
                    series.name for series in chart.series
                )

                var_labels = []
                all_value_labels = []
                all_series_values = {}
                for suffix in template_val_name_suffixes:
                    question_part = suffix
                    base_question = self._get_base_question(question_part)
                    var_labels.append(
                        self._get_column_label_for_sub_question(
                            base_question, question_part
                        )
                    )
                    series_values = []
                    val_labels = []
                    for var in var_names:
                        if var != "":
                            try:
                                var_num = float(var)
                                value = (
                                    self.database.percentage_df.filter(
                                        (pl.col("question") == question_part)
                                        & (pl.col("answer_value") == var_num)
                                    )
                                    .select(pl.col("Category") == category)
                                    .item(0, 0)
                                )
                                if value.empty:
                                    series_values.append("nan")
                                else:
                                    series_values.append(value.values[0])
                                if not all_value_labels:
                                    val_labels.append(
                                        self.database.question_sets.get(base_question)
                                        .get("value_labels")
                                        .get(var_num)
                                    )
                            except KeyError as e:
                                print(f"KeyError: {e}")
                            except ValueError as e:
                                print(f"ValueError: {e}")

                    all_series_values[suffix] = series_values
                    if not all_value_labels:
                        all_value_labels.append(val_labels)

                chart_data.categories = all_value_labels[0]
                for series_label, series_data in all_series_values.items():
                    chart_data.add_series(series_label, series_data)  # add each series

                chart.replace_data(chart_data)

            if chart.chart_type in [
                XL_CHART_TYPE.BAR_STACKED_100,
                XL_CHART_TYPE.BAR_CLUSTERED,
                XL_CHART_TYPE.COLUMN_CLUSTERED,
            ]:
                template_var_names = chart.plots[0].categories.flattened_labels
                var_names = tuple(label[0] for label in template_var_names)
                template_val_name_suffixes = tuple(
                    int(series.name) for series in chart.series
                )

                var_labels = []
                all_value_labels = []
                all_series_values = []
                for var in var_names:
                    question_part = var
                    base_question = self._get_base_question(question_part)
                    var_labels.append(
                        self._get_column_label_for_sub_question(
                            base_question, question_part
                        )
                    )
                    series_values = []
                    val_labels = []
                    for suffix in template_val_name_suffixes:
                        try:
                            var_num = float(suffix)
                            value = (
                                self.database.percentage_df.filter(
                                    (pl.col("question") == question_part)
                                    & (pl.col("answer_value") == var_num)
                                )
                                .select(pl.col("Category") == category)
                                .item(0, 0)
                            )
                            if value.empty:
                                series_values.append("nan")
                            else:
                                series_values.append(value.values[0])
                            if not all_value_labels:
                                val_labels.append(
                                    self.database.question_sets.get(base_question)
                                    .get("value_labels")
                                    .get(var_num)
                                )
                        except KeyError as e:
                            print(f"KeyError: {e}")
                        except ValueError as e:
                            print(f"ValueError: {e}")

                    all_series_values.append(series_values)
                    if not all_value_labels:
                        all_value_labels.append(val_labels)

                chart_data.categories = var_labels
                for i, suffix in enumerate(template_val_name_suffixes):
                    try:
                        series_label = all_value_labels[0][i]

                        series_data = [row[i] for row in all_series_values]

                        chart_data.add_series(series_label, series_data)

                    except (KeyError, ValueError, IndexError) as e:
                        print(f"Error adding series: {e}")

                chart.replace_data(chart_data)

            elif chart.chart_type == XL_CHART_TYPE.XY_SCATTER:
                chart_data = XyChartData()
                all_corr_values = []
                all_index_values = []
                if self.max_index is None:
                    self._get_min_max(category)

                chart.value_axis.minimum_scale = max(self.min_index - 0.2, 0)
                chart.value_axis.maximum_scale = min(self.max_index + 0.2, 5)
                chart.category_axis.minimum_scale = max(self.min_corr - 0.02, 0)
                chart.category_axis.maximum_scale = min(self.max_corr + 0.02, 1)
                chart.category_axis.visible = False
                chart.value_axis.visible = False

                for i, question in self.database.matrix[self.matrix_counter].items():
                    index = self.database.index.loc[
                        self.database.index["Category"] == category,
                        (slice(None), question),
                    ]
                    if index.empty:
                        index_value = 0
                    else:
                        index_value = index.values[0][0]
                    corr_df = self.database.correlation[
                        self.database.correlation["Category_Type"] == category
                    ]
                    corr_df = corr_df.set_index("index")  # sets index column as index
                    corr_value = corr_df.loc[question, "Correlation"]
                    if np.isnan(corr_value) or corr_value <= 0:
                        corr_value = 0
                    if np.isnan(index_value):
                        index_value = 0

                    all_corr_values.append(corr_value)
                    all_index_values.append(index_value)

                    matrix_series = chart_data.add_series(str(i))
                    matrix_series.add_data_point(corr_value, index_value)

                self.matrix_counter += 1
                chart.replace_data(chart_data)

            else:
                print(f"Warning: Chart type {chart.chart_type} is not supported.")

        else:
            print(
                f"Warning: Shape's chart attribute for category '{category}' is not a Chart object, but a {type(chart)}."
            )

    def _extract_placeholders(self, text):
        """Extract all placeholders from the text using regex."""
        brackets = re.findall(r"{(.*?)}", text)
        category = re.findall(r"category:category", text)
        if brackets:
            return brackets
        elif category:
            return category
        return [""]

    def _get_column_label_for_sub_question(self, base_question, column):
        """
        Retrieves the specific column label for a sub-question in a multi-response question.

        Args:
            base_question (str): The base question identifier (e.g., 'Q19').
            column (str): The specific column name (e.g., 'Q19C1').

        Returns:
            str or None: The column label for the given sub-question, or None if not found.
        """
        if base_question in self.database.question_df["base_question"]:
            try:
                index = self.database.question_sets[base_question]["column"].index(
                    column
                )
                return self.database.question_sets[base_question]["column_labels"][
                    index
                ].strip()
            except ValueError:
                return None
        return None

    def _format_number_swedish(self, number):
        """Formats a number using Swedish decimal separator (comma)."""
        if isinstance(number, (int, float)):
            return str(number).replace(".", ",")
        return str(number)

    def _get_base_question(self, question):
        if question in self.database.question_df["question"]:
            base_question = (
                self.database.question_df.filter(pl.col("question") == question)
                .select(pl.col("base_question"))
                .item(0, 0)
            )
        return str(base_question)

    def _get_min_max(self, category):
        area_map = self.database.config.area_map.copy()
        first_key = next(iter(area_map))
        del area_map[first_key]

        all_index = []
        all_corr = []
        for area, questions in area_map.items():
            for question in questions:
                index = self.database.index.loc[
                    self.database.index["Category"] == category,
                    (slice(None), question),
                ]
                if index.empty:
                    continue
                else:
                    index_value = index.values[0][0]

                corr_df = self.database.correlation[
                    self.database.correlation["Category_Type"] == category
                ]
                corr_df = corr_df.set_index("index")  # sets index column as index
                corr_value = corr_df.loc[question, "Correlation"]
                if np.isnan(corr_value) or np.isnan(index_value):
                    continue
                else:
                    all_index.append(index_value)
                    all_corr.append(corr_value)

        if not all_index:
            all_index.append(0)
            all_index.append(5)

        if not all_corr:
            all_corr.append(0)
            all_corr.append(1)

        self.max_corr = max(all_corr)
        self.min_corr = min(all_corr)
        self.min_index = min(all_index)
        self.max_index = max(all_index)
