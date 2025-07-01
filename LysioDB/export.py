import polars as pl
import pyreadstat as pystat
import matplotlib.pyplot as plt
from typing import Dict, List, Any, Tuple, Optional
from wordcloud import WordCloud
from xlsxwriter import Workbook
import spacy


class Export:
    def __init__(self, database):
        self.database = database

    def excel(self, file_path="result.xlsx"):
        """
        Exports calculated results to an Excel file with separate sheets
        for different data structures if they are not empty.

        Args:
            file_path (str): The path where the Excel file will be saved.
                             Defaults to "percentage_result.xlsx".
        """
        print(f"Attempting to save data to Excel file: {file_path}")

        sheets_to_write: Dict[str, pl.DataFrame] = {}
        if (
            self.database.question_df is not None
            and not self.database.question_df.is_empty()
        ):
            sheets_to_write["Questions"] = self.database.question_df
            print("Added 'Questions' sheet.")
        else:
            print("Question DataFrame is empty or None. Skipping 'Questions' sheet.")

        if (
            self.database.percentage_df is not None
            and not self.database.percentage_df.is_empty()
        ):
            sheets_to_write["Percentages"] = self.database.percentage_df.fill_nan(0)
            print("Added 'Percentages' sheet.")
        else:
            print(
                "Percentage DataFrame is empty or None. Skipping 'Percentages' sheet."
            )

        if (
            self.database.ranked_df is not None
            and not self.database.ranked_df.is_empty()
        ):
            sheets_to_write["Ranks"] = self.database.ranked_df
            print("Added 'Ranks' sheet.")
        else:
            print("Ranked DataFrame is empty or None. Skipping 'Ranks' sheet.")

        if self.database.index_df is not None and not self.database.index_df.is_empty():
            sheets_to_write["Index"] = self.database.index_df
            print("Added 'Index' sheet.")
        else:
            print("Index list is empty or None. Skipping 'Index' sheet.")

        if (
            self.database.correlate_df is not None
            and not self.database.correlate_df.is_empty()
        ):
            try:
                sheets_to_write["Correlate"] = self.database.correlate_df
                print("Added 'Correlation' sheet.")
            except Exception as e:
                print(
                    f"Warning: Could not convert correlation list to DataFrame. Skipping 'Correlation' sheet. Error: {e}"
                )
        else:
            print("Correlation list is empty or None. Skipping 'Correlation' sheet.")

        if self.database.eni_df is not None and not self.database.eni_df.is_empty():
            sheets_to_write["ENI"] = self.database.eni_df
            print("Added 'ENI' sheet.")
        else:
            print("ENI list is empty or None. Skipping 'ENI' sheet.")
        if (
            self.database.eni_percentage_df is not None
            and not self.database.eni_percentage_df.is_empty()
        ):
            sheets_to_write["ENI Percentages"] = self.database.eni_percentage_df
            print("Added 'ENI Percentages' sheet.")
        else:
            print(
                "ENI Percentages list is empty or None. Skipping 'ENI Percentages' sheet."
            )
        if (
            self.database.open_text_df is not None
            and not self.database.open_text_df.is_empty()
        ):
            try:
                sheets_to_write["Open Text"] = self.database.open_text_df
                print("Added 'Open Text' sheet.")
            except Exception as e:
                print(
                    f"Warning: Could not convert open_text dictionary to DataFrame. Skipping 'Open Text' sheet. Error: {e}"
                )
        else:
            print("Open text dictionary is empty or None. Skipping 'Open Text' sheet.")

        if sheets_to_write:
            try:
                with Workbook(file_path) as wb:
                    for sheet_name, df_to_write in sheets_to_write.items():
                        ws = wb.add_worksheet(sheet_name)
                        df_to_write.write_excel(
                            workbook=wb,
                            worksheet=ws,
                        )
                        print(f"Written data to sheet: '{sheet_name}'.")

                print(f"Excel file successfully saved at: {file_path}")
            except Exception as e:
                print(f"Error saving Excel file '{file_path}': {e}")
        else:
            print("No data to write to Excel. File not created.")

    def sav(self, file_path="exported_database.sav", create_column=None):
        """Export processed DataFrame with grouped category columns."""
        df = self.database.df.clone()

        if create_column:
            for new_col, source_cols in create_column.items():
                value_mapping = {}
                current_value = 1

                for col in source_cols:
                    value_mapping[col] = current_value
                    current_value += 1

                expr = pl.coalesce(
                    *[
                        pl.when(pl.col(col) == 1).then(pl.lit(value_mapping.get(col)))
                        for col in source_cols
                    ]
                )

                df = df.with_columns(expr.alias(new_col)).drop(source_cols)

                if hasattr(self.database.meta, "column_names"):
                    self.database.meta.column_names.append(new_col)
                    self.database.meta.column_labels.append(new_col)
                if hasattr(self.database.meta, "variable_value_labels"):
                    self.database.meta.variable_value_labels[new_col] = {
                        v: k for k, v in value_mapping.items()
                    }
                if hasattr(self.database.meta, "readstat_variable_types"):
                    self.database.meta.readstat_variable_types[new_col] = "F8"

        column_labels = {
            col: label
            for col, label in self.database.meta.column_names_to_labels.items()
            # if col in df.columns
        }

        variable_value_labels = {
            col: labels
            for col, labels in self.database.meta.variable_value_labels.items()
            # if col in df.columns
        }

        pystat.write_sav(
            df.to_pandas(),
            file_path,
            column_labels=column_labels,
            variable_value_labels=variable_value_labels,
        )

        print(f"Database exported to: {file_path}")

    def raw_data(self, file_path: str = "raw_data.xlsx"):
        """
        Export raw data to Excel, ensuring consistent sorting and excluding 'direct' category columns.
        Generates two sheets: 'Numeric Data' (raw values) and 'Labeled Data' (with value labels applied).
        Also calls _generate_codebook to add a 'Codebook' sheet.
        """
        print("\n--- Exporting Raw Data to Excel ---")

        sheets_to_write: Dict[str, pl.DataFrame] = {}

        direct_columns_to_drop = {
            category
            for category, config in self.database.config.category_map.items()
            if config.get("type") == "direct"
        }

        filtered_df = self.database.df.drop(
            [col for col in direct_columns_to_drop if col in self.database.df.columns]
            + list(self.database.config.category_map.keys())
        )

        if filtered_df is not None and not filtered_df.is_empty():
            sheets_to_write["Numeric Data"] = filtered_df
            print("Added 'Numeric Data' sheet.")

        labeled_df = filtered_df.clone()

        expressions = []
        for var, labels_map in self.database.meta.variable_value_labels.items():
            if var in labeled_df.columns:
                expressions.append(
                    pl.col(var).cast(pl.Utf8).replace(labels_map).alias(var)
                )

        if expressions:
            labeled_df = labeled_df.with_columns(expressions)
            print("Value labels applied to 'Labeled Data'.")
        else:
            print("No value labels to apply or relevant columns found for labeling.")

        rename_map = {
            col: label
            for col, label in self.database.meta.column_names_to_labels.items()
            if col in labeled_df.columns and label is not None
        }
        if rename_map:
            labeled_df = labeled_df.rename(rename_map)
            print("Columns renamed for 'Labeled Data'.")
        else:
            print("No columns to rename for 'Labeled Data'.")

        if labeled_df is not None and not labeled_df.is_empty():
            sheets_to_write["Labeled Data"] = labeled_df
            print("Added 'Labeled Data' sheet.")

        var_code = [
            var for var in filtered_df.columns if var in self.database.meta.column_names
        ]
        var_label = [
            self.database.meta.column_names_to_labels.get(var, var) for var in var_code
        ]
        var_type_dict = self.database.meta.readstat_variable_types
        value_labels_dict = self.database.meta.variable_value_labels

        var_type = [var_type_dict.get(var, "unknown") for var in var_code]

        codebook_data = []
        for i, var in enumerate(var_code):
            if var in value_labels_dict:
                for val, label in sorted(value_labels_dict[var].items()):
                    codebook_data.append([var, var_label[i], var_type[i], val, label])
            else:
                codebook_data.append([var, var_label[i], var_type[i], "", ""])

        codebook_df = pl.DataFrame(
            codebook_data,
            schema=[
                "Name",
                "Label",
                "Type",
                "Value",
                "Value Label",
            ],
        )

        if codebook_df is not None and not codebook_df.is_empty():
            sheets_to_write["Codebook"] = codebook_df
            print("Added 'Codebook' sheet.")

        if sheets_to_write:
            try:
                with Workbook(file_path) as wb:
                    for sheet_name, df_to_write in sheets_to_write.items():
                        ws = wb.add_worksheet(sheet_name)
                        df_to_write.write_excel(
                            workbook=wb,
                            worksheet=ws,
                        )
                        print(f"Written data to sheet: '{sheet_name}'.")

                print(f"Excel file successfully saved at: {file_path}")
            except Exception as e:
                print(f"Error saving Excel file '{file_path}': {e}")
        else:
            print("No data to write to Excel. File not created.")

        print(f"Raw data and codebook exported to: {file_path}")

    def generate_word_cloud(self):
        nlp = spacy.load("sv_core_news_lg")
        for question in self.database.question_df["question"]:
            if (
                self.database.question_df.filter(pl.col("question") == question)
                .select(pl.col("question_type"))
                .item(0, 0)
                == "open_text"
            ):
                question_label = self.database.question_sets.get(question, {}).get(
                    "column_labels", question
                )
                question_label = (
                    self.database.question_df.filter(pl.col("question") == question)
                    .select(pl.col("question_label"))
                    .item(0, 0)
                )
                responses = self.database.open_text_df.filter(
                    pl.col("question") == question
                )

                text = " ".join(responses)

                doc = nlp(text)

                clean_words = [
                    token.lemma_
                    for token in doc
                    if not token.is_stop and not token.is_punct and token.is_alpha
                ]

                clean_text = " ".join(clean_words)

                wordcloud = WordCloud(
                    width=1600,
                    height=800,
                    background_color="lightgrey",
                    colormap="gray",
                    contour_color="black",
                    contour_width=1,
                ).generate(clean_text)

                image_filename = f"{question}_wordcloud.png"
                plt.figure(figsize=(10, 5))
                plt.imshow(wordcloud, interpolation="bilinear")
                plt.axis("off")
                plt.title(question_label)

                plt.savefig(image_filename, bbox_inches="tight", dpi=300)
                plt.close()

                print(f"Saved word cloud for {question} as {image_filename}")

    def long_format(
        self,
        columns: List[str],
        file_path: str = "long_format_result.xlsx",
    ):
        """
        Transforms the percentage results DataFrame from wide to a 'long' format
        by unpivoting the dynamic category columns (e.g., 'Kvinna;18-29 år;Erikslustvägen-Linnégatan')
        and splitting the concatenated category strings into separate, named columns.

        The input DataFrame for this function is expected to be `self.database.percentage_df`,
        which is typically generated by the `percentages` calculation and has been pivoted
        such that the `Category_Value` (e.g., 'Kvinna;18-29 år;Erikslustvägen-Linnégatan')
        are column headers.

        Args:
            category_split_columns (List[str]): A list of new column names to
                                                assign to the parts of the split
                                                category string (e.g., ["Gender", "AgeGroup", "Location"]).
                                                The length of this list must match the expected number of parts
                                                in the concatenated category string (separated by ';').
            file_path (str): The path where the Excel file will be saved.
                             Defaults to "long_format_result.xlsx".
        """
        print(f"\n--- Exporting long format results to '{file_path}' ---")

        if (
            self.database.percentage_df is None
            or self.database.percentage_df.is_empty()
        ):
            print(
                "Percentage DataFrame is empty or None. Cannot generate long format. Skipping export."
            )
            return

        current_wide_df = self.database.percentage_df.clone()

        fixed_index_cols = [
            "question",
            "display_question_label",
            "answer_label",
            "metric_type",
        ]

        dynamic_category_cols = [
            col
            for col in current_wide_df.columns
            if col not in fixed_index_cols and col != "Totalt"
        ]

        if not dynamic_category_cols:
            print(
                "No dynamic category columns found in percentage_df. Already in a long-like format or no categories to split. Skipping."
            )
            return

        unpivoted_df = current_wide_df.unpivot(
            index=fixed_index_cols,
            on=dynamic_category_cols,
            variable_name="Concatenated_Category",
            value_name="Value",
        )

        unpivoted_df = unpivoted_df.fill_nan(0)

        if unpivoted_df.is_empty():
            print(
                "DataFrame became empty after unpivoting and dropping nulls. No data to export. Skipping."
            )
            return

        split_exprs = []
        for i, col_name in enumerate(columns):
            split_exprs.append(
                pl.col("Concatenated_Category")
                .str.split(";")
                .list.get(i)
                .alias(col_name)
            )

        long_format_df = unpivoted_df.with_columns(split_exprs)

        long_format_df = long_format_df.drop("Concatenated_Category")
        pivot_index_for_metric = [
            col for col in long_format_df.columns if col not in ["metric_type", "Value"]
        ]

        final_long_df = long_format_df.pivot(
            index=pivot_index_for_metric,
            columns="metric_type",
            values="Value",
            aggregate_function="first",
        )
        final_long_df = final_long_df.filter(pl.col("answer_label") != "Total")

        sheets_to_write: Dict[str, pl.DataFrame] = {
            "Long Format Results": final_long_df
        }

        try:
            with Workbook(file_path) as wb:
                for sheet_name, df_to_write in sheets_to_write.items():
                    ws = wb.add_worksheet(sheet_name)
                    df_to_write.write_excel(workbook=wb, worksheet=ws)
                    print(f"Written data to sheet: '{sheet_name}'.")

            print(f"Long format results successfully exported to: {file_path}")
        except Exception as e:
            print(
                f"Error exporting long format results to Excel file '{file_path}': {e}"
            )
            raise
