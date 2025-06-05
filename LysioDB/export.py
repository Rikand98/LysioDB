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
            print(
                "Percentage DataFrame is empty or None. Skipping 'Percentages' sheet."
            )

        if (
            self.database.percentage_df is not None
            and not self.database.percentage_df.is_empty()
        ):
            sheets_to_write["Percentages"] = self.database.percentage_df
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

    def sav(self, file_path="exported_databas.sav"):
        """Export processed DataFrame back to .sav format, removing 'direct' category columns and their metadata."""

        direct_columns = {
            category
            for category, config in self.database.config.category_map.items()
            if config.get("type") == "direct"
        }

        filtered_df = self.database.df.drop(
            columns=[col for col in direct_columns if col in self.database.df.columns]
        )

        filtered_column_labels = {
            col: label
            for col, label in self.database.meta.column_names_to_labels.items()
            if col not in direct_columns
        }

        filtered_variable_value_labels = {
            col: labels
            for col, labels in self.database.meta.variable_value_labels.items()
            if col not in direct_columns
        }

        pystat.write_sav(
            filtered_df,
            file_path,
            column_labels=filtered_column_labels,
            variable_value_labels=filtered_variable_value_labels,
        )

        print(f"Database exported to: {file_path}")

    def raw_data(self, output_file: str = "raw_data.xlsx"):
        """
        Export raw data to Excel, ensuring consistent sorting and excluding 'direct' category columns.
        Generates two sheets: 'Numeric Data' (raw values) and 'Labeled Data' (with value labels applied).
        Also calls _generate_codebook to add a 'Codebook' sheet.
        """
        print("\n--- Exporting Raw Data to Excel ---")

        direct_columns_to_drop = {
            category
            for category, config in self.database.config.category_map.items()
            if config.get("type") == "direct"
        }

        filtered_df = self.database.df.drop(
            [col for col in direct_columns_to_drop if col in self.database.df.columns]
        )

        try:
            filtered_df.write_excel(
                output_file, sheet_name="Numeric Data", index=False, mode="w"
            )
            print(f"Sheet 'Numeric Data' written to {output_file}")
        except Exception as e:
            print(f"Error writing 'Numeric Data' sheet: {e}")
            return  # Exit if the first sheet fails

        labeled_df = filtered_df.clone()

        expressions = []
        for var, labels_map in self.database.meta.variable_value_labels.items():
            if var in labeled_df.columns:
                expressions.append(pl.col(var).replace(labels_map).alias(var))

        if expressions:
            labeled_df = labeled_df.with_columns(expressions)
            print("Value labels applied to 'Labeled Data'.")
        else:
            print("No value labels to apply or relevant columns found for labeling.")

        rename_map = {
            col: label
            for col, label in self.database.meta.column_names_to_labels.items()
            if col in labeled_df.columns  # Ensure column exists in current DataFrame
        }
        if rename_map:
            labeled_df = labeled_df.rename(rename_map)
            print("Columns renamed for 'Labeled Data'.")
        else:
            print("No columns to rename for 'Labeled Data'.")

        try:
            labeled_df.write_excel(
                output_file, sheet_name="Labeled Data", index=False, mode="a"
            )
            print(f"Sheet 'Labeled Data' written to {output_file}")
        except Exception as e:
            print(f"Error writing 'Labeled Data' sheet: {e}")

        self._generate_codebook(output_file, filtered_df.columns.to_list())

        print(f"Raw data and codebook exported to: {output_file}")

    def _generate_codebook(self, output_file: str, filtered_columns: List[str]):
        """
        Generate a codebook sheet for the exported Excel file, sorted by the column order in the data export.
        """
        print("Generating codebook sheet...")

        var_code = [
            var for var in filtered_columns if var in self.database.meta.column_names
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

        try:
            codebook_df.write_excel(
                output_file, sheet_name="Codebook", index=False, mode="a"
            )
            print("Sheet 'Codebook' written successfully.")
        except Exception as e:
            print(f"Error writing 'Codebook' sheet: {e}")

    def generate_word_cloud(self):
        nlp = spacy.load("sv_core_news_lg")
        for question, details in self.database.question_sets.items():
            if details.get("category") == "open_text":
                question_label = self.database.question_sets.get(question, {}).get(
                    "column_labels", question
                )
                responses = self.database.open_text.get(question, [])

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

    def tabel(self):
        return
