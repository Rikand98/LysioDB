import pandas as pd
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

    def raw_data(self, output_file="raw_data.xlsx"):
        """Export raw data to Excel, ensuring consistent sorting and excluding 'direct' category columns."""

        direct_columns = {
            category
            for category, config in self.database.config.category_map.items()
            if config.get("type") == "direct"
        }

        filtered_df = self.database.df.drop(
            columns=[col for col in direct_columns if col in self.database.df.columns]
        )

        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            filtered_df.to_excel(writer, sheet_name="Numeric Data", index=False)

            labeled_df = filtered_df.copy()
            for var, labels in self.database.meta.variable_value_labels.items():
                if var in labeled_df.columns:
                    labeled_df[var] = (
                        labeled_df[var].map(labels).fillna(labeled_df[var])
                    )

            labeled_df.rename(
                columns={
                    col: label
                    for col, label in self.database.meta.column_names_to_labels.items()
                    if col in filtered_df.columns
                },
                inplace=True,
            )

            labeled_df.to_excel(writer, sheet_name="Labeled Data", index=False)

            self._generate_codebook(writer, filtered_df.columns.tolist())

        print(f"Raw data exported to: {output_file}")

    def _generate_codebook(self, writer, filtered_columns):
        """Generate a codebook sorted by the column order in the data export."""
        print("Generating codebook...")

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

        codebook_df = pd.DataFrame(
            codebook_data, columns=["Name", "Label", "Type", "Value", "Value Label"]
        )
        codebook_df.to_excel(writer, sheet_name="Codebook", index=False)

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
