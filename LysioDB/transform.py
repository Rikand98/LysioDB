import polars as pl
import pyreadstat as pystat
import re
from thefuzz import process, fuzz


class Transform:
    def __init__(self, database):
        self.database = database

    def add_background_data(
        database_path: str,
        database_token: str,
        background_path: str,
        background_token: str,
        path: str,
    ) -> pl.DataFrame:
        """Load background data from .sav or .xlsx file and merge it using Polars."""
        print("\n--- Loading background data ---")
        database_df_pd, database_meta = pystat.read_sav(database_path)
        database_df = pl.from_pandas(database_df_pd)

        try:
            background_df = None
            background_meta = None

            if background_path.lower().endswith((".sav", ".zsav")):
                background_df_pd, background_meta = pystat.read_sav(background_path)
                background_df = pl.from_pandas(background_df_pd)
            elif background_path.lower().endswith((".xls", ".xlsx")):
                background_df = pl.read_excel(background_path)
                background_meta = None
            else:
                raise ValueError(
                    "Unsupported file format. Please provide a .sav, .zsav, .xls, or .xlsx file."
                )

            if background_df is None:
                raise RuntimeError(
                    "Failed to load background data into a Polars DataFrame."
                )

            if (
                database_token not in database_df.columns
                or background_token not in background_df.columns
            ):
                raise ValueError(
                    f"Both DataFrames must contain their specified token columns for merging. "
                    f"Main DataFrame needs '{database_token}' and "
                    f"Background DataFrame needs '{background_token}'."
                )
                raise ValueError(
                    "Both DataFrames must contain a 'token' column for merging."
                )

            database_df = database_df.join(
                background_df,
                left_on=database_token,
                right_on=background_token,
                how="left",
            )

            if background_meta is not None:
                database_meta.column_names_to_labels.update(
                    background_meta.column_names_to_labels
                )
                database_meta.variable_value_labels.update(
                    background_meta.variable_value_labels
                )
                database_meta.column_names.extend(
                    [
                        name
                        for name in background_meta.column_names
                        if name not in database_meta.column_names
                    ]
                )
                database_meta.column_labels.extend(
                    [
                        background_meta.column_names_to_labels.get(name, name)
                        for name in background_meta.column_names
                        if name not in database_meta.column_names
                    ]
                )
                database_meta.readstat_variable_types.update(
                    background_meta.readstat_variable_types
                )

            else:
                print(
                    "No detailed metadata available from the loaded file type (e.g., Excel). Metadata was not updated from the file."
                )

            pystat.write_sav(
                database_df.to_pandas(),
                path,
                column_labels=database_meta.column_names_to_labels,
                variable_value_labels=database_meta.variable_value_labels,
            )

            print("Background data loading and merging process completed.")

        except FileNotFoundError:
            print(f"Error: File not found at {background_path}")
            raise
        except ValueError as ve:
            print(f"Configuration or data error: {ve}")
            raise
        except Exception as e:
            print(
                f"An unexpected error occurred during background data processing: {e}"
            )
            raise

        print("Background data and metadata added successfully.")
        return database_df

    def map(database_path: str, old_database_paths: dict, new_path: str):
        """
        Standardizes old datasets to match the 2025 schema and merges them
        by matching question text (metadata labels) instead of column names.
        """
        database_df_pd, database_meta = pystat.read_sav(database_path)
        database_df = pl.from_pandas(database_df_pd)

        base_questions = database_meta.column_names_to_labels
        database_df = database_df.with_columns(pl.lit("2025").alias("year"))
        merged_dfs = [database_df]

        for year, path in old_database_paths:
            df, meta = pystat.read_sav(path)
            df = df.loc[:, ~df.columns.duplicated()]

            old_questions = meta.column_names_to_labels

            question_mapping = {}

            base_question_cols = dict.fromkeys(base_questions.keys()).keys()
            old_question_cols = dict.fromkeys(old_questions.keys()).keys()

            for old_col, old_label in old_questions.items():
                if old_label is not None and old_col not in question_mapping:
                    remaining_base_cols = base_question_cols - set(
                        question_mapping.values()
                    )

                    if old_label.startswith("["):
                        old_label_before = old_label.split("]")[1]
                        old_label_after = old_label.split("]")[0][1:]

                        for col in base_questions:
                            if (
                                col in remaining_base_cols
                                and old_col not in question_mapping
                            ):
                                base_question = base_questions[col]

                                if "=" in base_question or "-" in base_question:
                                    split_base_question = re.split(
                                        r"[-=]", base_question
                                    )
                                    base_before = split_base_question[0]
                                    base_after = (
                                        split_base_question[1]
                                        if len(split_base_question) > 1
                                        else ""
                                    )

                                    score_before = fuzz.partial_ratio(
                                        base_before, old_label_before
                                    )
                                    score_after = fuzz.partial_ratio(
                                        base_after, old_label_after
                                    )

                                    if score_before > 70 and score_after > 70:
                                        print(f"{old_label} ----> {base_question}")
                                        question_mapping[old_col] = col

                    elif "=" in old_label or "-" in old_label:
                        split_old_label = re.split(r"[-=]", old_label)
                        old_label_before = split_old_label[0]
                        old_label_after = (
                            split_old_label[1] if len(split_old_label) > 1 else ""
                        )

                        for col in base_questions:
                            if (
                                col in remaining_base_cols
                                and old_col not in question_mapping
                            ):
                                base_question = base_questions[col]

                                if "=" in base_question or "-" in base_question:
                                    split_base_question = re.split(
                                        r"[-=]", base_question
                                    )
                                    base_before = split_base_question[0]
                                    base_after = (
                                        split_base_question[1]
                                        if len(split_base_question) > 1
                                        else ""
                                    )

                                    score_before = fuzz.partial_ratio(
                                        base_before, old_label_before
                                    )
                                    score_after = fuzz.partial_ratio(
                                        base_after, old_label_after
                                    )

                                    if score_before > 70 and score_after > 70:
                                        print(f"{old_label} ----> {base_question}")
                                        question_mapping[old_col] = col

                    else:
                        matches = process.extract(
                            old_label,
                            [base_questions[col] for col in remaining_base_cols],
                            scorer=fuzz.partial_ratio,
                            limit=10,
                        )
                        best_match, best_score = None, 0
                        old_type = meta.readstat_variable_types[old_col]

                        for match, score in matches:
                            matched_col = next(
                                col
                                for col in remaining_base_cols
                                if base_questions[col] == match
                            )
                            matched_type = database_meta.readstat_variable_types[
                                matched_col
                            ]
                            if (best_match is None or score >= best_score) and (
                                old_type == matched_type or len(match) == len(old_label)
                            ):
                                best_match, best_score = match, score

                        if best_match:
                            matched_col = next(
                                col
                                for col in remaining_base_cols
                                if base_questions[col] == best_match
                            )

                            if (
                                best_score > 70
                                and database_meta.readstat_variable_types[matched_col]
                                == meta.readstat_variable_types[old_col]
                            ):
                                question_mapping[old_col] = matched_col
                                print(f"{old_label} ----> {best_match}")

                            else:
                                print(
                                    f"{year} - {old_col}: {old_label} did not find a strong match"
                                )

            df.rename(columns=question_mapping, inplace=True)
            df["year"] = year

            df = df.reindex(columns=database_df.columns, fill_value=None)

            merged_dfs.append(df)

        df_combined = pl.concat(merged_dfs, how="vertical")
        database_df = df_combined
        pystat.write_sav(
            database_df.to_pandas(),
            new_path,
            column_labels=database_meta.column_names_to_labels,
            variable_value_labels=database_meta.variable_value_labels,
        )

        return df_combined

    def map_2(self, old_df_paths: list) -> pl.DataFrame:
        """Standardizes old datasets and merges them."""
        print("\n--- Start mapping and renaming of data ---")
        base_questions = self.database.meta.column_names_to_labels
        self.database.df["year"] = "2025"
        merged_dfs = [self.database.df]

        for year, path in old_df_paths:
            df, meta = pystat.read_sav(path)
            df = df.loc[:, ~df.columns.duplicated()]
            question_mapping = self._get_question_mapping(df, meta, base_questions)

            df.rename(columns=question_mapping, inplace=True)
            df["year"] = year
            print(df)
            df = df.reindex(columns=self.database.df.columns, fill_value=None)
            merged_dfs.append(df)

        print("\n--- Finished ---")
        return pl.concat(merged_dfs, ignore_index=True)

    def _get_question_mapping(
        self, df: pl.DataFrame, meta, base_questions: dict
    ) -> dict:
        """Maps old columns to new base columns using question labels."""

        question_mapping = {}
        old_questions = meta.column_names_to_labels
        remaining_base_cols = set(base_questions.keys()) - set(df.columns)

        for old_col, old_label in old_questions.items():
            if old_label and old_col not in question_mapping:
                matches = process.extract(
                    old_label,
                    [base_questions[col] for col in remaining_base_cols],
                    scorer=fuzz.partial_ratio,
                    limit=10,
                )
                best_match, best_score = max(matches, key=lambda x: x[1])
                if best_score > 70:
                    question_mapping[old_col] = best_match
        return question_mapping
