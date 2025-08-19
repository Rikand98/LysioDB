import polars as pl
import polars.selectors as cs


class Identify:
    def __init__(self, database):
        self.database = database
        print("Initialization of Identify object complete.")

    def identify_questions(self):
        """Identify and categorize questions, storing results in a Polars DataFrame."""
        print("\n--- Identifying questions ---")
        config = self.database.config
        meta = self.database.meta
        metadata = self.database.metadata

        question_prefixes_tuple = tuple(config.QUESTION_PREFIXES)

        relevant_column_names = self.database.df.select(
            cs.starts_with(*question_prefixes_tuple)
        ).columns

        if not relevant_column_names:
            self.database.question_sets_df = pl.DataFrame()
            print("\n--- No relevant columns found with specified prefixes ---")
            return

        all_relevant_cols_df = pl.DataFrame(
            {"question": relevant_column_names}
        ).with_columns(pl.col("question").cast(pl.Utf8).alias("question"))

        numeric_columns_set = sorted(
            list(
                col
                for col, dtype in meta.readstat_variable_types.items()
                if dtype == "double"
            )
        )
        string_columns_set = sorted(
            list(
                col
                for col, dtype in meta.readstat_variable_types.items()
                if dtype == "string"
            )
        )
        numeric_string_columns_set = numeric_columns_set + string_columns_set

        df_categorized = all_relevant_cols_df.with_columns(
            pl.col("question").is_in(numeric_columns_set).alias("is_numeric")
        ).filter(pl.col("question").is_in(numeric_string_columns_set))

        if df_categorized.is_empty():
            self.database.question_sets_df = pl.DataFrame()
            print(
                "\n--- No relevant numeric or string columns found after type filtering ---"
            )
            return

        patterns_map = {
            "multi_response": config.MULTI_RESPONSE_PATTERN,
            "ranking": config.RANKING_PATTERN,
            "grid": config.GRID_PATTERN,
            "single_choice": config.SINGLE_CHOICE_PATTERN,
        }
        base_grid_pattern = config.BASE_GRID_PATTERN

        numeric_pattern_category_expr = pl.lit("numeric_other")

        for category, pattern in reversed(list(patterns_map.items())):
            condition = pl.col("question").str.contains(pattern, strict=False)
            numeric_pattern_category_expr = (
                pl.when(condition)
                .then(pl.lit(category))
                .otherwise(numeric_pattern_category_expr)
            )

        question_type_expr = (
            pl.when(pl.col("is_numeric"))
            .then(numeric_pattern_category_expr)
            .otherwise(pl.lit("open_text"))
        )

        df_categorized = df_categorized.with_columns(
            question_type_expr.alias("question_type")
        )

        base_question_expr = pl.col("question")

        for category, pattern in patterns_map.items():
            pattern_to_sub = (
                base_grid_pattern
                if category == "grid" and base_grid_pattern is not None
                else pattern
            )
            pattern_to_sub_lit = pl.lit(pattern_to_sub)

            base_question_expr = (
                pl.when(pl.col("question_type") == category)
                .then(
                    pl.col("question").str.replace_all(pattern_to_sub_lit, pl.lit(""))
                )
                .otherwise(base_question_expr)
            )

        df_categorized = df_categorized.with_columns(
            base_question_expr.alias("base_question")
        )

        df_categorized = df_categorized.with_columns(
            pl.when(pl.col("base_question") == "")
            .then(pl.col("question"))
            .otherwise(pl.col("base_question"))
            .alias("base_question")
        )

        try:
            all_labels_list = [
                {
                    "question": col,
                    "column_label": metadata.get_column_label(col),
                    "value_labels_info": metadata.get_value_labels(column=col),
                }
                for col in df_categorized["question"].to_list()
            ]

            cleaned_all_labels_list = []
            for item in all_labels_list:
                cleaned_item = item.copy()
                value_labels_info = cleaned_item.get("value_labels_info")
                if isinstance(value_labels_info, dict):
                    cleaned_value_labels_info = {
                        str(key) if isinstance(key, float) else key: value
                        for key, value in value_labels_info.items()
                    }
                    cleaned_item["value_labels_info"] = cleaned_value_labels_info
                    cleaned_item["value_labels"] = ", ".join(value_labels_info.values())
                cleaned_all_labels_list.append(cleaned_item)

            df_labels = pl.DataFrame(
                cleaned_all_labels_list,
                schema={
                    "question": pl.Utf8,
                    "column_label": pl.Utf8,
                    "value_labels_info": pl.Object,
                    "value_labels": pl.Utf8,
                },
            )

            df_categorized = df_categorized.join(df_labels, on="question", how="left")

        except Exception as e:
            print(f"Warning: Could not efficiently fetch metadata labels and join: {e}")
            print(
                "Proceeding without detailed column/value labels in separate columns for now."
            )
            df_categorized = df_categorized.with_columns(
                [
                    pl.lit("").alias("column_label"),
                    pl.lit(None).alias("value_labels_info"),
                ]
            )
        ls_condition = pl.col("column_label").str.starts_with("[")
        apply_split_condition = pl.col("question_type").is_in(
            ["grid", "multi_response"]
        )
        regex_pattern_ls = r"^(.*)(])(.*)$"
        ls_extracted_parts = pl.col("column_label").str.extract_groups(
            pattern=regex_pattern_ls
        )
        match_ls_successful_condition = ls_extracted_parts.struct[0].is_not_null()
        regex_pattern_multi = r"^(.*)( \d{1,2} = )(.*)$"
        multi_extracted_parts = pl.col("column_label").str.extract_groups(
            pattern=regex_pattern_multi
        )
        match_multi_successful_condition = multi_extracted_parts.struct[0].is_not_null()
        regex_pattern_grid = r"^(.*)( - )(.*)$"
        grid_extracted_parts = pl.col("column_label").str.extract_groups(
            pattern=regex_pattern_grid
        )
        match_grid_successful_condition = grid_extracted_parts.struct[1].is_not_null()

        base_question_label_expr = (
            pl.when(ls_condition & match_ls_successful_condition)
            .then(ls_extracted_parts.struct[2])
            .otherwise(
                pl.when(apply_split_condition & match_multi_successful_condition)
                .then(multi_extracted_parts.struct[0])
                .when(apply_split_condition & match_grid_successful_condition)
                .then(grid_extracted_parts.struct[0])
                .otherwise(pl.col("column_label"))
            )
        )

        question_label_expr = (
            pl.when(ls_condition & match_ls_successful_condition)
            .then(ls_extracted_parts.struct[0].str.strip_prefix("["))
            .otherwise(
                pl.when(apply_split_condition & match_multi_successful_condition)
                .then(multi_extracted_parts.struct[2])
                .when(apply_split_condition & match_grid_successful_condition)
                .then(grid_extracted_parts.struct[2])
                .otherwise(pl.col("column_label"))
            )
        )

        df_categorized = df_categorized.with_columns(
            [
                question_label_expr.alias("question_label"),
                base_question_label_expr.alias("base_question_label"),
            ]
        )

        df_categorized = df_categorized.drop(["is_numeric", "column_label"])

        self.database.question_df = df_categorized
        print("\n--- Question identification complete and stored in DataFrame ---")
