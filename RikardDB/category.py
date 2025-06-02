import polars as pl


def _eval_polars_expr_string(expr_string, df):
    """Evaluates a string as a Polars expression against a DataFrame."""
    try:
        evaluated_expr = eval(expr_string, {"pl": pl, "df": df})
        if not isinstance(evaluated_expr, pl.Expr):
            print(
                f"Warning: Evaluated string '{expr_string}' did not result in a Polars Expression."
            )
            return pl.lit(False)
        return evaluated_expr
    except Exception as e:
        print(f"Error evaluating Polars expression string '{expr_string}': {e}")
        return pl.lit(False)


class Category:
    def __init__(self, database):
        self.database = database
        print("Initialization of Category object complete.")

    def create_categories(self):
        """
        Optimized categorization method using vectorized Polars operations.
        Creates new category columns based on config and updates metadata.
        """
        print("\n--- Creating Categories ---")

        new_columns_expressions = []
        new_category_metadata = {}

        for category, config in self.database.config.category_map.items():
            cat_type = config.get("type")
            new_column_expr = None

            new_category_metadata[category] = {
                "name_label": config.get("name_labels", category),
                "variable_type": config.get("variable_type", "unknown"),
                "value_labels": config.get("value_labels", {}),
            }

            if cat_type == "total":
                new_column_expr = pl.lit(1).alias(category)
                new_category_metadata[category]["variable_type"] = "int"

            elif cat_type == "conditional":
                default_value = config.get("default", None)
                conditions_list = config.get("conditions")

                if conditions_list:
                    conditional_expr = pl.lit(default_value)
                    for condition_cfg in reversed(conditions_list):
                        condition_string = condition_cfg.get("conditions")
                        value_to_assign = condition_cfg.get("value")

                        if condition_string is not None and value_to_assign is not None:
                            condition_polars_expr = _eval_polars_expr_string(
                                condition_string, self.database.df
                            )

                            if isinstance(condition_polars_expr, pl.Expr):
                                conditional_expr = (
                                    pl.when(condition_polars_expr)
                                    .then(pl.lit(value_to_assign))
                                    .otherwise(conditional_expr)
                                )
                            else:
                                print(
                                    f"Warning: Conditional category '{category}' condition '{condition_string}' did not evaluate to a boolean expression. Skipping this condition."
                                )
                        else:
                            print(
                                f"Warning: Conditional category '{category}' has an incomplete condition configuration. Skipping."
                            )

                    new_column_expr = conditional_expr.alias(category)

                else:
                    print(
                        f"Warning: Conditional category '{category}' has no conditions defined. Skipping."
                    )

            if new_column_expr is not None:
                new_columns_expressions.append(new_column_expr)

        if new_columns_expressions:
            self.database.df = self.database.df.with_columns(new_columns_expressions)
        else:
            print("No valid category columns to add to DataFrame.")

        for category, meta_info in new_category_metadata.items():
            if category in self.database.df.columns:
                self.database.meta.column_names.append(category)
                self.database.meta.column_labels.append(meta_info["name_label"])
                self.database.meta.readstat_variable_types[category] = meta_info[
                    "variable_type"
                ]
                self.database.meta.variable_value_labels[category] = meta_info[
                    "value_labels"
                ]
            else:
                print(
                    f"Metadata for category '{category}' not added as column was skipped."
                )

        self.database.df = self.database.df
        print("\n--- Categories created and metadata updated ---")

        return self.database.df
