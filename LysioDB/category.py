import polars as pl


class Category:
    def __init__(self, database):
        self.database = database
        print("Initialization of Category object complete.")

    def create_categories(self) -> pl.DataFrame:
        print("\n--- Creating categories ---")
        base = {
            "label": ["category", "column"],
            "totalt": ["total", "1==1"],
        }
        full_data = {**base, **self.database.config.category_data}
        category_df = pl.DataFrame(full_data)

        labels = pl.Series(category_df.columns)[1:]
        types = pl.Series(category_df.filter(pl.col("label") == "category").row(0)[1:])
        columns = pl.Series(category_df.filter(pl.col("label") == "column").row(0)[1:])

        exprs = []
        categories = []

        column_unique_mask = types.is_in(["column", "unique"])
        if column_unique_mask.any():
            source_cols = columns.filter(column_unique_mask).to_list()

            for col, src_col in zip(labels.filter(column_unique_mask), source_cols):
                unique_values = (
                    self.database.df.lazy()
                    .select(pl.col(src_col).unique())
                    .collect()
                    .to_series()
                    .drop_nulls()
                )

                cat_type = (
                    category_df.filter(pl.col("label") == "category")
                    .select(pl.col(col))
                    .item(0, 0)
                )
                if cat_type == "column":
                    value_labels = self.database.metadata.get_value_labels(src_col)
                    for val in unique_values:
                        val_label = value_labels.get(val, str(val))
                        name = f"{val_label} {col}"
                        exprs.append(
                            pl.when(pl.col(src_col) == val)
                            .then(1)
                            .otherwise(None)
                            .cast(pl.Int32)
                            .alias(name)
                        )
                        categories.append(name)

                elif cat_type == "unique":
                    unique_values = unique_values.sort()
                    for val in unique_values:
                        exprs.append(
                            pl.when(pl.col(src_col) == val)
                            .then(1)
                            .otherwise(None)
                            .cast(pl.Int32)
                            .alias(str(val))
                        )
                        categories.append(str(val))

        double_mask = types == "double"
        if double_mask.any():
            for col in labels.filter(double_mask):
                cols = (
                    category_df.filter(pl.col("label").is_in(["column"]))
                    .select(pl.col(col))
                    .item(0, 0)
                )
                col1 = cols.split(":")[0]
                col2 = cols.split(":")[1]
                unique_combinations = (
                    self.database.df.lazy().select([col1, col2]).unique().collect()
                )

                for row in unique_combinations.iter_rows():
                    val1, val2 = row
                    name = f"{val1}:{val2}"
                    exprs.append(
                        pl.when((pl.col(col1) == val1) & (pl.col(col2) == val2))
                        .then(1)
                        .otherwise(None)
                        .cast(pl.Int32)
                        .alias(name)
                    )
                    categories.append(name)

        total_mask = types == "total"
        if total_mask.any():
            exprs.extend(
                pl.lit(1).cast(pl.Int32).alias(col) for col in labels.filter(total_mask)
            )
            categories.append("totalt")

        single_mask = types == "single"
        if single_mask.any():
            for col, cond in zip(
                labels.filter(single_mask), columns.filter(single_mask)
            ):
                try:
                    expr = eval(cond, {"pl": pl})
                    exprs.append(
                        pl.when(expr).then(1).otherwise(None).cast(pl.Int32).alias(col)
                    )
                    categories.append(col)
                except Exception as e:
                    print(f"Error processing {col}: {e}")

        if exprs:
            df = self.database.df.with_columns(exprs)
            self.database.categories = pl.Series("Categories", categories)
            self.database.df = df
            print("\n--- Categories created ---")
            return df

        return self.database.df
