import polars as pl
from typing import Dict, List, Union


class Category:
    def __init__(self, database):
        self.database = database
        print("Initialization of Category object complete.")

    def create_categories(self) -> pl.DataFrame:
        """
        Fixed vectorized processing with proper label handling.
        Resolves the ShapeError while maintaining your structure.
        """
        base = {
            "label": ["category", "condition"],
            "totalt": ["total", "1==1"],
        }
        full_data = {**base, **self.database.config.category_data}
        category_df = pl.DataFrame(full_data)

        labels = pl.Series(category_df.columns)[1:]
        types = pl.Series(category_df.filter(pl.col("label") == "category").row(0)[1:])
        conditions = pl.Series(
            category_df.filter(pl.col("label") == "condition").row(0)[1:]
        )

        exprs = []
        catagories = []

        expand_mask = types.is_in(["column", "unique"])
        if expand_mask.any():
            source_cols = (
                conditions.filter(expand_mask).str.extract(r"col\('(\w+)'\)").to_list()
            )

            for col, src_col in zip(labels.filter(expand_mask), source_cols):
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
                        name = f"{col}_{val_label}"
                        exprs.append(
                            pl.when(pl.col(src_col) == val)
                            .then(1)
                            .otherwise(None)
                            .cast(pl.Int32)
                            .alias(name)
                        )
                        catagories.append(name)

                elif cat_type == "unique":
                    for val in unique_values:
                        print(val)
                        exprs.append(
                            pl.when(pl.col(src_col) == val)
                            .then(1)
                            .otherwise(None)
                            .cast(pl.Int32)
                            .alias(str(val))
                        )
                        catagories.append(str(val))

        total_mask = types == "total"
        if total_mask.any():
            exprs.extend(
                pl.lit(1).cast(pl.Int32).alias(col) for col in labels.filter(total_mask)
            )
            catagories.append("totalt")

        single_mask = types == "single"
        if single_mask.any():
            for col, cond in zip(
                labels.filter(single_mask), conditions.filter(single_mask)
            ):
                try:
                    expr = eval(cond, {"pl": pl})
                    exprs.append(
                        pl.when(expr).then(1).otherwise(None).cast(pl.Int32).alias(col)
                    )
                    catagories.append(col)
                except Exception as e:
                    print(f"Error processing {col}: {e}")

        if exprs:
            df = self.database.df.with_columns(exprs)
            self.database.categories = pl.Series("Categories", catagories)
            self.database.df = df
            return df

        return self.database.df
