import polars as pl
import plotly.graph_objects as go
import plotly.io as pio
import plotly.subplots as sp
import os
from typing import List, Dict, Optional, Tuple, Union


class Dashboard:
    def __init__(self, database):
        """
        Initialize the Dashboard class with a reference to the database.

        Args:
            database: Instance of the Database class providing access to data and config.
        """
        self.database = database
        self.default_node_color = "rgba(128, 128, 128, 0.8)"
        self.default_link_color = "rgba(128, 128, 128, 0.3)"
        print("Initialization of Dashboard object complete.")

    def create_sankey(
        self,
        question: str,
        metric_type: str = "count",
        categories: List[str] = ["Totalt"],
        answer_surfix: str = None,
        answer_prefix: str = None,
        exclude_answers: Optional[List[str]] = None,
        exclude_categories: Optional[List[str]] = None,
        output_file: str = "sankey_dashboard.html",
        title: str = "Sankey Diagram",
        node_colors: Optional[Dict[str, str]] = None,
        width: int = 2000,
        height: int = 1920,
    ) -> go.Figure:
        """
        Generate a Sankey diagram from percentage_df with customizable filters.

        Args:
            question (str): The question identifier (e.g., 'Q2') to filter percentage_df.
            metric_type (str): The metric type to filter (e.g., 'count', 'percentage'). Default is 'count'.
            exclude_answers (Optional[List[str]]): List of answer labels to exclude. Default is None.
            exclude_categories (Optional[List[str]]): List of categories to exclude. Default is None.
            output_file (str): Path to save the HTML output. Default is 'sankey_dashboard.html'.
            title (str): Title of the Sankey diagram. Default is 'Sankey Diagram'.
            node_colors (Optional[Dict[str, str]]): Dictionary mapping node labels to colors. Default is None.
            width (int): Width of the plot in pixels. Default is 2000.
            height (int): Height of the plot in pixels. Default is 1920.

        Returns:
            go.Figure: Plotly figure object for the Sankey diagram.
        """
        print(f"\n--- Generating Sankey diagram for question '{question}' ---")

        if (
            self.database.percentage_df is None
            or self.database.percentage_df.is_empty()
        ):
            print(
                "Percentage DataFrame is empty or None. Cannot generate Sankey diagram."
            )
            return go.Figure()

        # Clone percentage_df to avoid modifying the original
        df_to_melt = self.database.percentage_df.clone()

        # Define fixed columns
        id_columns = [
            "question",
            "display_question_label",
            "answer_label",
            "answer_value",
            "metric_type",
        ]
        value_columns = [col for col in df_to_melt.columns if col not in id_columns]

        # Melt the DataFrame to long format
        df_long = df_to_melt.melt(
            id_vars=id_columns,
            value_vars=value_columns,
            variable_name="Category",
            value_name="Percentage_Value",
        )
        df_long = df_long.filter(pl.col("question") == question)

        if answer_prefix:
            for label in df_long["answer_label"].unique():
                df_long = df_long.with_columns(
                    pl.col("answer_label")
                    .str.replace(label, f"{answer_prefix} {label}")
                    .alias("answer_label")
                )
                if label in exclude_answers:
                    new_label = f"{answer_prefix} {label}"
                    exclude_answers.append(new_label)

        if answer_surfix:
            for label in df_long["answer_label"].unique():
                df_long = df_long.with_columns(
                    pl.col("answer_label")
                    .str.replace(label, f"{label} {answer_surfix}")
                    .alias("answer_label")
                )
                if label in exclude_answers:
                    new_label = f"{label} {answer_surfix}"
                    exclude_answers.append(new_label)

        # Apply filters
        df_filtered = df_long.filter(
            (pl.col("question") == question)
            & (pl.col("metric_type") == metric_type)
            & (pl.col("Category").is_in(categories))
        )

        # Apply answer exclusions
        if exclude_answers:
            df_filtered = df_filtered.filter(
                ~pl.col("answer_label").is_in(exclude_answers)
            )

        # Apply category exclusions
        if exclude_categories:
            df_filtered = df_filtered.filter(
                ~pl.col("Category").is_in(exclude_categories)
            )

        if df_filtered.is_empty():
            print("Filtered DataFrame is empty. Returning empty figure.")
            fig = go.Figure()
            fig.update_layout(
                title_text="No data available for the selected filters.",
                height=300,
                width=500,
            )
            return fig

        # Create nodes
        question_labels = df_filtered["display_question_label"].unique().to_list()
        answer_labels = df_filtered["answer_label"].unique().to_list()
        categories = df_filtered["Category"].unique().to_list()

        node_labels = question_labels + answer_labels + categories
        node_to_index = {label: i for i, label in enumerate(node_labels)}

        # Apply node colors
        if node_colors is None:
            node_colors = {}
        plotly_node_colors = [
            node_colors.get(label, self.default_node_color) for label in node_labels
        ]

        # Create links from question to answer
        source_indices = []
        target_indices = []
        link_values = []
        link_colors = []

        df_question_to_answer_totals = df_filtered.group_by(
            ["display_question_label", "answer_label"]
        ).agg(pl.sum("Percentage_Value").alias("summed_value"))

        for row in df_question_to_answer_totals.iter_rows(named=True):
            source_label = row["display_question_label"]
            target_label = row["answer_label"]
            value = row["summed_value"]

            source_indices.append(node_to_index[source_label])
            target_indices.append(node_to_index[target_label])
            link_values.append(value)
            link_colors.append(node_colors.get(source_label, self.default_link_color))

        # Create links from answer to category
        for row in df_filtered.iter_rows(named=True):
            source_label = row["answer_label"]
            target_label = row["Category"]
            value = row["Percentage_Value"]

            source_indices.append(node_to_index[source_label])
            target_indices.append(node_to_index[target_label])
            link_values.append(value)
            link_colors.append(node_colors.get(target_label, self.default_link_color))

        # Create Sankey diagram
        fig = go.Figure(
            data=[
                go.Sankey(
                    node=dict(
                        pad=15,
                        thickness=20,
                        line=dict(color="black", width=0.5),
                        label=node_labels,
                        color=plotly_node_colors,
                    ),
                    link=dict(
                        source=source_indices,
                        target=target_indices,
                        value=link_values,
                        color=link_colors,
                    ),
                )
            ]
        )

        # Update layout
        fig.update_layout(
            title_text=title,
            font_size=8,
            height=height,
            width=width,
        )

        print("\n--- Sankey diagram generation complete ---")
        return fig

    def create_bar_chart(
        self,
        question: str,
        metric_type: str = "percentage",
        categories: Optional[List[str]] = None,
        output_file: str = "bar_chart_dashboard.html",
        title: str = "Bar Chart",
        width: int = 1600,
        height: int = 1200,
    ) -> go.Figure:
        """
        Generate a bar chart from percentage_df for a specific question and metric type.

        Args:
            question (str): The question identifier (e.g., 'Q2') to filter percentage_df.
            metric_type (str): The metric type to filter (e.g., 'percentage', 'count'). Default is 'percentage'.
            categories (Optional[List[str]]): List of categories to include. Default is None (all categories).
            output_file (str): Path to save the HTML output. Default is 'bar_chart_dashboard.html'.
            title (str): Title of the bar chart. Default is 'Bar Chart'.
            width (int): Width of the plot in pixels. Default is 1200.
            height (int): Height of the plot in pixels. Default is 800.

        Returns:
            go.Figure: Plotly figure object for the bar chart.
        """
        print(f"\n--- Generating bar chart for question '{question}' ---")

        if (
            self.database.percentage_df is None
            or self.database.percentage_df.is_empty()
        ):
            print("Percentage DataFrame is empty or None. Cannot generate bar chart.")
            return go.Figure()

        # Filter data
        df_filtered = self.database.percentage_df.filter(
            (pl.col("question") == question) & (pl.col("metric_type") == metric_type)
        )

        if categories:
            df_filtered = df_filtered.select(
                ["question", "display_question_label", "answer_label", "metric_type"]
                + categories
            )

        if df_filtered.is_empty():
            print("Filtered DataFrame is empty. Returning empty figure.")
            fig = go.Figure()
            fig.update_layout(
                title_text="No data available for the selected filters.",
                height=300,
                width=500,
            )
            return fig

        # Prepare data for bar chart
        traces = []
        for category in [
            col
            for col in df_filtered.columns
            if col
            not in ["question", "display_question_label", "answer_label", "metric_type"]
        ]:
            text_values = df_filtered.with_columns(
                (pl.col(category).mul(100).round(2).cast(pl.String) + "%")
                .fill_null("")
                .alias(f"{category}_text")
            )[f"{category}_text"]
            trace = go.Bar(
                x=df_filtered["answer_label"],
                y=df_filtered[category],
                name=category,
                text=text_values,
                textposition="auto",
            )
            traces.append(trace)

        # Create figure
        fig = go.Figure(data=traces)
        fig.update_layout(
            title_text=title,
            xaxis_title="Answer Options",
            yaxis_title=metric_type.capitalize(),
            barmode="group",
            font_size=16,
            width=width,
            height=height,
        )

        print("\n--- Bar chart generation complete ---")
        return fig

    def save(
        self,
        figures: List[go.Figure],
        output_file: str = "dashboard",
        output_format: Union[str, List[str]] = "html",
    ):
        """
        Save multiple Plotly figures as separate files (HTML and/or PDF).

        Args:
            figures (List[go.Figure]): List of Plotly figures to save.
            output_file (str): Base path for the output files (without extension). Files will be suffixed with index or title. Default is 'dashboard'.
            output_format (Union[str, List[str]]): Format(s) to save the figures ('html', 'pdf', or list of both). Default is 'html'.
        """
        print(f"\n--- Saving dashboard figures to '{output_file}' ---")
        os.makedirs("html", exist_ok=True)
        os.makedirs("pdf", exist_ok=True)
        if not figures:
            print("No figures provided to save.")
            return

        if isinstance(output_format, str):
            output_format = [output_format]

        for idx, fig in enumerate(figures):
            # Use figure title for file name, or fall back to index
            title = (
                fig.layout.title.text if fig.layout.title.text else f"figure_{idx + 1}"
            )
            # Sanitize title to make it file-name friendly
            title = "".join(
                c if c.isalnum() or c in ["_", "-"] else "_" for c in title
            ).strip("_")
            file_name = f"{output_file}_{title}"

            for fmt in output_format:
                try:
                    if fmt == "html":
                        fig.write_html(f"html/{file_name}.html")
                        print(f"Figure '{title}' saved to: {file_name}.html")
                    elif fmt == "pdf":
                        fig.write_image(f"pdf/{file_name}.pdf", engine="kaleido")
                        print(f"Figure '{title}' saved to: {file_name}.pdf")
                    else:
                        print(
                            f"Unsupported format '{fmt}'. Supported formats: 'html', 'pdf'."
                        )
                except Exception as e:
                    print(f"Error saving figure '{title}' to '{file_name}.{fmt}': {e}")

        print("\n--- Dashboard figures saved successfully ---")
