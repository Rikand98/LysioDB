from LysioDB import database
import polars as pl
import plotly.graph_objects as go
import plotly.io as pio
import plotly.subplots as sp
import os
import copy
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

    def pie_chart(
        self,
        question: str,
        metric_type: str = "percentage",
        categories: Optional[List[str]] = None,
        exclude_answers: Optional[List[str]] = None,
        title: str = "Pie Chart",
        width: int = 3840,
        height: int = 2160,
        font_family: str = "Aptos",
        font_size: int = 8,
    ) -> go.Figure:
        """
        Generate a pie chart from percentage_df for a specific question and metric type.

        Args:
            question (str): The question identifier (e.g., 'Q2') to filter percentage_df.
            metric_type (str): The metric type to filter (e.g., 'percentage', 'count'). Default is 'percentage'.
            categories (Optional[List[str]]): List of categories to include. Default is None (all categories).
            exclude_answers (Optional[List[str]]): List of answer labels to exclude. Default is None.
            title (str): Title of the pie chart. Default is 'Pie Chart'.
            width (int): Width of the plot in pixels. Default is 1200.
            height (int): Height of the plot in pixels. Default is 800.
            font_family (str): Font family for the figure (e.g., 'Arial', 'Helvetica'). Default is 'Arial'.
            font_size (int): Font size for the figure. Default is 16.

        Returns:
            go.Figure: Plotly figure object for the pie chart.
        """
        print(f"\n--- Generating pie chart for question '{question}' ---")

        if (
            self.database.percentage_df is None
            or self.database.percentage_df.is_empty()
        ):
            print("Percentage DataFrame is empty or None. Cannot generate pie chart.")
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

        if exclude_answers:
            df_filtered = df_filtered.filter(
                ~pl.col("answer_label").is_in(exclude_answers)
            )

        if df_filtered.is_empty():
            print("Filtered DataFrame is empty. Returning empty figure.")
            fig = go.Figure()
            fig.update_layout(
                title_text="No data available for the selected filters.",
                height=300,
                width=500,
                font=dict(family=font_family, size=font_size),
            )
            return fig

        # Aggregate data by answer_label for pie chart
        category_columns = [
            col
            for col in df_filtered.columns
            if col
            not in ["question", "display_question_label", "answer_label", "metric_type"]
        ]
        if not category_columns:
            print("No categories available for pie chart. Returning empty figure.")
            return go.Figure()

        # Sum across categories for each answer_label
        df_aggregated = df_filtered.group_by("answer_label").agg(
            pl.sum_horizontal(category_columns).alias("value")
        )

        # Format text as percentage
        df_aggregated = df_aggregated.with_columns(
            (pl.col("value").mul(100).round(2).cast(pl.String) + "%")
            .fill_null("")
            .alias("text")
        )

        # Create pie chart
        fig = go.Figure(
            data=[
                go.Pie(
                    labels=df_aggregated["answer_label"],
                    values=df_aggregated["value"],
                    text=df_aggregated["text"],
                    textinfo="label+percent",
                    textposition="auto",
                )
            ]
        )
        fig.update_layout(
            title_text=title,
            font=dict(family=font_family, size=font_size),
            width=width,
            height=height,
            showlegend=True,
        )

        print("\n--- Pie chart generation complete ---")
        return fig

    def likert(
        self,
        question: str,
        metric_type: str = "percentage",
        categories: Optional[List[str]] = None,
        exclude_answers: Optional[List[str]] = None,
        title: str = "Horizontal Stacked Bar Chart",
        width: int = 1400,
        height: int = 1000,
        font_family: str = "Aptos",
        font_size: int = 14,
        colors: Optional[List[str]] = None,
    ) -> go.Figure:
        """
        Generate a horizontal stacked bar chart from percentage_df, with categories on y-axis and answer labels on x-axis, using pre-normalized percentages summing to 100% per category. Adds a table to the right with columns 'Index', 'Answers', and optionally 'Nans' (if NaN data exists). Includes annotations for category labels, percentages, answer labels, and NaN percentages.

        Args:
            question (str): The question identifier (e.g., 'Q2') to filter percentage_df.
            metric_type (str): The metric type to filter (e.g., 'percentage', 'count'). Default is 'percentage'.
            categories (Optional[List[str]]): List of categories to include. Default is None (all categories).
            exclude_answers (Optional[List[str]]): List of answer labels to exclude. Default is None.
            title (str): Title of the bar chart. Default is 'Horizontal Stacked Bar Chart'.
            width (int): Width of the plot in pixels. Default is 1400 to accommodate table on the right.
            height (int): Height of the plot in pixels. Default is 1000.
            font_family (str): Font family for annotations and table (e.g., 'Aptos', 'Arial'). Default is 'Aptos'.
            font_size (int): Font size for annotations and table. Default is 14.
            colors (Optional[List[str]]): List of colors for answer labels. Default is None (uses ['DarkBlue', 'MediumBlue', 'DarkSlateBlue', 'mediumpurple', 'thistle']).

        Returns:
            go.Figure: Plotly figure object for the horizontal stacked bar chart with table.
        """
        print(
            f"\n--- Generating horizontal stacked bar chart for question '{question}' ---"
        )

        if (
            self.database.percentage_df is None
            or self.database.percentage_df.is_empty()
        ):
            print(
                "Percentage DataFrame is empty or None. Cannot generate horizontal stacked bar chart."
            )
            return go.Figure()

        # Filter data
        df_filtered = self.database.percentage_df.filter(
            (pl.col("question") == question)
        )

        if categories:
            df_filtered = df_filtered.select(
                [
                    "question",
                    "display_question_label",
                    "answer_label",
                    "metric_type",
                    "answer_value",
                ]
                + categories
            )

        if exclude_answers:
            df_filtered = df_filtered.filter(
                ~pl.col("answer_label").is_in(exclude_answers)
            )

        if df_filtered.is_empty():
            print("Filtered DataFrame is empty. Returning empty figure.")
            fig = go.Figure()
            fig.update_layout(
                title_text="No data available for the selected filters.",
                height=300,
                width=500,
                font=dict(family=font_family, size=font_size),
            )
            return fig

        # Get category columns
        category_columns = [
            col
            for col in df_filtered.columns
            if col
            not in [
                "question",
                "display_question_label",
                "answer_label",
                "metric_type",
                "answer_value",
            ]
        ]
        if not category_columns:
            print(
                "No categories available for horizontal stacked bar chart. Returning empty figure."
            )
            return go.Figure()

        # Get answer labels
        answer_labels = self.database.question_df.filter(
            pl.col("question") == question
        )["value_labels_info"][0]
        answer_labels = {
            k: v for k, v in answer_labels.items() if k != "nan" and k != "Total"
        }

        # Default colors if none provided
        default_colors = [
            "DarkBlue",
            "MediumBlue",
            "DarkSlateBlue",
            "mediumpurple",
            "thistle",
        ]
        colors = colors if colors else default_colors
        if len(colors) < len(answer_labels):
            colors = colors * (len(answer_labels) // len(colors) + 1)
        colors_list = colors[: len(answer_labels)]
        color_map = {
            key: colors_list[i] for i, (key, _) in enumerate(answer_labels.items())
        }

        # Create subplot figure
        fig = sp.make_subplots(
            rows=1,
            cols=2,
            column_widths=[0.8, 0.2],
            specs=[[{"type": "bar"}, {"type": "table"}]],
            horizontal_spacing=0.05,
            shared_yaxes=True,  # Align table rows with chart categories
        )

        # Compute NaN percentages for annotations
        nan_percentages = {}
        nans_data = (
            df_filtered.filter(
                (pl.col("metric_type") == "percentage")
                & (pl.col("answer_label") == "nan")
            )
            .select(category_columns)
            .to_dicts()
        )
        nans_available = bool(nans_data)
        if nans_available:
            nans_data = nans_data[0]
            for cat in category_columns:
                nan_percentages[cat] = round(nans_data.get(cat, 0) * 100, 2)

        # Create bar traces
        annotations = []
        for i, (key, answer) in enumerate(answer_labels.items()):
            percentages = (
                df_filtered.filter(
                    (pl.col("answer_label") == answer)
                    & (pl.col("metric_type") == "percentage")
                )
                .select(category_columns)
                .to_dicts()
            )
            if not percentages:
                continue
            percentages = percentages[0]
            values_percentage = [percentages.get(cat, 0) for cat in category_columns]

            bar_trace = go.Bar(
                x=values_percentage,
                y=category_columns,
                orientation="h",
                name=answer,
                marker=dict(
                    color=color_map[key], line=dict(color="ghostwhite", width=1)
                ),
            )
            fig.add_trace(bar_trace, row=1, col=1)

        # Add table trace
        table_values = [
            [
                round(
                    self.database.index_df.filter(pl.col("Category") == cat).item(
                        0, question
                    ),
                    2,
                )
                if hasattr(self.database, "index_df")
                and self.database.index_df is not None
                else i + 1
                for i, cat in enumerate(category_columns[::-1])
            ],
            [
                df_filtered.filter(
                    (pl.col("answer_label") == "Total")
                    & (pl.col("metric_type") == "count")
                    & (pl.col("answer_value") == "total")
                )
                .select(category_columns)
                .item(0, cat)
                or 0
                for cat in category_columns[::-1]
            ],
        ]
        if nans_available:
            table_values.append(
                [
                    (
                        df_filtered.filter(
                            (pl.col("answer_label") == "nan")
                            & (pl.col("metric_type") == "percentage")
                        )
                        .select(category_columns)
                        .item(0, cat)
                        or 0
                    )
                    * 100
                    for cat in category_columns[::-1]
                ]
            )

        cell_height = height // (len(category_columns) + 1)
        fig.add_trace(
            go.Table(
                header=dict(
                    values=["Index", "Answers", "Nans"]
                    if nans_available
                    else ["Index", "Answers"],
                    font=dict(family=font_family, size=font_size, color="dimgray"),
                    fill_color="ghostwhite",
                    align="center",
                    height=font_size * 2,
                ),
                cells=dict(
                    values=table_values,
                    font=dict(family=font_family, size=font_size, color="dimgray"),
                    fill_color="ghostwhite",
                    align="center",
                    height=cell_height,
                ),
            ),
            row=1,
            col=2,
        )

        for yd in category_columns:
            space = 0
            for i, (key, answer) in enumerate(answer_labels.items()):
                percentages = (
                    df_filtered.filter(
                        (pl.col("answer_label") == answer)
                        & (pl.col("metric_type") == "percentage")
                    )
                    .select(yd)
                    .to_dicts()
                )
                if not percentages:
                    continue
                value = percentages[0][yd]
                if value is not None and value >= 0.03:
                    annotations.append(
                        dict(
                            xref="x",
                            yref="y",
                            x=space + (value / 2),
                            y=yd,
                            text=f"{int(value * 100)}%",
                            font=dict(
                                family=font_family, size=font_size, color="ghostwhite"
                            ),
                            showarrow=False,
                        )
                    )
                space += value if value is not None else 0
            if nans_available:
                annotations.append(
                    dict(
                        xref="x",
                        yref="y",
                        x=100,
                        y=yd,
                        xanchor="left",
                        text=f"NaN: {nan_percentages.get(yd, 0):.2f}%",
                        font=dict(family=font_family, size=font_size, color="dimgray"),
                        showarrow=False,
                        align="left",
                    )
                )

        # Update layout
        fig.update_layout(
            title_text=title,
            xaxis=dict(
                range=[0, 1],  # Adjusted for scaled percentages
                tickvals=[0, 0.2, 0.4, 0.6, 0.8, 1],
                ticktext=["0%", "20%", "40%", "60%", "80%", "100%"],
                domain=[0.15, 0.8],  # Adjusted to prevent squeezing
            ),
            barmode="stack",
            paper_bgcolor="ghostwhite",
            plot_bgcolor="ghostwhite",
            font=dict(family=font_family, size=font_size),
            width=width,
            height=height,
            margin=dict(l=150, r=50, t=100, b=100),  # Adjusted for better spacing
            annotations=annotations,
        )

        print("\n--- Horizontal stacked bar chart generation complete ---")
        return fig

    def sankey(
        self,
        question: str,
        metric_type: str = "count",
        categories: List[str] = ["Totalt"],
        answer_surfix: str = None,
        answer_prefix: str = None,
        exclude_answers: Optional[List[str]] = None,
        exclude_categories: Optional[List[str]] = None,
        title: str = "Sankey Diagram",
        node_colors: Optional[Dict[str, str]] = None,
        width: int = 3840,
        height: int = 2160,
        font_family: str = "Aptos",
        font_size: int = 8,
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
            font_family=font_family,
            font_size=font_size,
            height=height,
            width=width,
        )

        print("\n--- Sankey diagram generation complete ---")
        return fig

    def bar_chart(
        self,
        question: str,
        metric_type: str = "percentage",
        categories: Optional[List[str]] = None,
        title: str = "Bar Chart",
        width: int = 3840,
        height: int = 2160,
        font_family: str = "Aptos",
        font_size: int = 8,
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
            font_family=font_family,
            font_size=font_size,
            width=width,
            height=height,
        )

        print("\n--- Bar chart generation complete ---")
        return fig

    def save(
        self,
        figures: List[go.Figure],
        output_file: str = "dashboard",
        format: Union[str, List[str]] = "html",
        font_size: str = 8,
    ):
        """
        Save multiple Plotly figures as separate files (HTML and/or PDF).

        Args:
            figures (List[go.Figure]): List of Plotly figures to save.
            output_file (str): Base path for the output files (without extension). Files will be suffixed with index or title. Default is 'dashboard'.
            output_format (Union[str, List[str]]): Format(s) to save the figures ('html', 'pdf', or list of both). Default is 'html'.
        """
        print(f"\n--- Saving dashboard figures to '{output_file}' ---")
        if not figures:
            print("No figures provided to save.")
            return

        if isinstance(format, str):
            format = [format]

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

            for fmt in format:
                try:
                    if fmt == "html":
                        os.makedirs("html", exist_ok=True)
                        fig.write_html(f"html/{file_name}.html")
                        print(f"Figure '{title}' saved to: {file_name}.html")
                    elif fmt == "pdf":
                        os.makedirs("pdf", exist_ok=True)
                        fig_static = copy.deepcopy(fig)
                        fig_static.update_layout(font_size=font_size)
                        fig_static.write_image(f"pdf/{file_name}.pdf", engine="kaleido")
                        print(f"Figure '{title}' saved to: {file_name}.pdf")
                    elif fmt == "png":
                        os.makedirs("png", exist_ok=True)
                        fig_static = copy.deepcopy(fig)
                        fig_static.update_layout(font_size=font_size)
                        fig_static.write_image(
                            f"png/{file_name}.png", engine="kaleido", scale=5
                        )
                        print(f"Figure '{title}' saved to: {file_name}.png")
                    else:
                        print(
                            f"Unsupported format '{fmt}'. Supported formats: 'html', 'pdf', 'png'."
                        )
                except Exception as e:
                    print(f"Error saving figure '{title}' to '{file_name}.{fmt}': {e}")

        print("\n--- Dashboard figures saved successfully ---")
