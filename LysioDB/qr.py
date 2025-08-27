import polars as pl
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.graphics.barcode.qr import QrCodeWidget
from reportlab.graphics.shapes import Drawing
from reportlab.graphics import renderPDF
from typing import Optional, List, Tuple
import os


class QR:
    def __init__(self, database):
        """
        Initialize the QR class with a reference to the database.

        Args:
            database: Instance of the Database class providing access to data and config.
        """
        self.database = database
        print("Initialization of QR object complete.")

    def generate_qr_codes(
        self,
        column: str,
        output_dir: str = "qr_codes",
        output_format: str = "png",
        qr_size: int = 100,
        error_correction: str = "H",
        qr_version: int = 1,
        pdf_fields: Optional[List[str]] = None,
        pdf_layout: Optional[Tuple[float, float, float, float]] = None,
    ) -> pl.DataFrame:
        """
        Generate QR codes from a specified column in the database DataFrame and save them as images or PDFs.

        Args:
            column (str): Column name in self.database.df containing strings to encode as QR codes.
            output_dir (str): Directory to save QR code images or PDFs. Defaults to 'qr_codes'.
            output_format (str): Output format ('png' for images, 'pdf' for PDFs with optional text fields).
                        Defaults to 'png'.
            qr_size (int): Size of the QR code in points (for PDFs) or pixels (for PNGs). Defaults to 100.
            error_correction (str): QR code error correction level ('L', 'M', 'Q', 'H'). Defaults to 'H'.
            qr_version (int): QR code version (1–40, controls data capacity). Defaults to 1.
            pdf_fields (Optional[List[str]]): List of additional DataFrame columns to include as text in PDFs.
                                             Defaults to None (QR code only).
            pdf_layout (Optional[Tuple[float, float, float, float]]): (qr_x, qr_y, text_x, text_y) coordinates for
                                                                    PDF layout (in points). Defaults to None
                                                                    (uses (100, 600, 100, 650)).

        Returns:
            pl.DataFrame: Updated DataFrame with a new column 'qr_path' containing paths to generated QR code files.
        """
        print(f"\n--- Generating QR codes from column '{column}' ---")

        # Validate inputs
        if column not in self.database.df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame.")
        if output_format not in ["png", "pdf"]:
            raise ValueError("output_format must be 'png' or 'pdf'.")
        if pdf_fields:
            for field in pdf_fields:
                if field not in self.database.df.columns:
                    raise ValueError(
                        f"PDF field column '{field}' not found in DataFrame."
                    )

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Initialize new column for QR code file paths
        df = self.database.df.with_columns(pl.lit(None).cast(pl.Utf8).alias("qr_path"))

        # Default PDF layout
        default_layout = (100, 600, 100, 650)  # qr_x, qr_y, text_x, text_y
        qr_x, qr_y, text_x, text_y = pdf_layout if pdf_layout else default_layout
        width, height = letter if output_format == "pdf" else (qr_size, qr_size)

        # Process each row
        qr_paths = []
        for idx, row in enumerate(df.iter_rows(named=True)):
            value = row[column]
            if value is None or str(value).strip() == "":
                print(
                    f"Warning: Skipping row {idx} due to null or empty value in '{column}'."
                )
                qr_paths.append(None)
                continue

            # Generate QR code
            qr = QrCodeWidget(str(value))
            qr.barWidth = qr_size
            qr.barHeight = qr_size
            qr.qrVersion = qr_version
            qr.errorCorrection = error_correction

            # Generate filename (use row index or a unique column if available)
            filename_base = f"qr_{row.get('ID', idx)}"
            if output_format == "png":
                output_path = os.path.join(output_dir, f"{filename_base}.png")
                d = Drawing(qr_size, qr_size)
                d.add(qr)
                d.save(formats=["png"], outDir=output_dir, fnRoot=filename_base)
            else:  # output_format == "pdf"
                output_path = os.path.join(output_dir, f"{filename_base}.pdf")
                c = canvas.Canvas(output_path, pagesize=letter)

                # Draw QR code
                d = Drawing(0, 0)
                d.add(qr)
                renderPDF.draw(d, c, qr_x, qr_y)

                # Add text fields if specified
                if pdf_fields:
                    y_offset = text_y
                    for field in pdf_fields:
                        c.drawString(text_x, y_offset, f"{field}: {row.get(field, '')}")
                        y_offset -= 20  # Space out text fields
                c.save()

            qr_paths.append(output_path)
            print(f"Generated QR code for row {idx}: {output_path}")

        # Update DataFrame with QR code paths
        df = df.with_columns(pl.Series("qr_path", qr_paths))

        # Store updated DataFrame
        self.database.df = df
        print(
            f"\n--- QR code generation complete. Added 'qr_path' column to DataFrame. ---"
        )
        return df
