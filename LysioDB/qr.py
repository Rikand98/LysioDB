import polars as pl
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.graphics.barcode.qr import QrCodeWidget
from reportlab.graphics.shapes import Drawing
from reportlab.graphics import renderPDF
from reportlab.lib.utils import ImageReader
from PyPDF2 import PdfReader, PdfWriter
from typing import Optional, List, Tuple, Dict
import os
import tempfile
import re


class QR:
    def __init__(self, database):
        """
        Initialize the QR class with a reference to the database.

        Args:
            database: Instance of the Database class providing access to data and config.
        """
        self.database = database
        print("Initialization of QR object complete.")

    def generate_qr(
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

    def generate_pdfs(
        excel_path: str,
        pdf_path: str,
        placeholders: Dict[str, str],
        qr_column: str,
        output_dir: str = "merged_pdfs",
        qr_size: int = 100,
        error_correct_level: str = "H",
        qr_version: int = 1,
        layout: Optional[Dict[str, Tuple[float, float]]] = None,
    ) -> pl.DataFrame:
        """
        Merge data from an Excel file into a PDF template, overlaying text for placeholders and adding QR codes.

        Args:
            excel_path (str): Path to the Excel file containing data (e.g., Förnamn, Efternamn, Adress, Postnummer, Postort, token).
            template_pdf (str): Path to the PDF template file to overlay text and QR codes.
            placeholders (Dict[str, str]): Dictionary mapping placeholder names (e.g., '<<förnamn>>') to DataFrame column names (e.g., 'Förnamn').
            qr_column (str): Column name containing strings to encode as QR codes.
            output_dir (str): Directory to save generated PDFs. Defaults to 'merged_pdfs'.
            qr_size (int): Size of the QR code in points. Defaults to 100.
            error_correct_level (str): QR code error correction level ('L', 'M', 'Q', 'H'). Defaults to 'H'.
            qr_version (int): QR code version (1–40, controls data capacity). Defaults to 1.
            layout (Optional[Dict[str, Tuple[float, float]]]): Dictionary mapping placeholders (including '{qr}') to (x, y) coordinates
                                                             in points. Defaults to None (uses predefined layout).

        Returns:
            pl.DataFrame: Updated DataFrame with a new column 'pdf_path' containing paths to generated PDFs.
        """
        print(
            f"\n--- Merging Excel data from '{excel_path}' into PDF template '{pdf_path}' with QR codes from '{qr_column}' ---"
        )

        # Load Excel data into Polars DataFrame
        try:
            df = pl.read_excel(excel_path)
        except Exception as e:
            raise ValueError(f"Failed to read Excel file '{excel_path}': {e}")

        # Validate inputs
        for placeholder, column in placeholders.items():
            if column not in df.columns:
                raise ValueError(
                    f"Column '{column}' for placeholder '{placeholder}' not found in Excel file."
                )
        if qr_column not in df.columns:
            raise ValueError(f"QR code column '{qr_column}' not found in Excel file.")
        if not os.path.exists(pdf_path):
            raise ValueError(f"Template PDF '{pdf_path}' not found.")
        if error_correct_level not in ["L", "M", "Q", "H"]:
            raise ValueError("error_correct_level must be 'L', 'M', 'Q', or 'H'.")

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Set default layout if none provided (in points, where 72 points = 1 inch)
        default_layout = {
            "<<förnamn>>": (100, 750),
            "<<efternamn>>": (100, 730),
            "<<adress>>": (100, 710),
            "<<postnummer>>": (100, 690),
            "<<postort>>": (100, 670),
            "<<token>>": (100, 650),
            "{qr}": (100, 500),
        }
        layout = layout if layout else default_layout

        # Validate layout
        if "{qr}" not in layout:
            raise ValueError("Layout must include '{qr}' key for QR code placement.")
        for placeholder in placeholders:
            if placeholder not in layout:
                raise ValueError(
                    f"Layout missing coordinates for placeholder '{placeholder}'."
                )

        # Initialize new column for PDF paths
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("pdf_path"))

        # Process each row
        pdf_paths = []
        for idx, row in enumerate(df.iter_rows(named=True)):
            value = row[qr_column]
            if value is None or str(value).strip() == "":
                print(
                    f"Warning: Skipping row {idx} due to null or empty value in '{qr_column}'."
                )
                pdf_paths.append(None)
                continue

            # Generate filename (use row index or a unique column if available)
            filename_base = f"merged_{row.get('ID', idx)}"
            output_path = os.path.join(output_dir, f"{filename_base}.pdf")

            # Create temporary PDF with new content
            temp_pdf = os.path.join(tempfile.gettempdir(), f"temp_{filename_base}.pdf")
            c = canvas.Canvas(temp_pdf, pagesize=letter)

            # Add text fields
            for placeholder, column in placeholders.items():
                if placeholder in layout:
                    x, y = layout[placeholder]
                    text_value = str(row.get(column, ""))
                    c.drawString(x, y, text_value)

            # Generate and add QR code
            qr = QrCodeWidget(str(value))
            qr.barWidth = qr_size
            qr.barHeight = qr_size
            qr.qrVersion = qr_version
            # qr.errorCorrectLevel = error_correct_level  # Fixed attribute

            qr_x, qr_y = layout["{qr}"]
            d = Drawing(0, 0)
            d.add(qr)
            renderPDF.draw(d, c, qr_x, qr_y)

            c.save()

            # Merge with template PDF
            template_reader = PdfReader(pdf_path)
            temp_reader = PdfReader(temp_pdf)
            writer = PdfWriter()

            # Assume template is single-page for simplicity
            template_page = template_reader.pages[0]
            temp_page = temp_reader.pages[0]
            template_page.merge_page(temp_page)
            writer.add_page(template_page)

            with open(output_path, "wb") as f:
                writer.write(f)

            # Clean up temporary file
            os.remove(temp_pdf)

            pdf_paths.append(output_path)
            print(f"Generated PDF for row {idx}: {output_path}")

        # Update DataFrame with PDF paths
        df = df.with_columns(pl.Series("pdf_path", pdf_paths))

        # Store updated DataFrame
        print(f"\n--- PDF merging complete. Added 'pdf_path' column to DataFrame. ---")
        return df
