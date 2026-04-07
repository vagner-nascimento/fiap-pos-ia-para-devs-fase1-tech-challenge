import chardet
import duckdb as dd
import logging
from tqdm import tqdm
import os

logging.basicConfig(
    filename="conversion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)


def detect_encoding(file_path: str) -> str:
    """
    Detect the encoding of a file using chardet.

    Args:
        file_path (str): The path to the file for which to detect encoding.

    Returns:
        str: The detected encoding of the file.
    """
    try:
        with open(file_path, "rb") as f:
            raw_data = f.read(10000)  # Read the first 10KB for encoding detection

        encoding = chardet.detect(raw_data)["encoding"]
        return encoding
    except Exception as e:
        logging.error(
            f"An error occurred while detecting encoding for {file_path}: {e}"
        )
        return None  # Default to UTF-8 if detection fails


def to_utf8(file_path: str, encoding: str) -> str:
    """
    Convert a file to UTF-8 encoding.

    Args:
        file_path (str): The path to the input file.
        encoding (str): The current encoding of the file.

    Returns:
        str: The path to the UTF-8 encoded file.
    """
    total_size = os.path.getsize(file_path)

    utf8_file_path = file_path.replace(".csv", "_utf8.csv")
    try:
        with open(file_path, "r", encoding=encoding, errors="replace") as f_in, open(
            utf8_file_path, "w", encoding="utf-8", errors="replace"
        ) as f_out, tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            desc=f"Converting {file_path} to UTF-8",
        ) as pbar:
            for chunk in iter(lambda: f_in.read(1_000_000), ""):
                f_out.write(chunk)
                pbar.update(len(chunk.encode(encoding, errors="replace")))
        print(f"File {file_path} converted to UTF-8 and saved as {utf8_file_path}")
        return utf8_file_path
    except Exception as e:
        logging.error(f"An error occurred while converting {file_path} to UTF-8: {e}")
        return None  # Return original file path if conversion fails


def to_parquet(file_path: str, output_path: str) -> None:
    """
    Convert a CSV file to Parquet format using DuckDB.

    Args:
        file_path (str): The path to the input CSV file.
        output_path (str): The path where the output Parquet file will be saved.
    """
    print(f"Starting conversion of {file_path} to Parquet format...")
    print(f"Detecting file encoding for {file_path}...")
    encoding = detect_encoding(file_path)
    print(f"Detected encoding: {encoding}")
    if not encoding:
        logging.error(
            f"Failed to detect encoding for {file_path}. Aborting conversion."
        )
        return

    if encoding.lower() != "utf-8":
        print(f"Converting {file_path} to UTF-8 encoding...")
        file_path = to_utf8(file_path, encoding)
        print(f"File converted to UTF-8: {file_path}")

    if not file_path:
        logging.error(f"Failed to convert {file_path} to UTF-8. Aborting conversion.")
        return

    encoding = detect_encoding(file_path)

    if encoding.lower() != "utf-8":
        logging.error(
            f"Failed to convert {file_path} to UTF-8. Aborting conversion. Encoding detected: {encoding}"
        )
        print(f"Failed to convert {file_path} to UTF-8. Encoding detected: {encoding}.")
        return

    print(f"Converting {file_path} to {output_path}...")

    try:
        # Create a DuckDB connection
        dd.sql(
            f"COPY (SELECT * FROM read_csv('{file_path}', encoding='{encoding}')) TO '{output_path}' (FORMAT 'parquet')"
        )
        print(f"Successfully converted {file_path} to {output_path}")
    except Exception as e:
        logging.error(f"An error occurred while converting {file_path} to Parquet: {e}")
        print(
            f"Failed to convert {file_path} to Parquet. Check conversion.log for details."
        )
