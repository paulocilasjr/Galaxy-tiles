import argparse
import subprocess
from pathlib import Path
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import zipfile

logging.basicConfig(
    filename="tile_processing.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

VALID_EXTENSIONS = {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".svs", ".dat"}


def run_pyhist_cli(script_path, image_path):
    """
    Run the local pyhist.py script on the given image.
    """
    script_path = Path(script_path).resolve()
    parent_dir = image_path.parent
    output_root = parent_dir / "output"
    output_root.mkdir(exist_ok=True)

    cmd = [
        "python3",
        str(script_path),
        "--patch-size", "512",
        "--content-threshold", "0.4",
        "--output-downsample", "4",
        "--borders", "0000",
        "--corners", "1010",
        "--percentage-bc", "1",
        "--k-const", "1000",
        "--minimum_segmentsize", "1000",
        "--save-patches",
        "--save-tilecrossed-image",
        "--info", "verbose",
        "--output", str(output_root),
        str(image_path),
    ]

    logging.info("Running pyhist CLI: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True, cwd=script_path.parent)
        logging.info("PyHIST CLI executed successfully for %s", image_path)
    except subprocess.CalledProcessError as e:
        logging.error("PyHIST CLI failed for %s: %s", image_path, e.stderr)
        raise RuntimeError(f"PyHIST processing failed: {e.stderr}") from e

    image_folder = output_root / image_path.stem
    expected_tile_folder = image_folder / f"{image_path.stem}_tiles"
    return expected_tile_folder


def append_to_zip(output_zip_path, original_name, tile_dir):
    original_base = Path(original_name).stem

    with zipfile.ZipFile(output_zip_path, "a", compression=zipfile.ZIP_DEFLATED) as zipf:
        for file in tile_dir.glob("*.png"):
            file_stem = file.stem
            parts = file_stem.split("_")
            tile_number = next((p for p in reversed(parts) if p.isdigit()), "0000")
            new_name = f"{original_base}_{tile_number}.png"
            arcname = f"{original_base}/{new_name}"
            zipf.write(file, arcname)

    logging.info("Appended tiles from %s to %s", original_base, output_zip_path)


def process_image(args):
    script_path, image_path_str, original_name, output_zip = args
    image_path = Path(image_path_str).resolve()
    output_zip_path = Path(output_zip).resolve()

    if not image_path.exists() or image_path.suffix.lower() not in VALID_EXTENSIONS:
        logging.warning("Skipping invalid or unsupported file: %s", image_path)
        return

    try:
        tile_dir = run_pyhist_cli(script_path, image_path)
        if tile_dir.exists():
            append_to_zip(output_zip_path, original_name, tile_dir)
        else:
            logging.warning("No tiles found for: %s", image_path)
    except Exception as e:
        logging.error("Failed processing %s: %s", image_path, str(e))


def main(input_path_pairs, output_zip, script_path, max_workers=None):
    if max_workers is None:
        max_workers = os.cpu_count() or 1
    max_workers = min(max_workers, os.cpu_count() or 1)

    args_list = [
        (script_path, img, name, output_zip)
        for img, name in input_path_pairs
    ]

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_image, args) for args in args_list]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error("Unhandled error in worker: %s", str(e))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Tile images using local PyHIST CLI and zip the results."
    )

    parser.add_argument(
        "--input",
        dest="input_paths",
        action="append",
        required=True,
        help="Paths to one or more input images.",
    )
    parser.add_argument(
        "--original_name",
        dest="original_names",
        action="append",
        required=True,
        help="Original names of the input images.",
    )
    parser.add_argument(
        "--output_zip",
        required=True,
        help="Path to the output ZIP file with tiles."
    )
    parser.add_argument(
        "--pyhist-script",
        dest="pyhist_script",
        default="pyhist.py",
        help="Path to the local pyhist.py script."
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Maximum number of parallel workers (defaults to CPU count)."
    )

    args = parser.parse_args()
    pairs = list(zip(args.input_paths, args.original_names))
    main(pairs, args.output_zip, args.pyhist_script, args.max_workers)
