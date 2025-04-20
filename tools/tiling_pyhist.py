import argparse
import zipfile
import subprocess
from pathlib import Path
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import os

logging.basicConfig(
    filename="tile_processing.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

VALID_EXTENSIONS = {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".svs", ".dat"}


def run_pyhist_docker(image_path):
    parent_dir = image_path.parent
    output_root = parent_dir / "output"
    output_root.mkdir(exist_ok=True)

    cmd = [
        "docker",
        "run",
        "--rm",
        "--platform",
        "linux/amd64",
        "-v",
        f"{parent_dir}:/pyhist/images",
        "mmunozag/pyhist",
        "--patch-size",
        "512",
        "--content-threshold",
        "0.4",
        "--output-downsample",
        "4",
        "--borders",
        "0000",
        "--corners",
        "1010",
        "--percentage-bc",
        "1",
        "--k-const",
        "1000",
        "--minimum_segmentsize",
        "1000",
        "--save-patches",
        "--save-tilecrossed-image",
        "--info",
        "verbose",
        "--output",
        f"/pyhist/images/output",
        f"/pyhist/images/{image_path.name}",
    ]

    logging.info("Running docker command: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        logging.info("PyHIST docker executed successfully for %s", image_path)
    except subprocess.CalledProcessError as e:
        logging.error("PyHIST docker failed for %s: %s", image_path, e.stderr)
        raise RuntimeError(
            "PyHIST docker processing failed: %s" % e.stderr
        ) from e

    image_folder = parent_dir / "output" / image_path.stem
    expected_tile_folder = image_folder / f"{image_path.stem}_tiles"
    return expected_tile_folder


def append_to_zip(output_zip_path, original_name, tile_dir):
    # Strip extension from original name
    original_base = Path(original_name).stem

    with zipfile.ZipFile(
        output_zip_path, "a", compression=zipfile.ZIP_DEFLATED
    ) as zipf:
        for file in tile_dir.glob("*.png"):
            file_stem = file.stem  # Without .png extension

            # Get last numeric part as tile number
            parts = file_stem.split("_")
            tile_number = next(
                (p for p in reversed(parts) if p.isdigit()),
                "0000"
            )

            new_name = f"{original_base}_{tile_number}.png"
            arcname = f"{original_base}/{new_name}"
            zipf.write(file, arcname)

    logging.info(
        "Appended tiles from %s to %s",
        original_base, output_zip_path)


def process_image(args):
    image_path_str, original_name, output_zip = args
    input_path = Path(image_path_str).resolve()
    output_zip_path = Path(output_zip).resolve()

    if not input_path.exists() or \
       input_path.suffix.lower() not in VALID_EXTENSIONS:

        logging.warning("Skipping invalid or unsupported file: %s", input_path)
        return

    try:
        tile_dir = run_pyhist_docker(input_path)
        if tile_dir.exists():
            append_to_zip(output_zip_path, original_name, tile_dir)
        else:
            logging.warning("No tiles found for: %s", input_path)
    except Exception as e:
        logging.error("Failed processing %s: %s", input_path, str(e))


def main(input_path_pairs, output_zip):
    max_workers = min(4, os.cpu_count() or 1)  # Conservative cap for Galaxy
    args_list = [(img, name, output_zip) for img, name in input_path_pairs]

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_image, args) for args in args_list]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error("Unhandled error in parallel worker: %s", str(e))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Tile images using PyHIST docker and zip the results."
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

    args = parser.parse_args()
    main(list(zip(args.input_paths, args.original_names)), args.output_zip)
