import os
import zipfile
import tempfile
import subprocess
import shutil
from pathlib import Path
import logging

logging.basicConfig(
    filename="tile_processing.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

VALID_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.tif', '.tiff', '.svs', '.dat'}


def extract_zip(zip_file):
    temp_dir = tempfile.mkdtemp(prefix="zip_extract_")
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
            file_list = [str(Path(temp_dir) / f) for f in zip_ref.namelist()]
        logging.info("ZIP file extracted to: %s", temp_dir)
        return temp_dir, file_list
    except zipfile.BadZipFile as exc:
        raise RuntimeError("Invalid ZIP file.") from exc


def pull_docker_image():
    try:
        subprocess.run(["docker", "pull", "mmunozag/pyhist"],
                       check=True, capture_output=True, text=True)
        logging.info("Pulled docker image: mmunozag/pyhist")
    except subprocess.CalledProcessError as e:
        logging.error("Failed to pull docker image: %s", e.stderr)
        raise RuntimeError("Failed to pull mmunozag/pyhist: %s" % e.stderr) from e


def run_pyhist_docker(image_path):
    parent_dir = image_path.parent
    output_root = parent_dir / "output"
    output_root.mkdir(exist_ok=True)

    cmd = [
        "docker", "run", "--rm",
        "--platform", "linux/amd64",
        "-v", f"{parent_dir}:/pyhist/images",
        "mmunozag/pyhist",
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
        "--output", f"/pyhist/images/output",
        f"/pyhist/images/{image_path.name}"
    ]

    logging.info("Running docker command: %s", ' '.join(cmd))
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        logging.info("PyHIST docker executed successfully for %s", image_path)
    except subprocess.CalledProcessError as e:
        logging.error("PyHIST docker failed for %s: %s", image_path, e.stderr)
        raise RuntimeError("PyHIST docker processing failed: %s" % e.stderr) from e

    image_folder = output_root / image_path.stem
    expected_tile_folder = image_folder / f"{image_path.stem}_tiles"
    return expected_tile_folder


def process_files(input_path):
    temp_dir = Path.cwd() / "temp_processing"
    temp_dir.mkdir(exist_ok=True)
    input_path = Path(input_path).resolve()

    image_tile_map = {}
    if input_path.suffix.lower() in VALID_EXTENSIONS:
        output_dir = run_pyhist_docker(input_path)
        tile_files = list(output_dir.glob("*.png"))
        if tile_files:
            image_tile_map[input_path.stem] = output_dir
        else:
            logging.warning("No PNG tiles found in %s", output_dir)
    elif input_path.suffix.lower() == ".zip":
        temp_dir, file_list = extract_zip(input_path)
        for file_path in file_list:
            file_ext = Path(file_path).suffix.lower()
            if file_ext in VALID_EXTENSIONS:
                output_dir = run_pyhist_docker(Path(file_path))
                tile_files = list(output_dir.glob("*.png"))
                if tile_files:
                    image_tile_map[Path(file_path).stem] = output_dir
                else:
                    logging.warning("No PNG tiles found in %s", output_dir)
    else:
        raise ValueError(f"Unsupported input file type: {input_path.suffix}. Expected .zip or {VALID_EXTENSIONS}")

    return image_tile_map


def create_output_zip(image_tile_map, output_zip_path):
    with zipfile.ZipFile(output_zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        for image_name, tile_dir in image_tile_map.items():
            for file in tile_dir.glob("*.png"):
                arcname = f"{image_name}/{file.name}"
                zipf.write(file, arcname)
    logging.info("Output ZIP created: %s", output_zip_path)


def main(input_path, output_zip):
    pull_docker_image()
    image_tile_map = process_files(input_path)
    create_output_zip(image_tile_map, output_zip)
    logging.info("Processing completed successfully.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Tile images using PyHIST docker.")
    parser.add_argument("--input", required=True, help="Path to the input ZIP file or single image.")
    parser.add_argument("--output_zip", required=True, help="Path to the output ZIP file with tiles.")
    args = parser.parse_args()
    main(args.input, args.output_zip)
