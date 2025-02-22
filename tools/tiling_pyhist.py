import os
import zipfile
import tempfile
import subprocess
import shutil
from pathlib import Path

# Configure logging
import logging
logging.basicConfig(
    filename="tile_processing.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

VALID_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.tif', '.tiff', '.svs'}

def extract_zip(zip_file):
    """Extracts a ZIP file into a temporary directory."""
    temp_dir = tempfile.mkdtemp(prefix="zip_extract_")
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
            file_list = [os.path.join(temp_dir, f) for f in zip_ref.namelist()]
        logging.info(f"ZIP file extracted to: {temp_dir}")
        return temp_dir, file_list
    except zipfile.BadZipFile as exc:
        raise RuntimeError("Invalid ZIP file.") from exc

def pull_podman_image():
    """Pulls the mmunozag/pyhist Podman image if not already present."""
    try:
        subprocess.run(["podman", "pull", "mmunozag/pyhist"], check=True, capture_output=True, text=True)
        logging.info("Pulled Podman image: mmunozag/pyhist")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to pull Podman image: {e.stderr}")
        raise RuntimeError(f"Failed to pull mmunozag/pyhist: {e.stderr}") from e

def run_pyhist_podman(image_path):
    """Runs PyHIST via Podman on the given image with specific parameters."""
    image_path = os.path.abspath(image_path)
    image_name = os.path.splitext(os.path.basename(image_path))[0]
    ext = os.path.splitext(image_path)[1]  # Extract the extension
    current_dir = os.getcwd()  # Equivalent to $(pwd) in bash

    container_image_path = f"/pyhist/images/{image_name}{ext}"

    cmd = [
        "podman", "run", "--rm",
        "-v", f"{current_dir}:/pyhist/images",
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
        "--output", "images/",  # Kept for consistency, though PyHIST ignores it
        container_image_path
    ]

    logging.info(f"Running Podman command: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logging.info(f"PyHIST Podman executed successfully for {image_path}")
        logging.debug(f"PyHIST stdout: {result.stdout}")
        logging.debug(f"PyHIST stderr: {result.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"PyHIST Podman failed for {image_path}: {e.stderr}")
        raise RuntimeError(f"PyHIST Podman processing failed: {e.stderr}") from e

def process_files(input_path):
    """Processes either a ZIP file or a single image using PyHIST Podman."""
    temp_dir = tempfile.mkdtemp(prefix="process_")
    image_tile_map = {}
    
    try:
        ext = os.path.splitext(input_path)[1].lower()
        if ext in VALID_EXTENSIONS:
            image_name = os.path.splitext(os.path.basename(input_path))[0]
            run_pyhist_podman(input_path)
            # Tiles are in <image_name>/<image_name>_tiles/ at the root
            tile_dir = os.path.join(os.getcwd(), image_name, f"{image_name}_tiles")
            logging.info(f"Checking for tiles in: {tile_dir}")
            if os.path.exists(tile_dir):
                tile_files = [f for f in os.listdir(tile_dir) if f.endswith('.png')]
                logging.info(f"Found {len(tile_files)} PNG files in {tile_dir}: {tile_files}")
                if tile_files:
                    image_tile_map[image_name] = tile_dir
                else:
                    logging.warning(f"No PNG tiles found in {tile_dir}")
            else:
                logging.warning(f"Tile directory {tile_dir} does not exist")
        elif ext == '.zip':
            temp_dir, file_list = extract_zip(input_path)
            original_dir = os.getcwd()
            os.chdir(temp_dir)  # Change to temp_dir for ZIP processing
            try:
                for file_path in file_list:
                    file_ext = os.path.splitext(file_path)[1].lower()
                    if file_ext not in VALID_EXTENSIONS:
                        logging.info(f"Skipping non-image file: {file_path}")
                        continue
                    image_name = os.path.splitext(os.path.basename(file_path))[0]
                    run_pyhist_podman(file_path)
                    tile_dir = os.path.join(temp_dir, image_name, f"{image_name}_tiles")
                    logging.info(f"Checking for tiles in: {tile_dir}")
                    if os.path.exists(tile_dir):
                        tile_files = [f for f in os.listdir(tile_dir) if f.endswith('.png')]
                        logging.info(f"Found {len(tile_files)} PNG files in {tile_dir}: {tile_files}")
                        if tile_files:
                            image_tile_map[image_name] = tile_dir
                        else:
                            logging.warning(f"No PNG tiles found in {tile_dir}")
                    else:
                        logging.warning(f"Tile directory {tile_dir} does not exist")
            finally:
                os.chdir(original_dir)  # Restore original directory
        else:
            raise ValueError(f"Unsupported input file type: {ext}. Expected .zip or {VALID_EXTENSIONS}")
        
        return temp_dir, image_tile_map
    except Exception as e:
        raise e
    finally:
        if ext == '.zip':
            shutil.rmtree(temp_dir, ignore_errors=True)

def create_output_zip(image_tile_map, output_zip_path):
    """Creates a ZIP file containing tiles organized by image name."""
    with zipfile.ZipFile(output_zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        for image_name, tile_dir in image_tile_map.items():
            for root, _, files in os.walk(tile_dir):
                for file in files:
                    if file.endswith('.png'):
                        file_path = os.path.join(root, file)
                        arcname = os.path.join(image_name, os.path.basename(file_path))
                        zipf.write(file_path, arcname)
                        logging.info(f"Added {file_path} to ZIP as {arcname}")
            logging.info(f"Added tiles for {image_name} to ZIP")
    logging.info(f"Output ZIP created: {output_zip_path}")

def main(input_path, output_zip_path):
    pull_podman_image()
    temp_dir, image_tile_map = process_files(input_path)
    try:
        create_output_zip(image_tile_map, output_zip_path)
        if os.path.splitext(input_path)[1].lower() in VALID_EXTENSIONS:
            image_name = os.path.splitext(os.path.basename(input_path))[0]
            shutil.rmtree(os.path.join(os.getcwd(), image_name), ignore_errors=True)
    finally:
        if os.path.splitext(input_path)[1].lower() in VALID_EXTENSIONS:
            shutil.rmtree(temp_dir, ignore_errors=True)
        logging.info(f"Temporary directory cleaned up: {temp_dir}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Tile images from a ZIP file or single image using PyHIST Podman.")
    parser.add_argument("--zip_file", required=True, help="Path to the input ZIP file or single image.")
    parser.add_argument("--output_zip", required=True, help="Path to the output ZIP file with tiles.")
    
    args = parser.parse_args()
    main(args.zip_file, args.output_zip)
