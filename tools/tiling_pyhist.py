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

def pull_docker_image():
    """Pulls the mmunozag/pyhist Docker image if not already present."""
    try:
        subprocess.run(["docker", "pull", "mmunozag/pyhist"], check=True, capture_output=True, text=True)
        logging.info("Pulled Docker image: mmunozag/pyhist")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to pull Docker image: {e.stderr}")
        raise RuntimeError(f"Failed to pull mmunozag/pyhist: {e.stderr}") from e

def run_pyhist_docker(image_path, output_dir, patch_size, content_threshold, output_downsample):
    """Runs PyHIST via Docker on the given image."""
    os.makedirs(output_dir, exist_ok=True)

    image_path = os.path.abspath(image_path)
    output_dir = os.path.abspath(output_dir)
    image_name = os.path.basename(image_path)  # Extract the filename
    image_dir = os.path.dirname(image_path)    # Extract the parent directory

    # Get user and group IDs for permissions
    uid = os.getuid()
    gid = os.getgid()

    # Mount the parent directory and use the filename in the container path
    container_image_path = f"/pyhist/images/{image_name}"

    cmd = [
        "docker", "run", "--rm",
        "-v", f"{image_dir}:/pyhist/images",  # Mount the directory containing the image
        "-v", f"{output_dir}:/pyhist/output",
        "-v", "/etc/passwd:/etc/passwd:ro",
        "-u", f"{uid}:{gid}",
        "mmunozag/pyhist",
        "--patch-size", str(patch_size),
        "--content-threshold", str(content_threshold),
        "--output-downsample", str(output_downsample),
        "--save-patches",
        "--info", "verbose",
        "--output", "/pyhist/output",
        container_image_path  # Dynamically set the image path inside the container
    ]

    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logging.info(f"PyHIST Docker executed successfully for {image_path}")
        logging.debug(f"PyHIST stdout: {result.stdout}")
        logging.debug(f"PyHIST stderr: {result.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"PyHIST Docker failed for {image_path}: {e.stderr}")
        raise RuntimeError(f"PyHIST Docker processing failed: {e.stderr}") from e

def process_files(input_path, patch_size, content_threshold, output_downsample):
    """Processes either a ZIP file or a single image using PyHIST Docker."""
    temp_dir = tempfile.mkdtemp(prefix="process_")
    image_tile_map = {}

    try:
        ext = os.path.splitext(input_path)[1].lower()
        if ext in VALID_EXTENSIONS:
            # Single image file
            image_name = os.path.splitext(os.path.basename(input_path))[0]
            output_dir = os.path.join(temp_dir, "output", image_name)
            run_pyhist_docker(input_path, output_dir, patch_size, content_threshold, output_downsample)
            tile_dir = output_dir
            if os.path.exists(tile_dir) and any(f.endswith('.png') for f in os.listdir(tile_dir)):
                image_tile_map[image_name] = tile_dir
            else:
                logging.warning(f"No tiles generated for {image_name}")
        elif ext == '.zip':
            temp_dir, file_list = extract_zip(input_path)
            for file_path in file_list:
                file_ext = os.path.splitext(file_path)[1].lower()
                if file_ext not in VALID_EXTENSIONS:
                    logging.info(f"Skipping non-image file: {file_path}")
                    continue
                image_name = os.path.splitext(os.path.basename(file_path))[0]
                output_dir = os.path.join(temp_dir, "output", image_name)
                run_pyhist_docker(file_path, output_dir, patch_size, content_threshold, output_downsample)
                tile_dir = output_dir
                if os.path.exists(tile_dir) and any(f.endswith('.png') for f in os.listdir(tile_dir)):
                    image_tile_map[image_name] = tile_dir
                else:
                    logging.warning(f"No tiles generated for {image_name}")
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
            logging.info(f"Added tiles for {image_name} to ZIP")
    logging.info(f"Output ZIP created: {output_zip_path}")

def main(input_path, output_zip_path, patch_size, content_threshold, output_downsample):
    """Main function to process input (ZIP or single image) and create a tiled output ZIP."""
    pull_docker_image()
    temp_dir, image_tile_map = process_files(input_path, patch_size, content_threshold, output_downsample)
    try:
        create_output_zip(image_tile_map, output_zip_path)
    finally:
        if os.path.splitext(input_path)[1].lower() in VALID_EXTENSIONS:
            shutil.rmtree(temp_dir, ignore_errors=True)
        logging.info(f"Temporary directory cleaned up: {temp_dir}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Tile images from a ZIP file or single image using PyHIST Docker.")
    parser.add_argument("--zip_file", required=True, help="Path to the input ZIP file or single image.")
    parser.add_argument("--output_zip", required=True, help="Path to the output ZIP file with tiles.")
    parser.add_argument("--patch-size", type=int, default=256, help="Size of the tiles (default: 256)")
    parser.add_argument("--content-threshold", type=float, default=0.05, help="Minimum tissue content threshold (default: 0.05)")
    parser.add_argument("--output-downsample", type=int, default=16, help="Downsample factor for overview (default: 16)")
    
    args = parser.parse_args()
    main(args.zip_file, args.output_zip, args.patch_size, args.content_threshold, args.output_downsample)
