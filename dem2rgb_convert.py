import numpy as np
import rasterio
import os
import glob
from pathlib import Path
from numba import njit
from tqdm import tqdm

# --- CONFIGURATION ---
# Folder containing input DEM files
INPUT_FOLDER = r""
# Folder where processed RGB files will be saved
OUTPUT_FOLDER = r""

# Ensure output directory exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

@njit
def convert_to_terrain_rgb(elevation):
    """
    Converts elevation data to Mapbox Terrain-RGB format.
    Formula: height = -10000 + ((R * 256 * 256 + G * 256 + B) * 0.1)
    """
    elevation_scaled = (elevation + 10000) / 0.1
    r = np.clip(np.floor(elevation_scaled / (256 * 256)), 0, 255).astype(np.uint8)
    g = np.clip(np.floor((elevation_scaled % (256 * 256)) / 256), 0, 255).astype(np.uint8)
    b = np.clip(elevation_scaled % 256, 0, 255).astype(np.uint8)
    return r, g, b

def process_single_file(input_path, output_path):
    try:
        # Open DEM file
        with rasterio.open(input_path, num_threads='all_cpus') as src:
            profile = src.profile
            profile.update(
                count=3,
                dtype=rasterio.uint8,
                nodata=0,
                driver='GTiff',        # Write in TIFF format
                BIGTIFF='YES'          # Enable BigTIFF support
            )

            # Create output file
            with rasterio.open(output_path, 'w', **profile) as dst:
                # Calculate total blocks for progress bar
                total_blocks = sum(1 for _ in src.block_windows(1))

                # Start progress bar for this file
                file_name = os.path.basename(input_path)
                with tqdm(total=total_blocks, desc=f"Processing {file_name}", unit="block") as pbar:
                    for ji, window in src.block_windows(1):
                        # Read data and apply conversions
                        elevation = src.read(1, window=window).astype(float)
                        
                        # Handle nodata (replace with 0)
                        elevation[elevation == src.nodata] = 0
                        
                        r, g, b = convert_to_terrain_rgb(elevation)

                        # Write RGB bands
                        dst.write(r, window=window, indexes=1)
                        dst.write(g, window=window, indexes=2)
                        dst.write(b, window=window, indexes=3)

                        # Update progress bar
                        pbar.update(1)

        print(f"Completed: {output_path}")

    except Exception as e:
        print(f"Error processing {input_path}: {e}")

def main():
    # Find all TIF files in the input directory
    tif_files = glob.glob(os.path.join(INPUT_FOLDER, "*.tif"))
    
    if not tif_files:
        print(f"No .tif files found in {INPUT_FOLDER}")
        return

    print(f"Found {len(tif_files)} files. Starting batch processing...")
    print(f"Input: {INPUT_FOLDER}")
    print(f"Output: {OUTPUT_FOLDER}")
    print("-" * 50)

    for file_path in tif_files:
        # Create output filename (e.g., filename_rgb.tif)
        output_name = f"{Path(file_path).stem}_rgb.tif"
        output_path = os.path.join(OUTPUT_FOLDER, output_name)
        
        process_single_file(file_path, output_path)
    
    print("-" * 50)
    print("All files processed.")

if __name__ == "__main__":
    main()