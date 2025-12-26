# Offline Cesium Globe Project

## Project Overview
This project is a Cesium-based globe application engineered to operate in a fully offline environment. It is designed for high-performance visualization of satellite imagery and 3D terrain data without requiring an active internet connection.

The system integrates with GeoServer to serve:
* Satellite Imagery: As WMS (Web Map Service) layers.
* 3D Terrain: Using RGB-encoded DEM (Digital Elevation Model) data, processed for seamless visualization in Cesium.

---

## Key Features
* 100% Offline Capability: Suitable for secure, isolated, or air-gapped networks.
* High-Resolution Terrain: Utilizes processed RGB terrain tiles for realistic 3D elevation.
* Custom Data Processing Tools: Includes Python utilities for downloading and converting global elevation data.

---

## Included Tools

This repository contains essential Python scripts for data preparation:

### 1. Copernicus DEM Turbo Downloader (copernicus-downloader.py)
A high-performance tool to download Copernicus DEM 30m tiles globally.
* Sources: AWS S3 (Ultra Fast) or HTTP.
* Features: Multi-threaded downloading, connection pooling, and pause/resume capability.
* Compression: Supports on-the-fly lossless TIFF compression (LZW, DEFLATE) to save storage space.

### 2. DEM to Terrain-RGB Converter (dem2rgb_convert.py)
A batch processing tool to convert standard elevation data (GeoTIFF) into Mapbox Terrain-RGB format, which is required for client-side terrain rendering in Cesium or Mapbox/Deck.gl.
* Batch Processing: Automatically finds and processes all .tif files in a specified input folder.
* Optimization: Uses numba for fast pixel-wise calculations and rasterio for efficient I/O.

---

## Installation

### Prerequisites
* Python 3.8 or higher
* GeoServer (for serving the processed data)

### Install Dependencies
Install the required Python libraries for the tools:

pip install numpy rasterio tqdm requests boto3 numba

(Note: boto3 is optional but recommended for faster AWS S3 downloads.)

---

## Usage

### Step 1: Download DEM Data
Run the downloader script to fetch raw elevation tiles for your area of interest.

python copernicus-downloader.py

* Follow the interactive prompts to select a continent and download method.
* Downloaded files will be saved in the copernicus_dem directory.

### Step 2: Convert to Terrain-RGB
After downloading (or merging) your DEM files, use the converter to prepare them for the web.

1. Open dem2rgb_convert.py.
2. Update the INPUT_FOLDER and OUTPUT_FOLDER variables:
   INPUT_FOLDER = r"C:\path\to\your\raw_dem_files"
   OUTPUT_FOLDER = r"C:\path\to\processed_rgb_files"
3. Run the script:
   python dem2rgb_convert.py

### Step 3: Serve via GeoServer
1. Upload the processed RGB TIF files to your GeoServer.
2. Publish them as a WMS layer or use a plugin to serve them as terrain tiles to your Cesium client.

