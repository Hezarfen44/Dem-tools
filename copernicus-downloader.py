"""
Copernicus DEM Turbo Downloader v3.0
- Ultra fast download optimizations
- Lossless compression (LZW TIFF compression)
- Advanced multi-threaded downloading
- Smart connection management
"""

import requests
import time
import json
import threading
import signal
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging
from typing import Dict, List, Tuple
import os

# Optional libraries
try:
    import boto3
    import botocore
    from boto3.s3.transfer import TransferConfig
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("tqdm not found. For progress bar: pip install tqdm")

try:
    import rasterio
    from rasterio.enums import Compression
    RASTERIO_AVAILABLE = True
except ImportError:
    RASTERIO_AVAILABLE = False
    print("rasterio not found. For TIFF compression: pip install rasterio")

try:
    import zstandard as zstd
    ZSTD_AVAILABLE = True
except ImportError:
    ZSTD_AVAILABLE = False
    # ZSTD is optional - LZW will be used if not installed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('copernicus_turbo.log', encoding='utf-8')
    ]
)

class CopernicusAWSDownloader:
    """Copernicus DEM Downloader from AWS S3 - Ultra Optimized Edition"""
    
    BUCKET = 'copernicus-dem-30m'
    REGION = 'eu-central-1'
    BASE_URL = f'https://{BUCKET}.s3.{REGION}.amazonaws.com'
    
    CONTINENTS = {
        'europe': {
            'name': 'Europe',
            'bounds': {'min_lat': 35, 'max_lat': 72, 'min_lon': -25, 'max_lon': 40}
        },
        'asia': {
            'name': 'Asia',
            'bounds': {'min_lat': -10, 'max_lat': 80, 'min_lon': 25, 'max_lon': 180}
        },
        'north_america': {
            'name': 'North America',
            'bounds': {'min_lat': 15, 'max_lat': 72, 'min_lon': -170, 'max_lon': -50}
        },
        'south_america': {
            'name': 'South America',
            'bounds': {'min_lat': -56, 'max_lat': 15, 'min_lon': -82, 'max_lon': -34}
        },
        'africa': {
            'name': 'Africa',
            'bounds': {'min_lat': -35, 'max_lat': 37, 'min_lon': -20, 'max_lon': 55}
        },
        'oceania': {
            'name': 'Oceania',
            'bounds': {'min_lat': -50, 'max_lat': -10, 'min_lon': 110, 'max_lon': 180}
        }
    }
    
    def __init__(self, output_dir='copernicus_dem', max_workers=16, use_boto=True, 
                 compress=True, compression_type='LZW'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.max_workers = max_workers
        self.use_boto = use_boto and BOTO3_AVAILABLE
        self.compress = compress and RASTERIO_AVAILABLE
        self.compression_type = compression_type
        
        # Graceful shutdown
        self.shutdown_event = threading.Event()
        signal.signal(signal.SIGINT, self.signal_handler)
        
        # OPTIMIZATION: Larger connection pool
        self.sessions = [requests.Session() for _ in range(max_workers)]
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=max_workers * 2,
            pool_maxsize=max_workers * 2,
            max_retries=3,
            pool_block=False
        )
        for session in self.sessions:
            session.mount('https://', adapter)
            session.mount('http://', adapter)
            session.headers.update({
                'User-Agent': 'CopernicusTurboDownloader/3.0',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            })
        
        # OPTIMIZATION: Ultra fast S3 transfer settings
        if self.use_boto:
            try:
                transfer_config = TransferConfig(
                    multipart_threshold=1024 * 10,   # 10MB (earlier multipart)
                    max_concurrency=20,              # More parallel connections
                    multipart_chunksize=1024 * 10,   # 10MB
                    use_threads=True,
                    max_io_queue=1000,               # Larger I/O queue
                    io_chunksize=262144              # 256KB I/O chunks
                )
                
                s3_config = botocore.config.Config(
                    signature_version=botocore.UNSIGNED,
                    max_pool_connections=max_workers * 3,  # More connections
                    connect_timeout=10,
                    read_timeout=60,
                    retries={'max_attempts': 3, 'mode': 'adaptive'}
                )
                
                self.s3_client = boto3.client('s3', region_name=self.REGION, config=s3_config)
                self.transfer_config = transfer_config
                print(f"- AWS S3 client initialized (Ultra fast mode: {max_workers} workers, 20x parallel)")
            except Exception as e:
                print(f"Warning: Could not initialize S3 client, switching to HTTP: {e}")
                self.use_boto = False
        
        self.stats = {
            'success': 0, 'failed': 0, 'skipped': 0, 'verified': 0, 
            'total_size_mb': 0, 'compressed_size_mb': 0, 'compression_ratio': 0
        }
        self.stats_lock = threading.Lock()
        
        self.progress_file = self.output_dir / 'progress.json'
        self.completed_tiles = self.load_progress()
        
        print(f"- {max_workers} parallel workers (optimized)")
        print(f"- Output directory: {self.output_dir}")
        if self.compress:
            print(f"- Compression: {compression_type} (lossless)")
        print(f"Info: Press Ctrl+C to pause")

    def signal_handler(self, sig, frame):
        if not self.shutdown_event.is_set():
            print("\nStop signal received! Finishing current downloads...")
            print("Press Ctrl+C again to force exit (not recommended).")
            self.shutdown_event.set()
        else:
            print("\nForcing exit...")
            exit(1)

    def load_progress(self):
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                    completed = set(data.get('completed', []))
                    if completed:
                        print(f"- {len(completed)} tiles previously downloaded, skipping.")
                    return completed
            except json.JSONDecodeError:
                print(f"Warning: progress.json is corrupt, starting fresh.")
                return set()
        return set()
    
    def save_progress(self):
        with self.stats_lock:
            with open(self.progress_file, 'w') as f:
                json.dump({
                    'completed': sorted(list(self.completed_tiles)),
                    'stats': self.stats,
                    'last_update': datetime.now().isoformat()
                }, f, indent=2)

    def get_tile_name(self, lat: int, lon: int) -> str:
        lat_dir = 'N' if lat >= 0 else 'S'
        lon_dir = 'E' if lon >= 0 else 'W'
        lat_str, lon_str = f"{abs(lat):02d}", f"{abs(lon):03d}"
        return f"Copernicus_DSM_COG_10_{lat_dir}{lat_str}_00_{lon_dir}{lon_str}_00_DEM"

    def get_s3_key(self, lat: int, lon: int) -> str:
        tile_name = self.get_tile_name(lat, lon)
        return f"{tile_name}/{tile_name}.tif"

    def generate_tiles(self, bounds: Dict) -> List[Tuple[str, int, int]]:
        tiles = []
        for lat in range(int(bounds['min_lat']), int(bounds['max_lat'])):
            for lon in range(int(bounds['min_lon']), int(bounds['max_lon'])):
                tiles.append((f"{lat}_{lon}", lat, lon))
        return tiles

    def compress_tiff(self, input_path: Path, output_path: Path) -> float:
        """Lossless TIFF compression"""
        try:
            with rasterio.open(input_path) as src:
                profile = src.profile.copy()
                
                # Compression settings
                compression_map = {
                    'LZW': Compression.lzw,
                    'DEFLATE': Compression.deflate,
                    'ZSTD': Compression.zstd if ZSTD_AVAILABLE else Compression.lzw,
                    'LZMA': Compression.lzma
                }
                
                profile.update(
                    compress=compression_map.get(self.compression_type, Compression.lzw),
                    predictor=2,  # For horizontal differencing
                    tiled=True,
                    blockxsize=512,
                    blockysize=512,
                    BIGTIFF='IF_SAFER'
                )
                
                with rasterio.open(output_path, 'w', **profile) as dst:
                    for i in range(1, src.count + 1):
                        dst.write(src.read(i), i)
            
            # Calculate compression ratio
            original_size = input_path.stat().st_size
            compressed_size = output_path.stat().st_size
            ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0
            
            # Remove original file
            input_path.unlink()
            
            return ratio
        except Exception as e:
            logging.error(f"Compression error {input_path}: {e}")
            if output_path.exists():
                output_path.unlink()
            if input_path.exists() and not output_path.exists():
                input_path.rename(output_path)
            return 0

    def verify_tiff(self, filepath: Path) -> bool:
        try:
            if filepath.stat().st_size < 1024:
                return False
            if RASTERIO_AVAILABLE:
                with rasterio.open(filepath) as src:
                    return src.width > 0 and src.height > 0 and src.count > 0
            else:
                with open(filepath, 'rb') as f:
                    header = f.read(4)
                    return header[:2] in [b'II', b'MM']
        except Exception as e:
            logging.error(f"TIFF verification error {filepath}: {e}")
            return False

    def download_tile(self, tile_id: str, lat: int, lon: int, continent: str, 
                     session: requests.Session = None) -> Tuple[str, float, float]:
        """Main download function - with compression"""
        if tile_id in self.completed_tiles:
            with self.stats_lock:
                self.stats['skipped'] += 1
            return 'skipped', 0, 0

        continent_dir = self.output_dir / continent
        continent_dir.mkdir(parents=True, exist_ok=True)
        filename = f"Copernicus_30m_{tile_id}.tif"
        filepath = continent_dir / filename
        
        if filepath.exists() and self.verify_tiff(filepath):
            file_size_mb = filepath.stat().st_size / (1024 * 1024)
            self.completed_tiles.add(tile_id)
            with self.stats_lock:
                self.stats['skipped'] += 1
                self.stats['verified'] += 1
                self.stats['compressed_size_mb'] += file_size_mb
            return 'exists', file_size_mb, file_size_mb
        
        s3_key = self.get_s3_key(lat, lon)
        temp_filepath = filepath.with_suffix('.tmp')
        compressed_filepath = filepath.with_suffix('.compressed.tif')
        
        try:
            # Download
            if self.use_boto:
                self.s3_client.download_file(
                    Bucket=self.BUCKET, Key=s3_key, Filename=str(temp_filepath),
                    Config=self.transfer_config
                )
            else:
                url = f"{self.BASE_URL}/{s3_key}"
                with session.get(url, timeout=120, stream=True) as response:
                    response.raise_for_status()
                    # OPTIMIZATION: Larger chunk size
                    with open(temp_filepath, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=65536):  # 64KB chunks
                            if chunk:
                                f.write(chunk)
            
            if not self.verify_tiff(temp_filepath):
                raise Exception("Downloaded TIFF file is corrupt")
            
            original_size_mb = temp_filepath.stat().st_size / (1024 * 1024)
            
            # Compression
            if self.compress:
                compression_ratio = self.compress_tiff(temp_filepath, compressed_filepath)
                final_filepath = compressed_filepath
            else:
                temp_filepath.rename(filepath)
                final_filepath = filepath
                compression_ratio = 0
            
            # Move final file
            if final_filepath != filepath:
                final_filepath.rename(filepath)
            
            final_size_mb = filepath.stat().st_size / (1024 * 1024)
            self.completed_tiles.add(tile_id)
            
            with self.stats_lock:
                self.stats['success'] += 1
                self.stats['verified'] += 1
                self.stats['total_size_mb'] += original_size_mb
                self.stats['compressed_size_mb'] += final_size_mb
                
            return 'success', original_size_mb, final_size_mb

        except Exception as e:
            for p in [temp_filepath, compressed_filepath]:
                if p.exists():
                    p.unlink()
            
            is_404_boto = isinstance(e, botocore.exceptions.ClientError) and e.response['Error']['Code'] == '404'
            is_404_http = isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 404

            if is_404_boto or is_404_http:
                self.completed_tiles.add(tile_id)
                return 'nodata', 0, 0
            
            logging.error(f"{filename}: {e}")
            with self.stats_lock:
                self.stats['failed'] += 1
            return 'error', 0, 0
            
    def download_continent(self, continent_key: str):
        if continent_key not in self.CONTINENTS:
            print(f"Invalid continent: {continent_key}")
            return

        continent = self.CONTINENTS[continent_key]
        print(f"\n{'='*70}\nCONTINENT: {continent['name'].upper()}")
        print(f"Data: Copernicus DEM 30m (AWS S3)")
        if self.compress:
            print(f"Compression: {self.compression_type} (lossless)")
        print('='*70)
        
        tiles = self.generate_tiles(continent['bounds'])
        tiles_to_download = [t for t in tiles if t[0] not in self.completed_tiles]
        total_tiles_to_process = len(tiles_to_download)

        print(f"Total {len(tiles)} tiles, {len(self.completed_tiles)} previously downloaded")
        print(f"Downloading {total_tiles_to_process} tiles in this session")
        print(f"Method: {'AWS S3 (Ultra Fast)' if self.use_boto else 'HTTP (Optimized)'}")
        
        self.stats = {
            'success': 0, 'failed': 0, 'skipped': len(self.completed_tiles), 
            'verified': 0, 'total_size_mb': 0, 'compressed_size_mb': 0
        }
        
        if total_tiles_to_process == 0:
            print("\nAll tiles already downloaded!")
            return

        pbar = tqdm(
            total=total_tiles_to_process, desc="Copernicus 30m", 
            unit="tile", ncols=100, colour='cyan'
        ) if TQDM_AVAILABLE else None
        
        start_time = time.time()
        
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {}
                for i, (tile_id, lat, lon) in enumerate(tiles_to_download):
                    if self.shutdown_event.is_set():
                        print("New task submission stopped...")
                        break

                    session = self.sessions[i % len(self.sessions)] if not self.use_boto else None
                    future = executor.submit(self.download_tile, tile_id, lat, lon, continent_key, session)
                    futures[future] = tile_id
                
                for future in as_completed(futures):
                    status, orig_size, comp_size = future.result()
                    if pbar:
                        compression_ratio = ((1 - self.stats['compressed_size_mb'] / self.stats['total_size_mb']) * 100) if self.stats['total_size_mb'] > 0 else 0
                        pbar.update(1)
                        pbar.set_postfix({
                            'ok': self.stats['success'],
                            'skip': self.stats['skipped'],
                            'fail': self.stats['failed'],
                            'GB': f"{self.stats['compressed_size_mb']/1024:.1f}",
                            'comp': f"{compression_ratio:.0f}%"
                        })
                    
                    if (self.stats['success'] + self.stats['failed']) % 25 == 0:
                        self.save_progress()
        finally:
            if self.shutdown_event.is_set():
                print("\nPausing... Saving progress.")
            
            if pbar:
                pbar.close()
            self.save_progress()
            
            elapsed = time.time() - start_time
            speed = (self.stats['success'] + self.stats['failed']) / elapsed if elapsed > 0 else 0
            compression_ratio = ((1 - self.stats['compressed_size_mb'] / self.stats['total_size_mb']) * 100) if self.stats['total_size_mb'] > 0 else 0
            saved_space = self.stats['total_size_mb'] - self.stats['compressed_size_mb']
            
            print(f"\n{'─'*70}\nSESSION SUMMARY:")
            print(f"Success: {self.stats['success']} tiles")
            print(f"Failed: {self.stats['failed']} tiles")
            print(f"Skipped: {self.stats['skipped']} tiles")
            if self.compress:
                print(f"Original Size: {self.stats['total_size_mb']:.2f} MB ({self.stats['total_size_mb']/1024:.2f} GB)")
                print(f"Compressed: {self.stats['compressed_size_mb']:.2f} MB ({self.stats['compressed_size_mb']/1024:.2f} GB)")
                print(f"Space Saved: {saved_space:.2f} MB ({saved_space/1024:.2f} GB) - {compression_ratio:.1f}% compression")
            else:
                print(f"Downloaded Data: {self.stats['total_size_mb']:.2f} MB ({self.stats['total_size_mb']/1024:.2f} GB)")
            print(f"Speed: {speed:.2f} tiles/sec ({speed * 60:.0f} tiles/min)")
            print(f"Duration: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
            print(f"{'─'*70}\n")
            
            if self.shutdown_event.is_set():
                print("Program stopped safely. Run again to resume.")
            else:
                print("Download completed!")


def interactive_mode():
    """Interactive mode"""
    print("\n" + "="*70)
    print("COPERNICUS DEM 30M TURBO DOWNLOADER v3.0")
    print("="*70)
    print("\nNew: Ultra Fast Download + Lossless Compression")
    
    print("\nCONTINENT SELECTION:")
    continents = list(CopernicusAWSDownloader.CONTINENTS.keys())
    for i, key in enumerate(continents, 1):
        print(f"  {i}. {CopernicusAWSDownloader.CONTINENTS[key]['name']} ({key})")
    
    try:
        choice = int(input(f"\nContinent number (1-{len(continents)}): "))
        continent = continents[choice - 1]
    except (ValueError, IndexError):
        print("Invalid selection!")
        return
    
    use_boto = False
    if BOTO3_AVAILABLE:
        print("\nDOWNLOAD METHOD:")
        print("  1. AWS S3 (boto3) - Ultra fast, 20x parallel")
        print("  2. HTTP - Standard, optimized")
        try:
            method = int(input("\nMethod (1-2) [1]: ") or "1")
            use_boto = (method == 1)
        except:
            use_boto = True
    else:
        print("\nWarning: boto3 not found. Using HTTP method.")
        print("Tip: For faster download: pip install boto3")
    
    compress = False
    compression_type = 'LZW'
    if RASTERIO_AVAILABLE:
        print("\nCOMPRESSION:")
        print("  1. LZW (recommended) - Fast, good ratio")
        print("  2. DEFLATE - Better ratio, slightly slower")
        print("  3. ZSTD - Best ratio (requires zstandard)")
        print("  4. LZMA - Maximum compression")
        print("  5. NO Compression")
        try:
            comp_choice = int(input("\nCompression (1-5) [1]: ") or "1")
            comp_map = {1: 'LZW', 2: 'DEFLATE', 3: 'ZSTD', 4: 'LZMA', 5: None}
            compression_type = comp_map.get(comp_choice, 'LZW')
            compress = compression_type is not None
        except:
            compress = True
            compression_type = 'LZW'
    else:
        print("\nWarning: rasterio not found. Compression disabled.")
        print("Tip: For compression: pip install rasterio")
        
    print("\nSETTINGS:")
    try:
        output_dir = input("Output folder [copernicus_dem]: ").strip() or "copernicus_dem"
        max_workers = int(input("Parallel workers (recommended: 12-20) [16]: ") or "16")
    except ValueError:
        output_dir, max_workers = "copernicus_dem", 16
    
    print(f"\n{'='*70}\nSUMMARY:")
    print(f"  Continent: {CopernicusAWSDownloader.CONTINENTS[continent]['name']}")
    print(f"  Method: {'AWS S3 (Ultra Fast)' if use_boto else 'HTTP (Optimized)'}")
    if compress:
        print(f"  Compression: {compression_type} (lossless)")
    else:
        print(f"  Compression: NONE")
    print(f"  Output: {output_dir}")
    print(f"  Workers: {max_workers}")
    print(f"{'='*70}")
    
    if input("\nContinue? (y/n): ").lower() not in ['y', 'yes', '']:
        print("Cancelled")
        return
    
    downloader = CopernicusAWSDownloader(
        output_dir=output_dir, 
        max_workers=max_workers, 
        use_boto=use_boto,
        compress=compress,
        compression_type=compression_type
    )
    downloader.download_continent(continent)


if __name__ == '__main__':
    interactive_mode()