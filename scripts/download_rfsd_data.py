import os
import requests
import zipfile
import io

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data", "rfsd")

URLS = [
    "https://storage.yandexcloud.net/tochno-st-catalog/FNS/data_rfsd_152_v20251008/by_years/data_150_rfsd_2020_v20251010_csv.zip",
    "https://storage.yandexcloud.net/tochno-st-catalog/FNS/data_rfsd_152_v20251008/by_years/data_150_rfsd_2021_v20251010_csv.zip",
    "https://storage.yandexcloud.net/tochno-st-catalog/FNS/data_rfsd_152_v20251008/by_years/data_150_rfsd_2022_v20251010_csv.zip",
    "https://storage.yandexcloud.net/tochno-st-catalog/FNS/data_rfsd_152_v20251008/by_years/data_150_rfsd_2023_v20251010_csv.zip",
    "https://storage.yandexcloud.net/tochno-st-catalog/FNS/data_rfsd_152_v20251008/by_years/data_150_rfsd_2024_v20251010_csv.zip"
]

YEARS = [2020, 2021, 2022, 2023, 2024]


def check_existing_files():
    os.makedirs(DATA_DIR, exist_ok=True)
    existing = []
    missing = []
    for year in YEARS:
        filepath = os.path.join(DATA_DIR, f"{year}.csv")
        if os.path.exists(filepath):
            existing.append(year)
        else:
            missing.append(year)
    return existing, missing


def extract_year_from_url(url):
    for year in YEARS:
        if str(year) in url:
            return year
    return None


def download_and_extract(url, target_year):
    print(f"[DOWNLOAD] Year {target_year}: {url}")
    try:
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        print(f"[DOWNLOAD] Year {target_year}: Downloaded {len(response.content)} bytes")
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            csv_files = [name for name in zf.namelist() if name.endswith('.csv')]
            if not csv_files:
                print(f"[ERROR] Year {target_year}: No CSV files found in archive")
                return False
            
            target_csv = csv_files[0]
            print(f"[EXTRACT] Year {target_year}: Extracting {target_csv}")
            
            with zf.open(target_csv) as src:
                content = src.read()
            
            output_path = os.path.join(DATA_DIR, f"{target_year}.csv")
            with open(output_path, 'wb') as dst:
                dst.write(content)
            
            print(f"[SAVE] Year {target_year}: Saved to {output_path}")
            return True
            
    except requests.RequestException as e:
        print(f"[ERROR] Year {target_year}: Download failed - {e}")
        return False
    except zipfile.BadZipFile as e:
        print(f"[ERROR] Year {target_year}: Invalid ZIP file - {e}")
        return False
    except Exception as e:
        print(f"[ERROR] Year {target_year}: {e}")
        return False


def main():
    print("=" * 60)
    print("RFSD DATA DOWNLOAD MODULE")
    print("=" * 60)
    
    existing, missing = check_existing_files()
    
    if existing:
        print(f"\n[CHECK] Found existing files: {existing}")
    
    if not missing:
        print("\n[CHECK] All required files exist. Nothing to download.")
        print("=" * 60)
        return
    
    print(f"\n[CHECK] Missing files for years: {missing}")
    print()
    
    success_count = 0
    for url in URLS:
        year = extract_year_from_url(url)
        if year and year in missing:
            if download_and_extract(url, year):
                success_count += 1
            print()
    
    print("=" * 60)
    print(f"Download complete: {success_count}/{len(missing)} files downloaded")
    print("=" * 60)
    
    existing_final, missing_final = check_existing_files()
    if missing_final:
        print(f"[WARNING] Still missing: {missing_final}")


if __name__ == "__main__":
    main()
