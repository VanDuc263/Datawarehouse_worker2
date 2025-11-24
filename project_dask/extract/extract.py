import pandas as pd
from datetime import datetime
import configparser
import sys
from meta_logger.meta_logger import get_logger, upload_log_to_minio

def main():
    # Logger
    step_name = "load"
    logger = get_logger("staging_loader", log_dir="logs", step_name=step_name)

    # Config
    config = configparser.ConfigParser()
    config.read("config.ini")
    MINIO_STORAGE_OPTIONS = {
        "key": config["MINIO"]["key"],
        "secret": config["MINIO"]["secret"],
        "client_kwargs": {"endpoint_url": config["MINIO"]["endpoint_url"]}
    }
    bucket = config["MINIO"]["bucket"]
    staging_folder = config["PATHS"]["staging_folder"]

    # Đường dẫn
    input_path = f"s3://{bucket}/clean_data.csv"
    today = datetime.now().strftime("%Y-%m-%d")
    staging_path = f"s3://{bucket}/{staging_folder}/{today}/clean_data.csv"

    logger.info(f"🔹 Đang đọc dữ liệu clean từ: {input_path}")

    # Đọc clean data
    try:
        df = pd.read_csv(input_path, storage_options=MINIO_STORAGE_OPTIONS)
        logger.info(f"✅ Đã đọc {len(df)} dòng, {len(df.columns)} cột dữ liệu")
    except Exception as e:
        logger.error(f"❌ Lỗi khi đọc dữ liệu: {e}")
        sys.exit(1)

    # Lưu staging version hóa theo ngày
    try:
        df.to_csv(staging_path, index=False, encoding="utf-8-sig", storage_options=MINIO_STORAGE_OPTIONS)
        logger.info(f"🎯 Đã lưu staging thành công vào: {staging_path}")
    except Exception as e:
        logger.error(f"❌ Lỗi khi lưu staging: {e}")
        sys.exit(1)

    # Upload log
    upload_log_to_minio(logger.log_file, step_name=step_name)


if __name__ == "__main__":
    main()

