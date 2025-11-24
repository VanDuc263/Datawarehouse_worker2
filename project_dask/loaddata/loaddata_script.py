import pandas as pd
from datetime import datetime
import configparser
import sys
from meta_logger.meta_logger import get_logger, upload_log_to_minio
import s3fs

def update_file_status(file_name, status, bucket, minio_opts):
    """Cập nhật trạng thái P1-P4 cho file trên MinIO"""
    status_file = f"s3://{bucket}/file_status.csv"
    try:
        fs = s3fs.S3FileSystem(**minio_opts["client_kwargs"],
                               key=minio_opts["key"],
                               secret=minio_opts["secret"])
        if fs.exists(status_file):
            df_status = pd.read_csv(status_file, storage_options=minio_opts)
        else:
            df_status = pd.DataFrame(columns=["file_name","status","last_update"])

        now = datetime.now().isoformat()
        if file_name in df_status["file_name"].values:
            df_status.loc[df_status["file_name"]==file_name, ["status","last_update"]] = [status, now]
        else:
            df_status = pd.concat([df_status, pd.DataFrame([[file_name,status,now]], columns=df_status.columns)],
                                  ignore_index=True)

        df_status.to_csv(status_file, index=False, encoding="utf-8-sig", storage_options=minio_opts)
    except Exception as e:
        print(f"❌ Lỗi cập nhật file_status.csv: {e}")


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
    status_file = f"s3://{bucket}/file_status.csv"
    file_name = "clean_data.csv"

    # Kiểm tra clean_data đã P3 chưa
    try:
        fs = s3fs.S3FileSystem(**MINIO_STORAGE_OPTIONS["client_kwargs"],
                               key=MINIO_STORAGE_OPTIONS["key"],
                               secret=MINIO_STORAGE_OPTIONS["secret"])
        if fs.exists(status_file):
            df_status = pd.read_csv(status_file, storage_options=MINIO_STORAGE_OPTIONS)
            status = df_status.loc[df_status["file_name"]==file_name, "status"].values
            if len(status)==0 or status[0] != "P3":
                logger.info(f"🔹 {file_name} chưa P3 → Load dừng.")
                return
        else:
            logger.info("🔹 Chưa có file_status.csv → Load dừng.")
            return
    except Exception as e:
        logger.error(f"❌ Lỗi đọc file_status.csv: {e}")
        return

    logger.info(f"🔹 {file_name} đã P3 → Bắt đầu Load")

    try:
        # P1: bắt đầu Load
        logger.info(f"{file_name} - status: P1")
        update_file_status(file_name, "P1", bucket, MINIO_STORAGE_OPTIONS)

        df = pd.read_csv(input_path, storage_options=MINIO_STORAGE_OPTIONS)
        logger.info(f"✅ Đã đọc {len(df)} dòng, {len(df.columns)} cột dữ liệu")

        # P2: đang xử lý lưu staging
        logger.info(f"{file_name} - status: P2")
        update_file_status(file_name, "P2", bucket, MINIO_STORAGE_OPTIONS)

        df.to_csv(staging_path, index=False, encoding="utf-8-sig", storage_options=MINIO_STORAGE_OPTIONS)
        logger.info(f"🎯 Đã lưu staging thành công vào: {staging_path}")

        # P3: hoàn tất Load
        logger.info(f"{file_name} - status: P3")
        update_file_status(file_name, "P3", bucket, MINIO_STORAGE_OPTIONS)

    except Exception as e:
        logger.error(f"❌ Lỗi Load: {e}")
        logger.info(f"{file_name} - status: P4")
        update_file_status(file_name, "P4", bucket, MINIO_STORAGE_OPTIONS)

    # Upload log
    upload_log_to_minio(logger.log_file, step_name=step_name)


if __name__ == "__main__":
    main()

