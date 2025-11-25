import pandas as pd
from datetime import datetime
import configparser
import json
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
            df_status = pd.DataFrame(columns=["file_name", "status", "last_update"])

        now = datetime.now().isoformat()
        if file_name in df_status["file_name"].values:
            df_status.loc[df_status["file_name"] == file_name, ["status", "last_update"]] = [status, now]
        else:
            df_status = pd.concat([
                df_status,
                pd.DataFrame([[file_name, status, now]], columns=df_status.columns)
            ], ignore_index=True)

        df_status.to_csv(status_file, index=False, encoding="utf-8-sig", storage_options=minio_opts)

    except Exception as e:
        print(f"❌ Lỗi cập nhật file_status.csv: {e}")


def main():
    step_name = "data_quality"
    logger = get_logger("quality_checker", log_dir="logs", step_name=step_name)

    # --- Đọc config ---
    config = configparser.ConfigParser()
    config.read("config.ini")

    MINIO_STORAGE_OPTIONS = {
        "key": config["MINIO"]["key"],
        "secret": config["MINIO"]["secret"],
        "client_kwargs": {"endpoint_url": config["MINIO"]["endpoint_url"]},
    }

    # --- Đường dẫn ---
    bucket = config["MINIO"]["bucket"]
    folder = config["PATHS"]["staging_folder"]
    today = datetime.now().strftime("%Y-%m-%d")

    staging_path = f"s3://{bucket}/{folder}/{today}/clean_data.csv"
    report_path = f"s3://{bucket}/{folder}/{today}/data_quality_report.csv"
    status_file = f"s3://{bucket}/file_status.csv"
    file_name = "clean_data.csv"

    # --- Kiểm tra clean_data.csv có P3 chưa ---
    logger.info("🔍 Đang kiểm tra trạng thái file trước khi chạy Data Quality...")

    try:
        fs = s3fs.S3FileSystem(**MINIO_STORAGE_OPTIONS["client_kwargs"],
                               key=MINIO_STORAGE_OPTIONS["key"],
                               secret=MINIO_STORAGE_OPTIONS["secret"])

        if not fs.exists(status_file):
            logger.info("🔸 Chưa có file_status.csv → Dừng Data Quality.")
            return

        df_status = pd.read_csv(status_file, storage_options=MINIO_STORAGE_OPTIONS)
        status = df_status.loc[df_status["file_name"] == file_name, "status"].values

        if len(status) == 0 or status[0] != "P3":
            logger.info(f"🔸 {file_name} chưa P3 từ bước Load → Dừng Data Quality.")
            return

    except Exception as e:
        logger.error(f"❌ Lỗi khi đọc file_status.csv: {e}")
        return

    logger.info("✅ File đã đạt P3 → Bắt đầu Data Quality")

    try:
        # === P1: bắt đầu check ===
        update_file_status(file_name, "P1", bucket, MINIO_STORAGE_OPTIONS)
        logger.info(f"{file_name} - status: P1")

        df = pd.read_csv(staging_path, storage_options=MINIO_STORAGE_OPTIONS)
        logger.info(f"📥 Đã đọc {len(df)} dòng staging")

        # === P2: đang xử lý check ===
        update_file_status(file_name, "P2", bucket, MINIO_STORAGE_OPTIONS)
        logger.info(f"{file_name} - status: P2")

        # --- Check chất lượng dữ liệu ---
        report = {}
        report["num_rows"] = len(df)
        report["num_cols"] = len(df.columns)

        report["null_counts"] = df.isnull().sum().to_dict()
        report["duplicate_rows"] = df.duplicated().sum()

        if "giá" in df.columns:
            report["giá_min"] = df["giá"].min()
            report["giá_max"] = df["giá"].max()
            report["giá_invalid"] = df[df["giá"] <= 0].shape[0]

        # --- Lưu báo cáo ---
        pd.DataFrame([report]).to_csv(report_path, index=False, storage_options=MINIO_STORAGE_OPTIONS)
        logger.info(f"📄 Đã lưu báo cáo vào: {report_path}")

        # === P3: hoàn tất check ===
        update_file_status(file_name, "P3", bucket, MINIO_STORAGE_OPTIONS)
        logger.info(f"{file_name} - status: P3")

    except Exception as e:
        logger.error(f"❌ Lỗi Data Quality: {e}")

        # === P4: lỗi ===
        update_file_status(file_name, "P4", bucket, MINIO_STORAGE_OPTIONS)
        logger.info(f"{file_name} - status: P4")

    # Upload log
    upload_log_to_minio(logger.log_file, step_name=step_name)


if __name__ == "__main__":
    main()
