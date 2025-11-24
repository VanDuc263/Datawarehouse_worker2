import pandas as pd
from datetime import datetime
import configparser
import json
import sys
from meta_logger.meta_logger import get_logger, upload_log_to_minio


def main():
    # ========================
    # 🔧 Cấu hình logger
    # ========================
    step_name = "data_quality"
    logger = get_logger("quality_checker", log_dir="logs", step_name=step_name)

    # ========================
    # 📘 Đọc file cấu hình
    # ========================
    config = configparser.ConfigParser()
    config.read("config.ini")

    # ========================
    # ☁️ Cấu hình MinIO
    # ========================
    MINIO_STORAGE_OPTIONS = {
        "key": config["MINIO"]["key"],
        "secret": config["MINIO"]["secret"],
        "client_kwargs": {"endpoint_url": config["MINIO"]["endpoint_url"]},
    }

    # ========================
    # 🗂️ Đường dẫn dữ liệu
    # ========================
    bucket = config["PATHS"]["staging_bucket"]
    folder = config["PATHS"]["staging_folder"]
    today = datetime.now().strftime("%Y-%m-%d")

    staging_path = f"s3://{bucket}/{folder}/{today}/clean_data.csv"
    report_path = f"s3://{bucket}/{folder}/{today}/data_quality_report.csv"

    logger.info(f"🔹 Đang đọc dữ liệu từ: {staging_path}")

    # ========================
    # 📥 Đọc dữ liệu từ MinIO
    # ========================
    try:
        df = pd.read_csv(staging_path, storage_options=MINIO_STORAGE_OPTIONS)
        logger.info(f"✅ Số dòng dữ liệu staging: {len(df)}")
    except Exception as e:
        logger.error(f"❌ Lỗi khi đọc dữ liệu từ MinIO: {e}")
        sys.exit(1)

    # ========================
    # 🧠 Kiểm tra chất lượng dữ liệu
    # ========================
    report = {}

    # Tổng số dòng, số cột
    report["num_rows"] = len(df)
    report["num_cols"] = len(df.columns)
    logger.info(f"📊 Tổng số dòng: {report['num_rows']}, số cột: {report['num_cols']}")

    # Null count
    report["null_counts"] = df.isnull().sum().to_dict()
    null_summary = {k: v for k, v in report["null_counts"].items() if v > 0}
    if null_summary:
        logger.warning(f"⚠️ Có giá trị null ở các cột: {json.dumps(null_summary, ensure_ascii=False)}")
    else:
        logger.info("✅ Không có giá trị null nào trong dữ liệu.")

    # Duplicate rows
    report["duplicate_rows"] = df.duplicated().sum()
    if report["duplicate_rows"] > 0:
        logger.warning(f"⚠️ Có {report['duplicate_rows']} dòng trùng lặp.")
    else:
        logger.info("✅ Không có dòng trùng lặp.")

    # Kiểm tra giá hợp lệ (nếu có cột 'giá')
    if "giá" in df.columns:
        report["giá_min"] = df["giá"].min()
        report["giá_max"] = df["giá"].max()
        report["giá_invalid"] = df[df["giá"] <= 0].shape[0]
        logger.info(f"💰 Giá nhỏ nhất: {report['giá_min']}, lớn nhất: {report['giá_max']}")

        if report["giá_invalid"] > 0:
            logger.warning(f"⚠️ Có {report['giá_invalid']} dòng có giá không hợp lệ (≤ 0).")
        else:
            logger.info("✅ Tất cả giá trị 'giá' đều hợp lệ.")

    # ========================
    # 💾 Lưu báo cáo vào MinIO
    # ========================
    try:
        pd.DataFrame([report]).to_csv(report_path, index=False, storage_options=MINIO_STORAGE_OPTIONS)
        logger.info(f"🎯 Đã lưu báo cáo Data Quality vào: {report_path}")
    except Exception as e:
        logger.error(f"❌ Lỗi khi lưu báo cáo Data Quality vào MinIO: {e}")
        sys.exit(1)

    # ========================
    # ☁️ Upload log lên MinIO
    # ========================
    upload_log_to_minio(logger.log_file, step_name=step_name)


if __name__ == "__main__":
    main()

