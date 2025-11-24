import pandas as pd
import sqlalchemy
from sqlalchemy import text
from datetime import datetime
import configparser
from meta_logger.meta_logger import get_logger, upload_log_to_minio
import sys
import os
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
    step_name = "dw_load_dim_product"
    logger = get_logger("dw_loader", log_dir="logs", step_name=step_name)

    # ======================== Config
    config_file = "config.ini"
    if not os.path.exists(config_file):
        logger.error(f"{config_file} không tồn tại!")
        sys.exit(1)

    config = configparser.ConfigParser()
    config.read(config_file)

    MINIO_STORAGE_OPTIONS = {
        "key": config["MINIO"]["key"],
        "secret": config["MINIO"]["secret"],
        "client_kwargs": {"endpoint_url": config["MINIO"]["endpoint_url"]}
    }
    bucket = config["MINIO"]["bucket"]

    clean_file = "clean_data.csv"
    dim_file = "dim_product.csv"
    clean_path = f"s3://{bucket}/{clean_file}"
    dim_path = f"s3://{bucket}/{dim_file}"
    status_file = f"s3://{bucket}/file_status.csv"

    MYSQL_CONFIG = {
        "user": config["MYSQL"]["user"],
        "password": config["MYSQL"]["password"],
        "host": config["MYSQL"]["host"],
        "port": int(config["MYSQL"]["port"]),
        "database": config["MYSQL"]["database"]
    }

    # ======================== Kiểm tra clean_data đã P3
    try:
        fs = s3fs.S3FileSystem(**MINIO_STORAGE_OPTIONS["client_kwargs"],
                               key=MINIO_STORAGE_OPTIONS["key"],
                               secret=MINIO_STORAGE_OPTIONS["secret"])
        if fs.exists(status_file):
            df_status = pd.read_csv(status_file, storage_options=MINIO_STORAGE_OPTIONS)
            clean_status = df_status.loc[df_status["file_name"]==clean_file,"status"].values
            if len(clean_status)==0 or clean_status[0] != "P3":
                logger.info(f"🔹 {clean_file} chưa P3 → dw_load_dim_product dừng.")
                return
        else:
            logger.info("🔹 Chưa có file_status.csv → dw_load_dim_product dừng.")
            return
    except Exception as e:
        logger.error(f"❌ Lỗi đọc file_status.csv: {e}")
        return

    logger.info(f"🔹 {clean_file} đã P3 → Bắt đầu dw_load_dim_product")

    # ======================== P1: bắt đầu
    logger.info(f"{dim_file} - status: P1")
    update_file_status(dim_file, "P1", bucket, MINIO_STORAGE_OPTIONS)

    try:
        # ======================== Đọc clean_data
        df_clean = pd.read_csv(clean_path, storage_options=MINIO_STORAGE_OPTIONS)
        logger.info(f"✅ Đã đọc {len(df_clean)} dòng, {len(df_clean.columns)} cột dữ liệu")

        # ======================== P2: đang xử lý
        logger.info(f"{dim_file} - status: P2")
        update_file_status(dim_file, "P2", bucket, MINIO_STORAGE_OPTIONS)

        # ======================== Tạo dim_product
        df_clean["brand"] = df_clean["product_name"].apply(lambda x: x.split()[0])
        dim_brand = df_clean[["brand"]].drop_duplicates().reset_index(drop=True)
        dim_brand["brand_id"] = dim_brand.index + 1

        dim_product = df_clean[["product_name","brand"]].drop_duplicates().reset_index(drop=True)
        dim_product = dim_product.merge(dim_brand, on="brand", how="left")
        dim_product["product_id"] = dim_product.index + 1

        # Lưu tạm lên MinIO
        dim_product.to_csv(dim_path, index=False, encoding="utf-8-sig", storage_options=MINIO_STORAGE_OPTIONS)
        logger.info(f"✅ Tạo xong dim_product: {dim_path}")

        # ======================== Load vào MySQL
        engine_str = (
            f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}"
            f"@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
        )
        engine = sqlalchemy.create_engine(engine_str)
        table_name = "dim_product"
        dim_product.to_sql(table_name, con=engine, if_exists="replace", index=False)
        logger.info(f"🎯 Load dim_product thành công vào bảng `{table_name}`")

        # ======================== Kiểm tra số dòng
        with engine.connect() as conn:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
        logger.info(f"📊 Số dòng trong bảng {table_name}: {count}")

        # ======================== P3: hoàn tất
        logger.info(f"{dim_file} - status: P3")
        update_file_status(dim_file, "P3", bucket, MINIO_STORAGE_OPTIONS)

    except Exception as e:
        logger.error(f"❌ Lỗi dw_load_dim_product: {e}")
        logger.info(f"{dim_file} - status: P4")
        update_file_status(dim_file, "P4", bucket, MINIO_STORAGE_OPTIONS)

    # ======================== Upload log
    upload_log_to_minio(logger.log_file, step_name=step_name)


if __name__ == "__main__":
    main()

