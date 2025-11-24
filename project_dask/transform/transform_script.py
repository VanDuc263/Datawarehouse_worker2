 
import pandas as pd
from datetime import datetime
import configparser
import os
from meta_logger.meta_logger import get_logger, upload_log_to_minio

def main():
    step_name = "transform"
    logger = get_logger("transformer", log_dir="logs", step_name=step_name)

    # ========================
    # 📘 Đọc file config
    # ========================
    config_file = "config.ini"
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"{config_file} không tồn tại trên Worker!")

    config = configparser.ConfigParser()
    config.read(config_file)

    MINIO_STORAGE_OPTIONS = {
        "key": config["MINIO"]["key"],
        "secret": config["MINIO"]["secret"],
        "client_kwargs": {"endpoint_url": config["MINIO"]["endpoint_url"]}
    }
    bucket = config["MINIO"]["bucket"]

    raw_path = f"s3://{bucket}/raw_data.csv"
    clean_path = f"s3://{bucket}/clean_data.csv"
    dim_brand_path = f"s3://{bucket}/dim_brand.csv"
    dim_product_path = f"s3://{bucket}/dim_product.csv"
    fact_price_path = f"s3://{bucket}/fact_product_price.csv"

    logger.info(f"🔹 Đang đọc raw data: {raw_path}")

    # ========================
    # 📥 Đọc Raw
    # ========================
    try:
        df = pd.read_csv(raw_path, storage_options=MINIO_STORAGE_OPTIONS)
    except Exception as e:
        logger.error(f"❌ Lỗi đọc raw_data: {e}")
        raise e

    # ========================
    # 🧹 CLEAN DATA
    # ========================
    df = df.drop_duplicates()
    df = df.dropna(subset=["product_name", "price_raw"])

    def clean_price(p):
        p = str(p).replace("₫", "").replace(".", "").replace(",", "").strip()
        return int(p) if p.isdigit() else None

    df["price"] = df["price_raw"].apply(clean_price)
    df = df.dropna(subset=["price"])
    df["transform_time"] = datetime.now().isoformat()

    # Lưu clean
    df.to_csv(clean_path, index=False, encoding="utf-8-sig",
              storage_options=MINIO_STORAGE_OPTIONS)
    logger.info(f"✅ Clean data saved: {clean_path}")

    # ========================
    # 🏗️ BUILD DIMENSIONS
    # ========================
    # DIM BRAND
    df["brand"] = df["product_name"].apply(lambda x: x.split()[0])
    dim_brand = df[["brand"]].drop_duplicates().reset_index(drop=True)
    dim_brand["brand_id"] = dim_brand.index + 1
    dim_brand.to_csv(dim_brand_path, index=False, encoding="utf-8-sig",
                     storage_options=MINIO_STORAGE_OPTIONS)
    logger.info("✅ Tạo xong DimBrand")

    # DIM PRODUCT
    dim_product = df[["product_name", "brand"]].drop_duplicates().reset_index(drop=True)
    dim_product = dim_product.merge(dim_brand, on="brand", how="left")
    dim_product["product_id"] = dim_product.index + 1
    dim_product.to_csv(dim_product_path, index=False, encoding="utf-8-sig",
                       storage_options=MINIO_STORAGE_OPTIONS)
    logger.info("✅ Tạo xong DimProduct")

    # ========================
    # 📊 FACT PRODUCT PRICE
    # ========================
    fact = df.merge(dim_product, on="product_name", how="left")
    fact = fact[["product_id", "brand_id", "price", "transform_time"]]
    fact.to_csv(fact_price_path, index=False, encoding="utf-8-sig",
                storage_options=MINIO_STORAGE_OPTIONS)
    logger.info("📦 Đã tạo FactProductPrice")

    # ========================
    # ☁️ Upload log
    # ========================
    upload_log_to_minio(logger.log_file, step_name=step_name)

    return {
        "clean_path": clean_path,
        "dim_brand_path": dim_brand_path,
        "dim_product_path": dim_product_path,
        "fact_price_path": fact_price_path
    }

# Nếu chạy script trực tiếp (debug)
if __name__ == "__main__":
    main()


