from utils.mail_utils import send_error_mail

import traceback

def run_extract():
    try:
        from extract.extract import main
        
        raw_path = main()
        print(raw_path)
        return raw_path
    except Exception as e:
        tb = traceback.format_exc()
        print("❌ Lỗi run_extract:\n", tb)  # In ra console Worker
        send_error_mail(
            subject="[ETL ERROR] Extract Failed",
            message=tb,
            to_email="22130050@st.hcmuaf.edu.vn"
        )
        raise


def run_transform():
    try:
        from transform.transform_script import main
        return main()
    except Exception as e:
        send_error_mail(
            subject="[ETL ERROR] Transform Failed",
            message=str(e),
            to_email="22130050@st.hcmuaf.edu.vn"
        )
        raise e
def run_loaddata():
    try:
        from loaddata.loaddata_script import main
        return main()
    except Exception as e:
        send_error_mail(
            subject="[ETL ERROR] Loaddata Failed",
            message=str(e),
            to_email="22130050@st.hcmuaf.edu.vn"
        )
        raise e

def run_checkdata():
    try:
        from check.data_quality_check import main
        return main()
    except Exception as e:
        send_error_mail(
            subject="[ETL ERROR] Check Failed",
            message=str(e),
            to_email="22130050@st.hcmuaf.edu.vn"
        )
        raise e

# =========================
# 🔥 DW Load: dim_brand
# =========================
def run_dw_load_dim_brand():
    try:
        from dw_load.dw_load_dim_brand import main
        return main()
    except Exception:
        tb = traceback.format_exc()
        send_error_mail(
            subject="[ETL ERROR] DW Load Dim Brand Failed",
            message=tb,
            to_email="22130050@st.hcmuaf.edu.vn"
        )
        raise


# =========================
# 🔥 DW Load: dim_product
# =========================
def run_dw_load_dim_product():
    try:
        from dw_load.dw_load_dim_product import main
        return main()
    except Exception:
        tb = traceback.format_exc()
        send_error_mail(
            subject="[ETL ERROR] DW Load Dim Product Failed",
            message=tb,
            to_email="22130050@st.hcmuaf.edu.vn"
        )
        raise


# =========================
# 🔥 DW Load: fact_product_price
# =========================
def run_dw_load_fact_product_price():
    try:
        from dw_load.dw_load_fact_product_price import main
        return main()
    except Exception:
        tb = traceback.format_exc()
        send_error_mail(
            subject="[ETL ERROR] DW Load Fact Product Price Failed",
            message=tb,
            to_email="22130050@st.hcmuaf.edu.vn"
        )
        raise
