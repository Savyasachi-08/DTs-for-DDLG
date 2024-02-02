import pandas as pd
import numpy as np
import re
import json
import sys
import plotly.express as px
import cx_Oracle
import os


def get_cursor():
    connection = cx_Oracle.connect(
        os.getenv("VMART_VULCAN_GIN_DB_USER"),
        os.getenv("VMART_VULCAN_GIN_DB_PASS"),
        os.getenv("VMART_VULCAN_GIN_DB_HOST"),
    )
    cursor = connection.cursor()
    return connection, cursor


def send_message(message_obj):
    json_string = json.dumps(message_obj)
    print(json_string)


def fetch_sbi_data():
    try:
        csv_file_path_sbi = (
            "/home/savyasachi/Downloads/Automation_Store_Sale_Reconciliation/SBI CC.csv"
        )
        excel_file_path = "/home/savyasachi/Downloads/Automation_Store_Sale_Reconciliation/MASTER FILE FOR COLLECTION_VOL-2.xlsb"

        if not csv_file_path_sbi.endswith(".csv") or not excel_file_path.endswith(
            (".xlsb", ".xls")
        ):
            send_message(
                {
                    "severity": "error",
                    "message": "Invalid file extension. Please provide .csv for SBI file and .xlsb or .xls for Excel file.",
                }
            )
            sys.exit(1)

        dt_csv_sbi = pd.read_csv(csv_file_path_sbi, index_col=False)
        if "TID" not in dt_csv_sbi.columns:
            send_message(
                {"severity": "error", "message": "Missing 'TID' column in SBI CC file."}
            )
            sys.exit(1)
        dt_csv_sbi["TID"] = dt_csv_sbi["TID"].str.replace("'", "")

        dt_csv_sbi["Tran Date"] = pd.to_datetime(dt_csv_sbi["Tran Date"])
        dt_csv_sbi["Tran Date"] = dt_csv_sbi["Tran Date"].dt.date

        dt_csv_sbi = dt_csv_sbi[
            dt_csv_sbi["Tran Date"] == pd.to_datetime("2023-12-28").date()
        ]

        dt_excel_sbi = pd.read_excel(
            excel_file_path, sheet_name=11, usecols=["TID", "LOCATION NAME"]
        )

        if (
            "TID" not in dt_excel_sbi.columns
            or "LOCATION NAME" not in dt_excel_sbi.columns
        ):
            missing_columns = []
            if "TID" not in dt_excel_sbi.columns:
                missing_columns.append("'TID'")
            if "LOCATION NAME" not in dt_excel_sbi.columns:
                missing_columns.append("'LOCATION NAME'")

            send_message(
                {
                    "severity": "error",
                    "message": f"Missing {', '.join(missing_columns)} column(s) in MASTER FILE FOR COLLECTION_VOL-2.",
                }
            )
            sys.exit(1)

        dt_excel_sbi = dt_excel_sbi.drop_duplicates(subset="TID")

        dt_csv_sbi["TID"] = dt_csv_sbi["TID"].astype(str)
        dt_excel_sbi["TID"] = dt_excel_sbi["TID"].astype(str)

        merged_data = pd.merge(dt_csv_sbi, dt_excel_sbi, on="TID", how="left")

        final_sbi = merged_data[["LOCATION NAME", "Net Amount"]]
        final_sbi = final_sbi.rename(
            columns={"LOCATION NAME": "STORE", "Net Amount": "SBI_total_amt"}
        )

        final_sbi = final_sbi.groupby("STORE", as_index=False)["SBI_total_amt"].sum()

        final_sbi["SBI_total_amt"].fillna(0, inplace=True)

        total_sbi = final_sbi["SBI_total_amt"].sum()
        # print("Total SBI Net Amount:", total_sbi)

        return final_sbi

    except FileNotFoundError as e:
        send_message({"severity": "error", "message": f"File not found: {e}"})
    except ValueError as e:
        send_message({"severity": "error", "message": f"Value error occurred: {e}"})
    except Exception as e:
        send_message({"severity": "error", "message": f"An error occurred: {e}"})

    return None


def fetch_hdfc():
    try:
        excel_file_path = "/home/savyasachi/Downloads/Automation_Store_Sale_Reconciliation/MASTER FILE FOR COLLECTION_VOL-2.xlsb"
        excel_file_path_hdfc = "/home/savyasachi/Downloads/Automation_Store_Sale_Reconciliation/8386-29122023.xlsb"

        if not excel_file_path.endswith(".xlsb") or not excel_file_path_hdfc.endswith(
            ".xlsb"
        ):
            send_message(
                {
                    "severity": "error",
                    "message": "Invalid file extension. Please provide .xlsb for both HDFC and Master Excel files.",
                }
            )
            sys.exit(1)

        dt_excel_master = pd.read_excel(
            excel_file_path,
            sheet_name=4,
            usecols=["HDFC TID", "Store Locations"],
        )

        missing_master_headers = [
            col
            for col in ["HDFC TID", "Store Locations"]
            if col not in dt_excel_master.columns
        ]
        if missing_master_headers:
            send_message(
                {
                    "severity": "error",
                    "message": f"Missing header(s) {', '.join(missing_master_headers)} in MASTER FILE FOR COLLECTION_VOL-2.",
                }
            )
            sys.exit(1)

        dt_excel_hdfc = pd.read_table(excel_file_path_hdfc)

        if "TERMINAL NUMBER" not in dt_excel_hdfc.columns:
            send_message(
                {
                    "severity": "error",
                    "message": "Missing 'TERMINAL NUMBER' header in HDFC file.",
                }
            )
            sys.exit(1)

        dt_excel_hdfc["TERMINAL NUMBER"] = dt_excel_hdfc["TERMINAL NUMBER"].astype(str)
        dt_excel_master["HDFC TID"] = dt_excel_master["HDFC TID"].astype(str)

        merged_data = pd.merge(
            dt_excel_master,
            dt_excel_hdfc,
            how="left",
            left_on="HDFC TID",
            right_on="TERMINAL NUMBER",
        )

        merged_data = merged_data.drop(columns=["TERMINAL NUMBER"])
        merged_data = merged_data.rename(columns={"HDFC TID": "TID"})

        merged_data["INTNL AMT"] = pd.to_numeric(
            merged_data["INTNL AMT"], errors="coerce"
        )
        merged_data["DOMESTIC AMT"] = pd.to_numeric(
            merged_data["DOMESTIC AMT"], errors="coerce"
        )

        merged_data["INTNL AMT"].fillna(0, inplace=True)
        merged_data["DOMESTIC AMT"].fillna(0, inplace=True)

        merged_data["hdfc_total_amt"] = (
            merged_data["DOMESTIC AMT"] + merged_data["INTNL AMT"]
        )

        final_hdfc = merged_data.groupby("Store Locations", as_index=False)[
            "hdfc_total_amt"
        ].sum()
        final_hdfc = final_hdfc.rename(columns={"Store Locations": "STORE"})
        final_hdfc = final_hdfc[final_hdfc["STORE"] != "0x2a"]

        final_hdfc["hdfc_total_amt"].fillna(0, inplace=True)

        return final_hdfc

    except FileNotFoundError as e:
        send_message({"severity": "error", "message": f"File not found: {e}"})
    except ValueError as e:
        send_message({"severity": "error", "message": f"Value error occurred: {e}"})
    except Exception as e:
        send_message({"severity": "error", "message": f"An error occurred: {e}"})

    return None


def fetch_ginesys_advance():
    con, cursor = get_cursor()

    cursor.execute(
        """
        SELECT ADMSITE_CODE, MOPDESC, SUM(BASEAMT) AS TOTAL_BASEAMT, MAX(BILLDATE) AS MAX_BILLDATE
        FROM PSITE_POSBILLMOP@hitesh_tovmrl
        WHERE MOPDESC IN ('Credit Card', 'Paytm_EDC_1')
        AND BILLDATE BETWEEN TO_DATE('2023-12-28 00:00:00', 'yyyy/mm/dd HH24:MI:SS')
                        AND TO_DATE('2023-12-28 23:59:59', 'yyyy/mm/dd HH24:MI:SS')
        GROUP BY ADMSITE_CODE, MOPDESC
        """
    )
    render_data = cursor.fetchall()

    columns = ["ADMSITE_CODE", "MOPDESC", "TOTAL_BASEAMT", "MAX_BILLDATE"]
    df = pd.DataFrame(render_data, columns=columns)

    cursor.execute(
        """
        SELECT CODE, SHRTNAME
        FROM ADMSITE@hitesh_tovmrl
        """
    )
    site_data = cursor.fetchall()
    site_columns = ["CODE", "SHRTNAME"]
    df_site = pd.DataFrame(site_data, columns=site_columns)

    merged_df = pd.merge(df, df_site, left_on="ADMSITE_CODE", right_on="CODE")

    merged_df = merged_df.drop(columns=["ADMSITE_CODE", "CODE"])

    merged_df = merged_df.rename(columns={"SHRTNAME": "STORE"})

    total_ginesys_advance = merged_df.groupby("STORE", as_index=False).agg(
        {"TOTAL_BASEAMT": "sum", "MAX_BILLDATE": "max"}
    )

    total_ginesys_advance = total_ginesys_advance.rename(
        columns={
            "TOTAL_BASEAMT": "total_ginesys_advance",
            "MAX_BILLDATE": "max_BILLDATE",
        }
    )

    print(total_ginesys_advance)
    return total_ginesys_advance


def fetch_ginesys_new():
    try:
        new_mop_path = "/home/savyasachi/Downloads/Automation_Store_Sale_Reconciliation/NEW MOP (Finance)-28 DEC 23.csv"

        if not new_mop_path.endswith(".csv"):
            send_message(
                {
                    "severity": "error",
                    "message": "Invalid file extension. Please provide a .csv file for the input.",
                }
            )
            sys.exit(1)

        dt_new_mop = pd.read_csv(new_mop_path, header=1, encoding="ISO-8859-1")

        filtered_new_mop = dt_new_mop[
            (dt_new_mop["Ledger"] == "Credit Card Receivable")
            & (dt_new_mop["Entry type long"] == "POS Journal")
        ]

        total_new_ginesys = (
            filtered_new_mop.groupby("Source Short Name")["Balance SUM"]
            .sum()
            .reset_index()
        )
        total_new_ginesys = total_new_ginesys.rename(
            columns={"Source Short Name": "STORE", "Balance SUM": "total_ginesys_new"}
        )

        total_new_ginesys["total_ginesys_new"].fillna(0, inplace=True)

        return total_new_ginesys

    except FileNotFoundError as e:
        send_message({"severity": "error", "message": f"File not found: {e}"})
    except ValueError as e:
        send_message({"severity": "error", "message": f"Value error occurred: {e}"})
    except Exception as e:
        send_message({"severity": "error", "message": f"An error occurred: {e}"})

    return None


def bajaj_mpr():
    try:
        bajaj_excel = "/home/savyasachi/Downloads/Automation_Store_Sale_Reconciliation/Common_Ledger (26).xlsx"
        mpr_master = "/home/savyasachi/Downloads/Automation_Store_Sale_Reconciliation/MASTER FILE FOR COLLECTION_VOL-2.xlsb"

        if not (
            bajaj_excel.endswith((".xlsx", ".xlsb", ".xls"))
            and mpr_master.endswith((".xlsx", ".xlsb", ".xls"))
        ):
            send_message(
                {
                    "severity": "error",
                    "message": "Invalid file extension. Please provide .xlsx, .xlsb, or .xls files for the input.",
                }
            )
            sys.exit(1)

        df_mpr_master = pd.read_excel(
            mpr_master, sheet_name=14, usecols=["BFL\nDEALER CODE", "Store name"]
        )

        df_bajaj = pd.read_excel(bajaj_excel)
        df_bajaj["Invoice Date"] = df_bajaj["Invoice Date"].str.replace("'", "")

        df_bajaj["Invoice Date"] = pd.to_datetime(
            df_bajaj["Invoice Date"], format="%d/%m/%Y"
        )

        df_bajaj_filtered = df_bajaj[
            df_bajaj["Invoice Date"] == pd.to_datetime("2023-12-28")
        ]

        merged_data = pd.merge(
            df_mpr_master,
            df_bajaj_filtered,
            how="left",
            left_on="BFL\nDEALER CODE",
            right_on="Supplier ID",
        )

        filtered_merged_data = merged_data[
            merged_data["Supplier ID"] == merged_data["BFL\nDEALER CODE"]
        ]

        bajaj_total = (
            filtered_merged_data.groupby("Store name")["Invoice Amt"]
            .sum()
            .reset_index()
        )
        bajaj_total = bajaj_total.rename(
            columns={
                "Store name": "STORE",
                "Invoice Amt": "BAJAJ_total_amt",
            }
        )

        bajaj_total["BAJAJ_total_amt"].fillna(0, inplace=True)

        return bajaj_total

    except FileNotFoundError as e:
        send_message({"severity": "error", "message": f"File not found: {e}"})
    except ValueError as e:
        send_message({"severity": "error", "message": f"Value error occurred: {e}"})
    except Exception as e:
        send_message({"severity": "error", "message": f"An error occurred: {e}"})

    return None


def paytm_mpr():
    try:
        paytm_mpr = "/home/savyasachi/Downloads/Automation_Store_Sale_Reconciliation/Paytm_EDC.csv"
        mpr_master = "/home/savyasachi/Downloads/Automation_Store_Sale_Reconciliation/MASTER FILE FOR COLLECTION_VOL-2.xlsb"

        if not (
            paytm_mpr.endswith(".csv")
            or mpr_master.endswith((".xlsx", ".xlsb", ".xls"))
        ):
            send_message(
                {
                    "severity": "error",
                    "message": "Invalid file extension. Please provide .csv for Paytm file and .xlsx, .xlsb, or .xls for Master file.",
                }
            )
            sys.exit(1)

        df_mpr_master = pd.read_excel(
            mpr_master, sheet_name=3, header=1, usecols=["Production Mid", "LOCATION"]
        ).drop_duplicates(subset=["Production Mid"])

        missing_master_headers = [
            col
            for col in ["Production Mid", "LOCATION"]
            if col not in df_mpr_master.columns
        ]
        if missing_master_headers:
            send_message(
                {
                    "severity": "error",
                    "message": f"Missing header(s) {', '.join(missing_master_headers)} in MASTER FILE FOR COLLECTION_VOL-2.",
                }
            )
            sys.exit(1)

        df_paytm = pd.read_csv(paytm_mpr, encoding="ISO-8859-1", dtype=str)

        if "original_mid" not in df_paytm.columns:
            send_message(
                {
                    "severity": "error",
                    "message": "Missing 'original_mid' header in Paytm_EDC.csv file.",
                }
            )
            sys.exit(1)

        df_paytm["original_mid"] = df_paytm["original_mid"].str.replace("'", "")
        df_paytm["transaction_date"] = df_paytm["transaction_date"].str.replace("'", "")

        df_paytm["amount"] = df_paytm["amount"].str.replace("'", "")
        df_paytm["amount"] = df_paytm["amount"].apply(
            lambda x: sum(map(float, re.findall(r"\d+\.\d+", str(x))))
        )

        df_paytm["transaction_date"] = pd.to_datetime(
            df_paytm["transaction_date"], format="%d-%m-%Y %H:%M:%S"
        ).dt.date

        df_paytm_filtered = df_paytm[
            df_paytm["transaction_date"] == pd.to_datetime("2023-12-28").date()
        ]

        prev_total_paytm = df_paytm_filtered["amount"].sum()

        merged_data = pd.merge(
            df_mpr_master[
                df_mpr_master["Production Mid"].isin(df_paytm_filtered["original_mid"])
            ],
            df_paytm_filtered,
            how="left",
            left_on="Production Mid",
            right_on="original_mid",
        )
        again_prev_total_paytm = merged_data["amount"].sum()

        paytm_total_amt = merged_data.groupby("LOCATION", as_index=False)[
            "amount"
        ].sum()
        paytm_total_amt = paytm_total_amt.rename(
            columns={"LOCATION": "STORE", "amount": "Paytm_total_amt"}
        )

        paytm_total_amt["Paytm_total_amt"].fillna(0, inplace=True)

        total_paytm = paytm_total_amt["Paytm_total_amt"].sum()

        return paytm_total_amt

    except FileNotFoundError as e:
        send_message({"severity": "error", "message": f"File not found: {e}"})
    except ValueError as e:
        send_message({"severity": "error", "message": f"Value error occurred: {e}"})
    except Exception as e:
        send_message({"severity": "error", "message": f"An error occurred: {e}"})

    return None


def generate_csv():
    sbi_data = fetch_sbi_data()
    hdfc_data = fetch_hdfc()
    ginesys_advance_data = fetch_ginesys_advance()
    ginesys_new_data = fetch_ginesys_new()
    bajaj_data = bajaj_mpr()
    paytm_data = paytm_mpr()

    merged_data = pd.merge(sbi_data, hdfc_data, on="STORE", how="outer")
    merged_data = pd.merge(merged_data, ginesys_advance_data, on="STORE", how="outer")
    merged_data = pd.merge(merged_data, ginesys_new_data, on="STORE", how="outer")
    merged_data = pd.merge(merged_data, bajaj_data, on="STORE", how="outer")
    merged_data = pd.merge(merged_data, paytm_data, on="STORE", how="outer")

    merged_data["total_CC_recd"] = merged_data[
        ["SBI_total_amt", "hdfc_total_amt", "BAJAJ_total_amt", "Paytm_total_amt"]
    ].sum(axis=1)

    merged_data["Difference"] = (
        merged_data["total_ginesys_advance"] + merged_data["total_ginesys_new"]
    ) - merged_data["total_CC_recd"]

    merged_data.fillna(0, inplace=True)

    merged_data = merged_data[
        [
            "STORE",
            "total_ginesys_advance",
            "total_ginesys_new",
            "SBI_total_amt",
            "hdfc_total_amt",
            "BAJAJ_total_amt",
            "Paytm_total_amt",
            "total_CC_recd",
            "Difference",
        ]
    ]

    differences = merged_data[merged_data["Difference"] != 0]["Difference"].tolist()

    if differences:
        filtered_data = merged_data[merged_data["Difference"] != 0]

        custom_template = {
            "layout": {
                "paper_bgcolor": "rgb(255, 255, 224)",
                "plot_bgcolor": "rgb(255, 255, 224)",
            }
        }

        fig = px.bar(
            filtered_data,
            x="STORE",
            y="Difference",
            title="Difference in Sale Reconciliation",
            text="Difference",
        )

        fig.update_traces(
            marker_color=[
                f"rgba( 255, 108, 108, 0.6)"
                if diff < 0
                else f"rgba( 150, 232, 109  0.6)"
                for diff in filtered_data["Difference"]
            ]
        )
        fig.update_layout(xaxis_title="Store", yaxis_title="Difference")

        fig.update_layout(bargap=0.7)

        fig.update_layout(template=custom_template)

        fig.show()

        fig.write_html(
            "/home/savyasachi/Downloads/Graphs_Sale_Reconciliation/difference_bar_plot.html"
        )
        try:
            merged_data.to_csv(
                "/home/savyasachi/Downloads/Automation_Store_Sale_Reconciliation/merged_data_final.csv",
                index=False,
            )
            send_message(
                {
                    "severity": "success",
                    "message": "CSV file has been generated successfully.",
                }
            )
        except Exception as e:
            send_message(
                {"severity": "error", "message": f"Error generating CSV: {str(e)}"}
            )

    else:
        print("No differences found in the 'Difference' column.")

    merged_data.to_csv(
        "/home/savyasachi/Downloads/Automation_Store_Sale_Reconciliation/merged_data_final.csv",
        index=False,
    )


generate_csv()


# def fetch_ginesys_advance():
#     try:
#         advance_mop_path = "/home/savyasachi/Downloads/Automation_Store_Sale_Reconciliation/Advance_MOP_ZBM (64).xlsx"

#         if not advance_mop_path.endswith((".xlsx", ".xlsb", ".xls")):
#             send_message(
#                 {
#                     "severity": "error",
#                     "message": "Invalid file extension. Please provide .xlsx, .xlsb, or .xls for the input file.",
#                 }
#             )
#             sys.exit(1)

#         dt_advance_mop = pd.read_excel(advance_mop_path, header=3)

#         filtered_advance_mop = dt_advance_mop[
#             (dt_advance_mop["MOP"] == "Credit Card")
#             | (dt_advance_mop["MOP"] == "Paytm_EDC_1")
#         ]

#         total_ginesys_advance = (
#             filtered_advance_mop.groupby("Site Name")["COLLECTION"].sum().reset_index()
#         )
#         total_ginesys_advance = total_ginesys_advance.rename(
#             columns={"Site Name": "STORE", "COLLECTION": "total_ginesys_advance"}
#         )

#         total_ginesys_advance["total_ginesys_advance"].fillna(0, inplace=True)

#         return total_ginesys_advance

#     except FileNotFoundError as e:
#         send_message({"severity": "error", "message": f"File not found: {e}"})
#     except ValueError as e:
#         send_message({"severity": "error", "message": f"Value error occurred: {e}"})
#     except Exception as e:
#         send_message({"severity": "error", "message": f"An error occurred: {e}"})

#     return None
