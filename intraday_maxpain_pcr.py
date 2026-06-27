import requests
import pandas as pd

# =====================================================
# AUTH
# =====================================================
access_token_file = r"C:\Users\VEKSHA\Documents\Trading\Python codes\UPSTOX_FINAL\access_token.txt"

with open(access_token_file, "r") as file:
    access_token = file.read().strip()

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {access_token}"
}

# =====================================================
# PARAMETERS
# =====================================================

instrument = 'NSE_INDEX|Nifty Bank'
expiry_date = '2026-06-30'
query_date = '2026-06-25'
interval = 3

params = {
    'instrument_key': instrument,
    'expiry': expiry_date,
    'date': query_date,
    'bucket_interval': interval
}

# =====================================================
# FETCH MAX PAIN
# =====================================================
mp_url = "https://api.upstox.com/v2/market/max-pain"
mp_res = requests.get(mp_url, params=params, headers=headers).json()

# =====================================================
# FETCH PCR
# =====================================================
pcr_url = "https://api.upstox.com/v2/market/pcr"
pcr_res = requests.get(pcr_url, params=params, headers=headers).json()

# =====================================================
# FETCH INTRADAY CANDLES
# =====================================================
quote = requests.utils.quote("NSE_FO|62329")

# candle_url = (
#     f"https://api.upstox.com/v3/historical-candle/intraday/"
#     f"{quote}/minutes/{interval}"
# )

candle_url = f'https://api.upstox.com/v3/historical-candle/{quote}/minutes/{interval}/2026-06-25/2026-06-24'

candle_res = requests.get(candle_url, headers=headers).json()

# =====================================================
# BUILD MP + PCR DATAFRAME
# =====================================================
df_mp = pd.DataFrame(mp_res["data"]["insights"]) 
df_pcr = pd.DataFrame(pcr_res["data"]["insights"]) 

df_pcr.drop(columns=["spot_price"], inplace=True, errors="ignore")

merged_df = pd.merge(
    df_mp,
    df_pcr,
    on="time",
    how="outer"
)

merged_df = merged_df[
    ["time", "max_pain", "pcr"]
]

# =====================================================
# BUILD CANDLE DATAFRAME
# =====================================================
candles = candle_res["data"]["candles"]

df_candle = pd.DataFrame(
    candles,
    columns=[
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "oi"
    ]
)

df_candle["datetime"] = pd.to_datetime(df_candle["datetime"])

# Convert candle timestamp to HH:MM
df_candle["time"] = df_candle["datetime"].dt.strftime("%H:%M")

# Keep only required columns
df_candle = df_candle[
    [
        "time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "oi"
    ]
]

# =====================================================
# FINAL MERGE
# =====================================================
final_df = pd.merge(
    merged_df,
    df_candle,
    on="time",
    how="left"
)

final_df.sort_values("time", inplace=True)

# =====================================================
# DISPLAY
# =====================================================
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

print("\n" + "="*120)
print("INSTRUMENT | SPOT | MAX PAIN | PCR | CANDLE DATA")
print("="*120)

print(
    final_df.to_string(
        index=False,
        formatters={
            "spot_price": "{:.2f}".format,
            "max_pain": "{:.0f}".format,
            "pcr": "{:.4f}".format,
            "open": "{:.2f}".format,
            "high": "{:.2f}".format,
            "low": "{:.2f}".format,
            "close": "{:.2f}".format,
        }
    )
)