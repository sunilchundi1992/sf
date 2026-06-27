import requests
import pandas as pd
from datetime import datetime

# 1. Setup Authentication and Parameters
access_token_file = r"C:\Users\VEKSHA\Documents\Trading\Python codes\UPSTOX_FINAL\access_token.txt"
with open(access_token_file, "r") as file:
    access_token = file.read().strip()

# Target parameters (Adjust dates as per current active trading cycle)
instrument = 'NSE_INDEX|Nifty 50'
expiry_date = '2026-06-30' # Updated to match your JSON expiry
query_date = datetime.now().strftime('%Y-%m-%d')

headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Authorization': f'Bearer {access_token}'
}

# 2. Fetch Absolute Open Interest Data
oi_url = 'https://api.upstox.com/v2/market/oi'
oi_params = {'instrument_key': instrument, 'expiry': expiry_date, 'date': query_date}
oi_res = requests.get(oi_url, params=oi_params, headers=headers).json()

# 3. Fetch Change in Open Interest Data
chg_url = 'https://api.upstox.com/v2/market/change-oi'
chg_params = {'instrument_key': instrument, 'expiry': expiry_date, 'date': query_date, 'interval': 1}
chg_res = requests.get(chg_url, params=chg_params, headers=headers).json()

# 4. Data Cleansing & Merging Process
if oi_res.get('status') == 'success' and chg_res.get('status') == 'success':
    oi_data = oi_res.get('data') or {}
    chg_data = chg_res.get('data') or {}
    
    spot_price = oi_data.get('spot_closing_price')
    oi_list = oi_data.get('call_put_oi_data_list', [])
    chg_list = chg_data.get('call_put_oi_data_list', [])
    
    if oi_list and chg_list:
        # Create Absolute OI DataFrame
        df_oi = pd.DataFrame(oi_list)[['strike_price', 'call_oi', 'put_oi']]
        
        # Create Change in OI DataFrame
        df_chg = pd.DataFrame(chg_list)
        
        # ---------------------------------------------------------
        # THE FIX: Map Upstox's exact JSON keys to our DataFrame keys
        # ---------------------------------------------------------
        df_chg.rename(columns={
            'call_change_oi': 'call_chg_oi', 
            'put_change_oi': 'put_chg_oi'
        }, inplace=True)
        
        df_chg = df_chg[['strike_price', 'call_chg_oi', 'put_chg_oi']]
        
        # Merge both datasets perfectly on Strike Price
        merged_df = pd.merge(df_oi, df_chg, on='strike_price', how='inner')
        merged_df.sort_values(by='strike_price', ascending=True, inplace=True)
        
        # Calculate derived metrics
        merged_df['Net_Absolute_OI'] = merged_df['put_oi'] - merged_df['call_oi']
        merged_df['Net_Change_OI'] = merged_df['put_chg_oi'] - merged_df['call_chg_oi']
        
        # ---------------------------------------------------------
        # 5. APPLY RANGE FILTER (e.g., 24000 to 25000 based on your spot)
        # ---------------------------------------------------------
        lower_bound = 21000
        upper_bound = 27000
        filtered_df = merged_df[(merged_df['strike_price'] >= lower_bound) & (merged_df['strike_price'] <= upper_bound)].copy()
        filtered_df.reset_index(drop=True, inplace=True)
        
        if not filtered_df.empty:
            # AUTOMATIC PAIN AREA IDENTIFICATION
            max_call_strike = filtered_df.loc[filtered_df['call_oi'].idxmax(), 'strike_price']
            max_put_strike = filtered_df.loc[filtered_df['put_oi'].idxmax(), 'strike_price']
            max_call_chg_strike = filtered_df.loc[filtered_df['call_chg_oi'].idxmax(), 'strike_price']
            max_put_chg_strike = filtered_df.loc[filtered_df['put_chg_oi'].idxmax(), 'strike_price']
            
            # 6. PRINT INTEGRATED MARKET INSIGHTS
            print("\n==========================================================================")
            print(f"       COMBINED OI ANALYSIS ENGINE (Strikes: {lower_bound}-{upper_bound})")
            print("==========================================================================")
            if spot_price:
                print(f"Current Underlying Spot Price: {spot_price}")
            print(f"🟢 Structural Max SUPPORT (Floor):       {max_put_strike}")
            print(f"🔴 Structural Max RESISTANCE (Ceiling):  {max_call_strike}")
            print(f"⚡ Intraday Fresh Put Writing (Bullish): {max_put_chg_strike}")
            print(f"⚡ Intraday Fresh Call Writing (Bearish): {max_call_chg_strike}")
            print("==========================================================================\n")
            
            print("--- Comprehensive Cleansed Matrix View ---")
            print(filtered_df.to_string(index=False, formatters={
                'call_oi': '{:,.0f}'.format, 'put_oi': '{:,.0f}'.format,
                'call_chg_oi': '{:+,.0f}'.format, 'put_chg_oi': '{:+,.0f}'.format,
                'Net_Absolute_OI': '{:+,.0f}'.format, 'Net_Change_OI': '{:+,.0f}'.format
            }))
        else:
            print(f"No contracts found within the selected {lower_bound}-{upper_bound} parameters.")
    else:
        print("Data extraction failed. Verify if one or both endpoints returned empty payloads.")
else:
    print("API Execution Failed. Check authorization token or endpoint validity.")