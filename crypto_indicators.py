#!/usr/bin/env python3
"""
Crypto Indicators Script
Fetches 1-minute OHLCV data and computes SMA and percentage change indicators.
"""

import argparse
import csv
import json
import sys
import time
from typing import Dict, List
import requests


def fetch_ohlcv(symbol: str) -> List[Dict]:
    """
    Fetch 1-minute OHLCV data for the last 60 minutes from Binance API.
    
    Args:
        symbol: Cryptocurrency symbol (e.g., 'BTCUSDT', 'ETHUSDT')
        
    Returns:
        List of dicts with keys: timestamp, open, high, low, close, volume
        
    Raises:
        SystemExit: If API request fails or insufficient data
    """
    # Convert common symbol names to Binance format
    symbol_map = {
        'bitcoin': 'BTCUSDT',
        'ethereum': 'ETHUSDT',
        'cardano': 'ADAUSDT',
        'solana': 'SOLUSDT',
        'dogecoin': 'DOGEUSDT',
        'litecoin': 'LTCUSDT',
        'chainlink': 'LINKUSDT',
        'polkadot': 'DOTUSDT'
    }
    
    # Map symbol to Binance format if needed
    trading_pair = symbol_map.get(symbol.lower(), symbol.upper())
    if not trading_pair.endswith('USDT') and symbol.lower() in symbol_map:
        trading_pair = symbol_map[symbol.lower()]
    elif not trading_pair.endswith('USDT'):
        trading_pair = f"{trading_pair}USDT"
    
    # Binance API endpoint for kline/candlestick data
    url = "https://api.binance.com/api/v3/klines"
    
    # Get last 60 minutes of 1-minute candles
    params = {
        'symbol': trading_pair,
        'interval': '1m',
        'limit': 60
    }
    
    try:
        print(f"Fetching data for {trading_pair}...")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if not data:
            print(f"Error: No data found for {trading_pair}")
            sys.exit(1)
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        print(f"Note: Make sure the symbol '{trading_pair}' exists on Binance")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        sys.exit(1)
    
    if len(data) < 60:
        print(f"Warning: Only {len(data)} minutes of data available (expected 60)")
        if len(data) < 10:
            print("Error: Insufficient data for analysis (need at least 10 minutes)")
            sys.exit(1)
    
    # Convert Binance kline data to OHLCV format
    ohlcv_data = []
    
    for kline in data:
        # Binance kline format: [timestamp, open, high, low, close, volume, ...]
        ohlcv_data.append({
            'timestamp': int(kline[0] / 1000),  # Convert ms to seconds
            'open': float(kline[1]),
            'high': float(kline[2]),
            'low': float(kline[3]),
            'close': float(kline[4]),
            'volume': float(kline[5])
        })
    
    print(f"Successfully fetched {len(ohlcv_data)} minutes of OHLCV data")
    return ohlcv_data


def compute_sma(data: List[Dict], window: int = 10) -> List[float]:
    """
    Compute Simple Moving Average of closing prices.
    
    Args:
        data: List of OHLCV dicts
        window: Moving average window size
        
    Returns:
        List of SMA values (same length as input data)
    """
    sma_values = []
    
    for i in range(len(data)):
        if i < window - 1:
            # For first (window-1) points, use the current close price
            sma_values.append(data[i]['close'])
        else:
            # Calculate SMA for current window
            window_closes = [data[j]['close'] for j in range(i - window + 1, i + 1)]
            sma = sum(window_closes) / window
            sma_values.append(sma)
    
    return sma_values


def compute_pct_change(data: List[Dict]) -> float:
    """
    Compute percentage change from first to last close price.
    
    Args:
        data: List of OHLCV dicts
        
    Returns:
        Percentage change as float
    """
    if len(data) < 2:
        return 0.0
    
    first_close = data[0]['close']
    last_close = data[-1]['close']
    
    if first_close == 0:
        return 0.0
    
    pct_change = ((last_close - first_close) / first_close) * 100
    return pct_change


def write_csv(data: List[Dict], sma_list: List[float], pct: float, out_path: str):
    """
    Write OHLCV data with indicators to CSV file.
    
    Args:
        data: List of OHLCV dicts
        sma_list: List of SMA values
        pct: Percentage change value (constant for all rows)
        out_path: Output CSV file path
    """
    fieldnames = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'sma_10', 'pct_change']
    
    try:
        with open(out_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for i, row in enumerate(data):
                output_row = {
                    'timestamp': row['timestamp'],
                    'open': f"{row['open']:.2f}",
                    'high': f"{row['high']:.2f}",
                    'low': f"{row['low']:.2f}",
                    'close': f"{row['close']:.2f}",
                    'volume': f"{row['volume']:.2f}",
                    'sma_10': f"{sma_list[i]:.2f}" if i < len(sma_list) else "",
                    'pct_change': f"{pct:.2f}"
                }
                writer.writerow(output_row)
        
        print(f"Data successfully written to {out_path}")
        
    except IOError as e:
        print(f"Error writing CSV file: {e}")
        sys.exit(1)


def main():
    """Main function to orchestrate the crypto indicator analysis."""
    parser = argparse.ArgumentParser(
        description='Fetch crypto data and compute technical indicators'
    )
    parser.add_argument(
        '--symbol', 
        type=str, 
        default='bitcoin',
        help='Cryptocurrency symbol (default: bitcoin)'
    )
    parser.add_argument(
        '--output', 
        type=str, 
        default='crypto_indicators.csv',
        help='Output CSV file path (default: crypto_indicators.csv)'
    )
    
    args = parser.parse_args()
    
    # Fetch OHLCV data
    ohlcv_data = fetch_ohlcv(args.symbol)
    
    # Compute indicators
    print("Computing indicators...")
    sma_values = compute_sma(ohlcv_data, window=10)
    pct_change = compute_pct_change(ohlcv_data)
    
    print(f"10-minute SMA computed for {len(sma_values)} data points")
    print(f"Overall percentage change: {pct_change:.2f}%")
    
    # Write to CSV
    write_csv(ohlcv_data, sma_values, pct_change, args.output)
    
    print(f"Analysis complete! Results saved to {args.output}")


if __name__ == "__main__":
    main()