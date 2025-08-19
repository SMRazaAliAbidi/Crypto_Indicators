# Crypto Indicators Script

A Python script that fetches 1-minute cryptocurrency OHLCV data and computes technical indicators (Simple Moving Average and percentage change).

## Installation

Install the required dependency:
```bash
pip install requests
```

## Usage

Run the script with default settings (Bitcoin data):
```bash
python crypto_indicators.py
```

Run with custom cryptocurrency and output file:
```bash
python crypto_indicators.py --symbol=ethereum --output=eth_indicators.csv
```

### Command Line Arguments
- `--symbol`: Cryptocurrency symbol. Accepts common names like 'bitcoin', 'ethereum' or Binance trading pairs like 'BTCUSDT', 'ETHUSDT' (default: bitcoin)
- `--output`: Output CSV file path (default: crypto_indicators.csv)

## Output

The script generates a CSV file with the following columns:
- `timestamp`: Unix timestamp (seconds)
- `open`, `high`, `low`, `close`: Price data in USD
- `volume`: Trading volume
- `sma_10`: 10-minute Simple Moving Average of close prices
- `pct_change`: Overall percentage change from first to last close price

### Command Line Arguments
- `--symbol`: Cryptocurrency symbol. Accepts common names like 'bitcoin', 'ethereum' or Binance trading pairs like 'BTCUSDT', 'ETHUSDT' (default: bitcoin)
- `--output`: Output CSV file path (default: crypto_indicators.csv)

## Assumptions & Implementation Notes

1. **Data Source**: Uses Binance's free public API which provides true OHLCV candlestick data for 1-minute intervals. No API key required.

2. **Symbol Mapping**: The script automatically maps common cryptocurrency names ('bitcoin', 'ethereum', etc.) to Binance trading pair format ('BTCUSDT', 'ETHUSDT'). You can also directly use Binance pair symbols.

3. **Timestamp Format**: Binance returns timestamps in milliseconds; the script converts them to Unix seconds for the CSV output.

4. **SMA Calculation**: For the first 9 data points (where a full 10-minute window isn't available), the script uses the current close price as the SMA value rather than leaving it blank.

5. **Error Handling**: The script gracefully handles API failures, invalid symbols, insufficient data (warns if less than 60 minutes available, exits if less than 10 minutes), and file I/O errors.

6. **Real OHLCV Data**: Unlike price-point APIs, Binance provides actual Open, High, Low, Close, and Volume data for each minute, giving more accurate technical analysis.