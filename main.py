from binance import *
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import math
import numpy as np

# Fetch data
n_timesteps_hours = 10
data = get_data("SUIUSDT", "1m", datetime.now() - timedelta(hours=n_timesteps_hours))

# Normalize prices
s = data["open"][0]
for col in ["open", "close", "low", "high"]:
    data[col] = data[col] / s * 100

# Calculate rolling volatility
data["sigma"] = data["close"].pct_change().rolling(12).std() * np.sqrt(12 * 288)  # Annualized volatility

data.dropna(inplace=True)

# Parameters
T = 1  # Time horizon (1 day)
gamma = 1  # Risk aversion parameter
k = 1.5  # Order book depth parameter
alpha = 0.1  # Volume sensitivity

profit = 0
final_q = 0
x = 0  # Cash balance
q = 0  # Inventory

orders = []
bids = []
asks = []
inv = [0]
unrealized_pnl = [0]

for timestep in range(1, len(data)):
    s = data["open"].iloc[timestep]
    t = timestep / len(data)
    
    sigma = data["sigma"].iloc[timestep]
    k = alpha * data["volume"].iloc[timestep]
    
    # Avellaneda-Stoikov model
    r = s - q * gamma * sigma**2 * (T - t)
    delta = gamma * sigma**2 * (T - t) + 2 / gamma * math.log(1 + gamma / k)
    
    bid = r - delta / 2
    ask = r + delta / 2
    
    bids.append(bid)
    asks.append(ask)
    
    # Execute trades
    if bid >= data["low"].iloc[timestep]:
        x -= bid
        q += 1
        orders.append((timestep, 1, bid))
    
    if ask <= data["high"].iloc[timestep]:
        x += ask
        q -= 1
        orders.append((timestep, -1, ask))
    
    inv.append(q)
    unrealized_pnl.append(x + q * s)

profit = x + q * s
final_q = q

print("Simulation Result:")
print("Profit:", profit)
print("Final q:", final_q)

# Plotting
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
ax3=ax2.twinx()

ax1.plot(data.index, data["open"], label="Open")
ax1.plot(data.index[1:], bids, label="Bids")
ax1.plot(data.index[1:], asks, label="Asks")
ax1.plot(data.index, data["low"], label="Low")
ax1.plot(data.index, data["high"], label="High")
ax1.legend()
ax1.set_title("Price and Orders")

ax2.plot(data.index, inv, label="Inventory")
ax3.plot(data.index, unrealized_pnl, label="Unrealized PnL")
ax2.legend()
ax2.set_title("Inventory and Unrealized PnL")

plt.tight_layout()
plt.show()