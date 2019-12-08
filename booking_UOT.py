""" Booking Utilization Over Time: consider the system utilization over time: aggregate rentals per hour of the day,
and then plot the number of booked cars (or percentage of booked/parked cars) per hour versus time of day."""

from pipeline_ict4ts import pip_uot, pip_uotF
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.cbook as cbook
import matplotlib.dates as mdates
import numpy as np
from pymongo import MongoClient
import pandas as pd


"""Cluster configuration"""
url_cluster = "bigdatadb.polito.it"
port_number = 27017
authSource = 'carsharing'
username = 'ictts'
psw = 'Ictts16!'
# client connection
client = MongoClient(host=url_cluster, port=port_number,
                     ssl=True, authSource=authSource,
                     tlsAllowINvalidCertificates=True)

db = client[authSource]
db.authenticate(username, psw)

start = datetime.timestamp(datetime(2017, 10, 1, 0, 0))
end = datetime.timestamp(datetime(2017, 10, 30, 0, 0))

# =======================================================
# BOOKINGS PER HOUR in WEEK
# =======================================================
# No Filter
# Bookings
to_b = list(db['PermanentBookings'].aggregate(pip_uot(city='Torino', start_timestamp=start, end_timestamp=end)))
po_b = list(db['PermanentBookings'].aggregate(pip_uot(city='Portland', start_timestamp=start, end_timestamp=end)))
to_be = list(db['enjoy_PermanentBookings'].aggregate(pip_uot(city='Torino', start_timestamp=start, end_timestamp=end)))

to_b_x = np.arange(len(to_b))
po_b_x = np.arange(len(po_b))
to_be_x = np.arange(len(to_be))

to_b_y = [car['total'] for car in to_b]
po_b_y = [car['total'] for car in po_b]
to_be_y = [car['total'] for car in to_be]

to_b_y = np.asarray(to_b_y)
po_b_y = np.asarray(po_b_y)
to_be_y = np.asarray(to_be_y)
# Parkings
to_p = list(db['PermanentParkings'].aggregate(pip_uot(city='Torino', start_timestamp=start, end_timestamp=end)))
po_p = list(db['PermanentParkings'].aggregate(pip_uot(city='Portland', start_timestamp=start, end_timestamp=end)))
to_pe = list(db['enjoy_PermanentParkings'].aggregate(pip_uot(city='Torino', start_timestamp=start, end_timestamp=end)))

to_p_x = np.arange(len(to_p))
po_p_x = np.arange(len(po_p))
to_pe_x = np.arange(len(to_pe))

to_p_y = [car['total'] for car in to_p]
po_p_y = [car['total'] for car in po_p]
to_pe_y = [car['total'] for car in to_pe]

to_p_y = np.asarray(to_p_y)
po_p_y = np.asarray(po_p_y)
to_pe_y = np.asarray(to_pe_y)
# Filter
# Bookings
to_bF = list(db['PermanentBookings'].aggregate(pip_uotF(city='Torino', start_timestamp=start, end_timestamp=end)))
po_bF = list(db['PermanentBookings'].aggregate(pip_uotF(city='Portland', start_timestamp=start, end_timestamp=end)))
to_beF = list(db['enjoy_PermanentBookings'].aggregate(pip_uotF(city='Torino', start_timestamp=start, end_timestamp=end)))

to_b_xF = np.arange(len(to_bF))
po_b_xF = np.arange(len(po_bF))
to_be_xF = np.arange(len(to_beF))

to_b_yF = [car['total'] for car in to_bF]
po_b_yF = [car['total'] for car in po_bF]
to_be_yF = [car['total'] for car in to_beF]

to_b_yF = np.asarray(to_b_yF)
po_b_yF = np.asarray(po_b_yF)
to_be_yF = np.asarray(to_be_yF)
""" DA DECIDERE SE SERVE FILTRARE PARKINGS
# Parkings
to_pF = list(db['PermanentParkings'].aggregate(pip_uotF(city='Torino', start_timestamp=start, end_timestamp=end)))
po_pF = list(db['PermanentParkings'].aggregate(pip_uotF(city='Portland', start_timestamp=start, end_timestamp=end)))
to_peF = list(db['enjoy_PermanentParkings'].aggregate(pip_uotF(city='Torino', start_timestamp=start, end_timestamp=end)))

to_p_xF = np.arange(len(to_pF))
po_p_xF = np.arange(len(po_pF))
to_pe_xF = np.arange(len(to_peF))

to_p_yF = [car['total'] for car in to_pF]
po_p_yF = [car['total'] for car in po_pF]
to_pe_yF = [car['total'] for car in to_peF]

to_p_yF = np.asarray(to_p_yF)
po_p_yF = np.asarray(po_p_yF)
to_pe_yF = np.asarray(to_pe_yF)
"""
# ==========================================
# PLOTS
# ==========================================

# Bookings
# No FILTER
plt.figure()

plt.subplot(311)
plt.plot(to_b_x, to_b_y, linewidth=1, label='Torino')
plt.plot(po_b_x, po_b_y, linewidth=1, label='Portland')
plt.plot(to_be_x, to_be_y, linewidth=1, label='Torino enjoy')
plt.xlabel("time [hour]")
plt.ylabel("Car per hour")
plt.legend(loc='upper right')
plt.title("Bookings")
plt.xlim(0, 167)
plt.xticks(
    np.arange(len(to_b_x)+1, step=12),
    [
        '24',
        '12\nMon',
        '24',
        '12\nTue',
        '24',
        '12\nWed',
        '24',
        '12\nThu',
        '24',
        '12\nFri',
        '24',
        '12\nSat',
        '24',
        '12\nSun',
        '24'
    ]
)
plt.grid(linestyle='--', linewidth=.4, which="both")

# FILTER

plt.subplot(312)
plt.plot(to_b_xF, to_b_yF, linewidth=1, label='Torino')
plt.plot(po_b_xF, po_b_yF, linewidth=1, label='Portland')
plt.plot(to_be_xF, to_be_yF, linewidth=1, label='Torino enjoy')
plt.xlabel("time [hour]")
plt.ylabel("Car per hour")
plt.legend(loc='upper right')
plt.title("Bookings (Filter)")
plt.xlim(0, 167)
plt.xticks(
    np.arange(len(to_b_x)+1, step=12),
    [
        '24',
        '12\nMon',
        '24',
        '12\nTue',
        '24',
        '12\nWed',
        '24',
        '12\nThu',
        '24',
        '12\nFri',
        '24',
        '12\nSat',
        '24',
        '12\nSun',
        '24'
    ]
)
plt.grid(linestyle='--', linewidth=.4, which="both")


# Parkings
# NO FILTER

plt.subplot(313)
plt.plot(to_p_x, to_p_y, linewidth=1, label='Torino')
plt.plot(po_p_x, po_p_y, linewidth=1, label='Portland')
plt.plot(to_pe_x, to_pe_y, linewidth=1, label='Torino enjoy')
plt.xlabel("time [hour]")
plt.ylabel("Car per hour")
plt.legend(loc='upper right')
plt.title("Parkings")
plt.xlim(0, 167)
plt.xticks(
    np.arange(len(to_b_xF)+1, step=12),
    [
        '24',
        '12\nMon',
        '24',
        '12\nTue',
        '24',
        '12\nWed',
        '24',
        '12\nThu',
        '24',
        '12\nFri',
        '24',
        '12\nSat',
        '24',
        '12\nSun',
        '24'
    ]
)
plt.grid(linestyle='--', linewidth=.4, which="both")

"""
# FILTER

plt.subplot(224)
plt.plot(to_p_xF, to_p_yF, linewidth=1, label='Torino')
plt.plot(po_p_xF, po_p_yF, linewidth=1, label='Portland')
plt.plot(to_pe_xF, to_pe_yF, linewidth=1, label='Torino enjoy')
plt.xlabel("time [hour]")
plt.ylabel("Car per hour")
plt.legend(loc='upper left')
plt.title("Parkings (Filter)")
plt.xlim(0, 167)
plt.xticks(
    np.arange(len(to_b_xF)+1, step=12),
    [
        '24',
        '12\nMon',
        '24',
        '12\nTue',
        '24',
        '12\nWed',
        '24',
        '12\nThu',
        '24',
        '12\nFri',
        '24',
        '12\nSat',
        '24',
        '12\nSun',
        '24'
    ]
)
plt.grid(linestyle='--', linewidth=.4, which="both")
plt.subplots_adjust(bottom=0.1, right=0.8, top=0.9, hspace=1)
"""
plt.subplots_adjust(bottom=0.1, right=0.8, top=0.9, hspace=1)
plt.savefig(fname="plots/Book_UOT_3.png")
plt.close()



# ====================
# Fundamental Analysis
# ====================

# Plot
"""
fig, ax1 = plt.subplots()
ax1.plot(timeseries.index.values, timeseries, label='Observed')
ax1.plot(timeseries.index.values, rolmean, label='Roll Mean')
ax1.plot(timeseries.index.values, rolstd, label='Roll std')
ax1.set_title(f'Rolling Mean & Standard Deviation for {zone}')
ax1.legend(loc='best')
plt.grid(linestyle='--', linewidth=.4, which="both")

# ax.plot(timeseries.index.values, rolmean)
# ax.plot()
# rotate and align the tick labels so they look better
fig.autofmt_xdate()
# use a more precise date string for the x axis locations in the
# toolbar
ax1.fmt_xdata = mdates.DateFormatter('%Y-%m-%d')
plt.savefig(f'../plots/rolling_{zone}.png')
# plt.show(block=False)
plt.close()

"""