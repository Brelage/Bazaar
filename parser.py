import os 
import sys
import time
from datetime import datetime
import logging
import pandas
import matplotlib.pyplot as plt
import numpy as np
import seaborn
import plotly

startprocess = time.process_time()
start = time.time()

os.makedirs("logs/parser", exist_ok=True)
logger = logging.getLogger(__name__)
logging.basicConfig(
    handlers= [
        logging.FileHandler(f"logs/parser/{datetime.now().strftime("%Y.%m.%d-%H:%M")}.log"),
        logging.StreamHandler()
    ],
    level=logging.INFO,
    format="%(asctime)s: %(message)s",
    datefmt="%Y.%m.%d %H:%M:%S"
    )

plt.style.use('_mpl-gallery')
x = np.linspace(0, 10, 100)
y = 4 + 1 * np.sin(2 * x)
x2 = np.linspace(0, 10, 25)
y2 = 4 + 1 * np.sin(2 * x2)

fig, ax = plt.subplots()

ax.plot(x2, y2 + 2.5, 'x', markeredgewidth=2)
ax.plot(x, y, linewidth=2.0)
ax.plot(x2, y2 - 2.5, 'o-', linewidth=2)

ax.set(xlim=(0, 8), xticks=np.arange(1, 8),
       ylim=(0, 8), yticks=np.arange(1, 8))

plt.show()



#with open("data", "r") as file:
 #   file.


end = time.time()
endprocess = time.process_time()
logger.info(f"""
FINISHED DATA ANALYSIS
CHECK VOLUME FOR RESULTS.
TOTAL RUNTIME: {start - end}
TOTAL CPU RUNTIME: {startprocess - endprocess}
""")
sys.exit(0)