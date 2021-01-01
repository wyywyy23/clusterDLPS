import tkinter as tk
from evalys.jobset import JobSet
from evalys import visu
js = JobSet.from_csv("./expe-out/out_jobs.csv")
visu.gantt.plot_gantt(js)
tk.mainloop()
