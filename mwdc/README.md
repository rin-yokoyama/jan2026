# MWDC analysis
## 1. Generate wire data with mwdc_processor.py
Assuming a raw data is already converted to a raw parquet file.
```
python mwdc_processor.py [input_parquet_file] --output-wire-data
```
This will generate an output file named [input_name]_mwdc.parquet or you can set it by --output-file option.
## 2. Check wire data using the chkmwdc.ipynb notebook and define cut ranges
 - Check if the map is correct <br> map files are map/dc31_map.csv, etc.
 - Look at the charge0 vs timeing0 plot and make cut for mwdc_processor.py such as:
 ```
dc31_charge_range = [50,150]
dc31_timing_range = [-35,15]
dc32_charge_range = [50,150]
dc32_timing_range = [-35,15]
 ```
Modify those ranges in mwdc_processor.py and mwdc_calib.ipynb
## 3. Generate drift time to distance parameters
 - Open mwdc_calib.ipynb file, set the cut ranges above and run.<br>
This will create drift time to distance parameter files in prm/ folder.<br>
 - Check the histograms if these look reasonable.
## 4. Run mwdc_processor.py again and check chkmwdc_calib.ipynb
 - Run mwdc_processor.py
```
python mwdc_processor.py [input_parquet_file]
```
Without the --output-wire-data option, it will output position data only.<br>
 - Check if the drift length correlations between `x1` vs `x2`, `y1` vs `y2`, etc. are shifted.
 - If so, adjust shifts parameters in the notebook so it align with the red line.
 ```
 shifts = [0.0, 0.0, 0.08, 0.15, -0.08, 0.0]
 ```
 These are the shifts to be aplied to the second planes (`x2`, `y2`, `x4`, `y4`, etc.) relative to the first ones (`x1`, `y1`, `x3`, `y3`, etc.)
 ## 5. Run mwdc_processor.py again and check chkmwdc_13shift.ipynb
 - Run mwdc_processor.py again after changing `hhifts` parameters in mwdc_processor.py
 - Open chkmwdc_13shift.ipynb and check if the histograms have the peak around `diff = 0`<br>
 These are the offsets of `x3`, `y3` planes relative to the `x1`, `y1` planes.
 - Modify `dc31_x3_shift` etc in mwdc_processor.py and re-run the script.

