# SRPPAC analysis
## Components
 - srppac_processor.py: Main analysis script to process raw parquet file to SRPPAC position data
 - make_SRPPAC_prm.py: Function to generate dqdx conversion prm file by R. Kojima
 - chksrppac_pulser.ipynb: Notebook to check raw hit ch array vs charge array <br>
   Input is raw parquet file (Output of streamingv1_to_parquet.py).
 - chksrppac.ipynb: Notebook to check strip data, hit array size, and q0,q1,q2 <br>
   It requires srppac_processor.py output with `--output-strip-data` option.
 - srppac_dqdx_calib.ipynb: Notebook to generate dqdx conversion using make_SRPPAC_prm.py <br>
   It requires srppac_processor.py output *without* `--output-strip-data` option.
 - map/: CSV files for TDC ch to strip id conversion.
 - prm/: dqdx conversion parameter files.

## 1. Generate strip data with srppac_processor.py
Assuming a raw data is already converted to a raw parquet file.
```
python srppac_processor.py [input_parquet_file] --output-strip-data
```
This will generate an output file named [input_name]_mwdc.parquet or you can set it by --output-file option.
Run with `--help` for the usage.
## 2. Check wire data using the chksrppac.ipynb notebook and define cut ranges
 - Check if the map is correct <br> map files are map/srx_{preamp_type}_map.csv, etc.
 - Look at the charge0 vs timeing0 plot and make cut for srppac_processor.py, for example:
 ```
TIME_RANGE_NS = (-76,0)
 ```
Modify the range in srppac_processor.py
## 3. Generate dqdx parameters
 - Open srppac_dqdx_calib.ipynb file and run.<br>
This will create dqdx parameter files in prm/srppac_q0q1_x_{runname} folder.<br>
 - Check the histograms if these look reasonable.
## 4. Run srppac_processor.py again and check chksrppac_calib.ipynb (NOT READY YET)
 - Run srppac_processor.py
```
python srppac_processor.py [input_parquet_file]
```
Without the --output-strip-data option, it will output position data only.<br>

