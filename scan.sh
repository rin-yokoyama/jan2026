#!/bin/bash
scp -p quser@192.168.1.218:~/exp/Himac_HH015/exp-config/himac_HH015/run_nlabdaq5/data/tdcdata/00/run00${1}.dat /home/h487/data/rawdata/
streamingv1_to_parquet.py /home/h487/data/rawdata/run00${1}.dat /home/h487/data/parquet/run${1}.parquet

cd /home/h487/notebooks/jan2026/srppac
python srppac_processor.py /home/h487/data/parquet/run${1}.parquet --output-file /home/h487/data/parquet/run${1}_srppac.parquet --output-strip-data
cd -

cd /home/h487/notebooks/jan2026/mwdc
python mwdc_processor.py /home/h487/data/parquet/run${1}.parquet --output-file /home/h487/data/parquet/run${1}_mwdc.parquet --output-wire-data
cd -