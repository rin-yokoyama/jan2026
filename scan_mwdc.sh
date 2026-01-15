#!/bin/bash
streamingv1_to_parquet.py /mnt/data/h445_h487/nestdaq/tdcdata/00/run00${1}.dat /home/h487/data/parquet/run${1}.parquet #--max-blocks 100000

cd /home/h487/notebooks/jan2026/mwdc
python mwdc_processor.py /home/h487/data/parquet/run${1}.parquet --output-file /home/h487/data/parquet/run${1}_mwdc.parquet --output-for-samidare 
cd -
