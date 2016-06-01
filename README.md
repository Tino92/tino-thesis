# tino-thesis

Running the code:
-----------------

python -m nordlys.preprocessor.indexer -ann_dir "FACC directory" -clueweb_dir "Clueweb Directory" -output_dir "Output directory" -num_processes "number of processes to run"

Example: 
python -m nordlys.preprocessor.indexer -ann_dir "/hdd2/export/nordlys/data/ClueWeb12-FACC1/ClueWeb12_00/" -clueweb_dir "/hdd2/export/nordlys/data/collections/ClueWeb12_B13/ClueWeb12_00/" -output_dir "output/warc_fix" -num_processes 8

python -m nordlys.preprocessor.merge_indexer -index_dir "Directory of indexes" -merged_index_dir "Output directory for merged index"

Example: 
python -m nordlys.preprocessor.merge_indexer -index_dir "/home/tinohl/tino-thesis/output/warc_fix" -merged_index_dir "/home/tinohl/tino-thesis/output/merged_index"
