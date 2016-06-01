"""
Merges several Lucene indexes into one Lucene index.

@author: Tino Hakim Lazreg
"""

import argparse
from nordlys.retrieval.lucene_tools import Lucene


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-index_dir", help="Indexes that should be merged")
    parser.add_argument("-merged_index_dir", help="Merged index directory")
    args = parser.parse_args()
    lucene = Lucene(args.merged_index_dir)
    lucene.open_writer()
    print "Merging indexes..."
    lucene.add_indexes(args.index_dir)
    print "Indexes is now merged: " + args.merged_index_dir
    lucene.close_writer()


if __name__ == '__main__':
    main()

