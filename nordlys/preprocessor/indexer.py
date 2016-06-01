"""
Creates a Lucene index with ClueWeb entity mentions replaced with FACC annotations.

Input:
    -ann_dir Annotation directory
    -cluweb_dir ClueWeb directory
    -output_dir Output directory
    -num_processes Number of processes to run

Output:
    Lucene index

@author: Tino Hakim Lazreg
"""

import argparse
import fileinput
import os
import time

import parmap
import warc

from nordlys.preprocessor.clueweb_facc_preprocessor import Annotation, WarcEntry
from nordlys.retrieval.lucene_tools import Lucene

class Indexer(object):
    def __init__(self, output_dir):
        self.contents = None
        self.lucene = Lucene(output_dir)
        self.lucene.open_writer()

    def __add_to_contents(self, field_name, field_value, field_type):
        """Adds field to document contents."""
        self.contents.append({'field_name': field_name,
                              'field_value': field_value,
                              'field_type': field_type})

    def index_file(self, record_id, replaced_annotated_record, cleaned_record, entities_record):
        """Builds index.
        :param record_id: Trec-ID
        :param cleaned_record: Cleaned record (removed HTML-tags, etc.).
        :param replaced_annotated_records: Cleaned record with entity mentions replaced with Freebase-ID
        """
        self.contents = []
        self.__add_to_contents(Lucene.FIELDNAME_ID, record_id, Lucene.FIELDTYPE_ID)
        self.__add_to_contents(Lucene.FIELDNAME_CONTENTS, cleaned_record, Lucene.FIELDTYPE_TEXT_TVP)
        self.__add_to_contents("contents_annotated", replaced_annotated_record, Lucene.FIELDTYPE_TEXT_NTVP)
        self.__add_to_contents("entities", entities_record, Lucene.FIELDTYPE_TEXT_NTVP)
        self.lucene.add_document(self.contents)

    def index_files(self, results):
        """
        Call index_file() on each record in results
        :param results: List of dictionaries
        """
        
        for warc_file in results:
            # Annotation .tsv is empty
            if warc_file is False:
                continue
            for record in warc_file:
                replaced_annotated_record = self.lucene.preprocess(record['replaced_record'])
                cleaned_record = self.lucene.preprocess(record['cleaned_record'])
                self.index_file(record['record_id'], replaced_annotated_record, cleaned_record, record['entities_record'])
        self.lucene.close_writer()


def read_and_clean_files(clueweb_file, ann_file, data_dir, ann_dir):
    """
    Read file from data_dir and ann_dir, replace entity mentions and clean records in that file
    :param clueweb_file:
    :param ann_file:
    :param data_dir: Warc files directory
    :param ann_dir: Annotations directory
    :return: {'record_id': record_id,
		'replaced_record': cleaned_replaced_record,
		'cleaned_record': cleaned_record}
    """
    annotation_input = fileinput.FileInput(os.path.join(ann_dir, ann_file), openhook=fileinput.hook_compressed)
    annotation_list = []
    for line in annotation_input:
	annotation_list.append(Annotation.parse_annotation(line))

    warc_path = os.path.join(data_dir, clueweb_file)
    warc_file = warc.open(warc_path)
    print "Replacing entity mentions for ", clueweb_file, ":", ann_file, "..."
    start = time.time()
    warc_entry = WarcEntry(warc_path, warc_file, annotation_list)
    cleaned_records = warc_entry.replace_entity_mentions()
    end = time.time()
    print "Time used: ", end - start
    warc_file.close()
    return cleaned_records


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-ann_dir", help="Annotation directory")
    parser.add_argument("-clueweb_dir", help="Clueweb directory")
    parser.add_argument("-output_dir", help="Output directory")
    parser.add_argument("-num_processes", help="Number of processes to run")
    args = parser.parse_args()

    num_processes = int(args.num_processes)

    # Iterate over each subdirectory in the clueweb dir
    for subdir, dirs, files in os.walk(args.clueweb_dir):
        for folder in dirs:
            ann_dir = os.path.join(args.ann_dir, folder)
            clueweb_dir = os.path.join(args.clueweb_dir, folder)
            output_dir = os.path.join(args.output_dir, folder)
            
            ann_iter = (sorted(os.listdir(ann_dir)))
            clueweb_iter = (sorted(os.listdir(clueweb_dir)))
            # Ann_dir and clueweb_dir sometimes have different number of files
            # Loop through, and create new ann list with correct files
            ann_list = []
            for cw_file in clueweb_iter:
                for ann_file in ann_iter:
                    # Split to only get filename, and not file extensions
                    if cw_file.split(".")[0] == ann_file.split(".")[0]:
                        ann_list.append(ann_file)
            kwargs = {'processes': num_processes}
            
            start = time.time()
            # Read and clean files in parallel
            results = parmap.starmap(read_and_clean_files, zip(clueweb_iter, ann_list),
                                     clueweb_dir, ann_dir, **kwargs)
            end = time.time()
            print "Time used reading and cleaning all files", end - start
            start = time.time()
            # Initiate indexer
            indexer = Indexer(output_dir)
            # Index all the cleaned records
            indexer.index_files(results)
            end = time.time()
            print "Time used indexing all files", end - start


if __name__ == '__main__':
    main()
