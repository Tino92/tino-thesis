"""
Matches and replaces entity mentions from ClueWeb data with FACC annotations,
 and writes the ClueWeb data with freebase identifier to plain text files.

Input:
    -ann_file annotation file in .tsv format
    -warc_file ClueWeb file in .warc format

Output:
    Cleaned records with entity mentions replaced, as a gzip containing the .txt files.
    
@author: Tino Hakim Lazreg
"""

import logging
import os
import os.path
import re
from HTMLParser import HTMLParser
from warc.utils import FilePart
from warc import WARCRecord
import cchardet as chardet
from bs4 import BeautifulSoup
from unidecode import unidecode

ex_ws_re = re.compile('\\s+')
ex_non_alpha_re = re.compile('\\W+')
ex_punct_re = re.compile('[\\.,;:]+')

# Create logger
logger = logging.getLogger('')
logger.setLevel(logging.DEBUG)
# filehandle = logging.FileHandler('ClueWeb_B13_logger.csv')
filehandle = logging.FileHandler('records_indexed_fix.csv')
filehandle.setLevel(logging.DEBUG)
# Create formatter
formatter = logging.Formatter('%(name)s,%(message)s')
filehandle.setFormatter(formatter)
logger.addHandler(filehandle)


# CSV format: Document id, annotation entity mention, found entity in document, cleaned entity, cleaned annotation


class Annotation(object):
    """

    :param trec_id:
    :param encoding:
    :param entity_mention:
    :param start_pos:
    :param end_pos:
    :param freebase_id:
    """
    all_encodings_tried = None

    def __init__(self, trec_id, encoding, entity_mention, start_pos, end_pos, freebase_id):
        self.trec_id = trec_id
        self.encoding = encoding
        self.entity_mention = entity_mention
        self.start_pos = start_pos
        self.end_pos = end_pos
        self.freebase_id = freebase_id

    @staticmethod
    def parse_annotation(annotation):
        """
        Extracts annotation from FACC .tsv file.
        :param annotation: Annotation from tsv file
        :return: Annotation object
        """
        cols = annotation.split('\t')
        # Replace '/' with '_' for lucene indexing
        cols[7] = cols[7].replace('/', '_')
        cols[3] = int(cols[3])
        cols[4] = int(cols[4])
        return Annotation(cols[0], cols[1], cols[2], cols[3], cols[4], cols[7])


class WarcEntry(object):
    def __init__(self, warc_path, warc_file, annotation_list):
        self.warc_path = warc_path
        self.warc_file = warc_file
        self.reader = self.warc_file.reader
        self.annotation_list = annotation_list

    @staticmethod
    def strip_tags(text):
        """
        Uses a HTMLParser to strip HTML- ,script- and style-tags
        :param text:
        :return: stripped text
        """
        soup = BeautifulSoup(text, from_encoding='utf-8')

        for script in soup(["script", "style", "sup"]):
            script.extract()

        return soup.get_text()

    @staticmethod
    def resolve_html_entities(text):
        if '&eacute' in text:
            text = text.replace("&eacute", "e")
        if '&aring' in text:
            text = text.replace("&aring", "a")
        if '&agrave' in text:
            text = text.replace("&agrave", "a")
        if '&nbsp' in text:
            text = text.replace("nbsp", "")
        if '&#928' in text:
            text = text.replace("&#928", "pi")
        if '&reg' in text:
            text = text.replace("&reg", "")
        if '&amp' in text:
            text = text.replace("&amp", "")
        if '&oacute' in text:
            text = text.replace("&oacute", "o")
        if '&aacute' in text:
            text = text.replace("&aacute", "a")
        if '&uacute' in text:
            text = text.replace("&uacute", "u")
        if '&iacute' in text:
            text = text.replace("&iacute", "i")
        if 'st1:placetype' in text:
            text = text.replace("st1:placetype", "")
        if 'st1:placename' in text:
            text = text.replace("st1:placename", "")
        if 'x-tad-smaller' in text:
            text = text.replace("x-tad-smaller", "")
        if 'xml:namespace' in text:
            text = text.replace("xml:namespace", "")
        return text

    @staticmethod
    def remove_non_words(text):
        return ex_non_alpha_re.sub("", text)

    @staticmethod
    def fold_to_ascii(text):
        folded_text = []
        for c in text:
            if c == u'\u03bc':
                c = 'u'
            elif c == u'\u03f5':
                c = 'e'
            elif c == u'\xae':
                c = ''
            folded_text.append(c)
        return ''.join(folded_text)

    @staticmethod
    def write_record(output_data, output_dir):
        """
        Cleans the record payload, and writes it to a .txt file.

        :param output_dir: Output directory
        :param output_data: Dictionary containing record_id and payload
        """

        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)

        for record in output_data:
            fname = os.path.join(output_dir, record['record_id'] + "replaced.txt")
            with open(fname, "w") as f:
                f.write(record['replaced_record'])
            fname = os.path.join(output_dir, record['record_id'] + "cleaned.txt")
            with open(fname, "w") as f:
                f.write(record['cleaned_record'])

    def clean_text(self, text):
        """
        Cleans the text.
        :param text: Text to be cleaned
        :return: cleaned text
        """
        try:
            text = text.decode('utf-8')
        except UnicodeDecodeError:
            return ""
        # Quick fix
        text += ";"
        if "</2>" in text:
            text = text.replace("</2>", "")
        if "<<" in text:
            text = text.replace("<<", "")
        if '\x00\x00' in text:
            text = text.replace("\x00\x00", "ph")
        # if '\xce\xa0' in text:
        #    text = text.replace("\xce\xa0", "pi")
        # strip html tags
        text = self.strip_tags(text)
        # Resolve HTML-entities (e.g. &amp)
        text = text.lower()
        parser = HTMLParser()
        text = parser.unescape(text)
        text = self.resolve_html_entities(text)
        # Replace multiple whitespace
        text = ex_ws_re.sub(" ", text)
        # Transliterate an Unicode object into an ASCII string
        text = self.fold_to_ascii(text)
        text = unidecode(text)
        # Remove punctuation
        text = ex_punct_re.sub("", text)
        return text

    def clean_full_text(self, text):
        text = text.decode('utf-8', 'replace')
        # Find index of <head>, and remove everything before that index
        i = text.find('<head')
        if i > 0:
            text = text[i:]
        text = self.strip_tags(text)
        text = unidecode(text)
        text = ex_ws_re.sub(" ", text)
        return text.lower()

    def match_text(self, payload, annotation, annotation_list, annotation_bytes):
        """
        Try different encodings, if no one works, use python library chardet, to detect encoding.
        :param annotation_bytes:
        :param annotation_list:
        :param payload: The records payload
        :param annotation: The annotation object with the entity mention.
        :return: Record payload with the replaced entity mentions, or False if no match was found.
        """
        encodings = ["ISO-8859-2", "utf-8", "ISO-8859-1", "WINDOWS-1252", "utf-8-sig", "BIG5", "GB18030", "EUC-JP",
                     "EUC_KR", "GB", "GB18_bug", "SHIFT_JIS", "win", "GB18030_bug", "win_bug", "html_bug",
                     "clueweb16_bug", "clueweb12_bug"]
        match, annotation_list, annotation_bytes = self.find_entity_mention(payload, annotation, annotation.encoding,
                                                                            annotation_list, annotation_bytes,
                                                                            encodings)
        if match:
            return match, annotation_list, annotation_bytes
        else:
            for encoding in encodings:
                match, annotation_list, annotation_bytes = self.find_entity_mention(payload, annotation, encoding,
                                                                                    annotation_list, annotation_bytes,
                                                                                    encodings)
                if match:
                    return match, annotation_list, annotation_bytes
        if annotation.trec_id == "clueweb12-1814wb-94-22661" or annotation.trec_id == "clueweb12-1814wb-78-05009":
            annotation.start_pos -= 2
            annotation.end_pos -= 2
        result = chardet.detect(payload)
        encoding = result['encoding']
        matches_detect_enc, annotation_list, annotation_bytes = self.find_entity_mention(payload, annotation, encoding,
                                                                       annotation_list, annotation_bytes, encodings)
        if matches_detect_enc:
            return matches_detect_enc, annotation_list, annotation_bytes
        else:
            annotation.all_encodings_tried = True
            # Try to add 32768 bytes to the offsets, this is a bug found in some annotations,
            # Where the annotations have a higher byte offset, than the content_length of the document
            annotation.start_pos -= 32768
            annotation.end_pos -= 32768
            annotation.encoding = "UTF-8"
            matches_content_length_bug, annotation_list, annotation_bytes = self.find_entity_mention(payload, annotation,
                                                                                   annotation.encoding, annotation_list,
                                                                                   annotation_bytes, encodings)
            if matches_content_length_bug:
                return matches_content_length_bug, annotation_list, annotation_bytes
            else:
                return False, annotation_list, annotation_bytes

    @staticmethod
    def fix_annotation_offset(encoding, annotation):
        if encoding == "EUC_KR":
            start_pos = annotation.start_pos + 6
            end_pos = annotation.end_pos + 6
        elif encoding == "ISO-8859-1":
            start_pos = annotation.start_pos + 3
            end_pos = annotation.end_pos + 3
        elif encoding == "GB18_bug":
            start_pos = annotation.start_pos + 12
            end_pos = annotation.end_pos + 12
        elif encoding == "EUC-JP":
            start_pos = annotation.start_pos + 10
            end_pos = annotation.end_pos + 10
        elif encoding == "SHIFT_JIS":
            start_pos = annotation.start_pos - 6
            end_pos = annotation.end_pos - 6
        elif encoding == "BIG5":
            start_pos = annotation.start_pos + 18
            end_pos = annotation.end_pos + 18
        elif encoding == "GB18030":
            start_pos = annotation.start_pos + 2
            end_pos = annotation.end_pos + 2
        elif encoding == "win":
            start_pos = annotation.start_pos - 12
            end_pos = annotation.end_pos - 12
        elif encoding == "GB":
            start_pos = annotation.start_pos + 5
            end_pos = annotation.end_pos + 5
        elif encoding == "GB18030_bug":
            start_pos = annotation.start_pos + 9
            end_pos = annotation.end_pos + 9
        elif encoding == "win_bug":
            start_pos = annotation.start_pos - 15
            end_pos = annotation.end_pos - 15
        elif encoding == "html_bug":
            start_pos = annotation.start_pos
            end_pos = annotation.end_pos + 3
        elif encoding == "clueweb16_bug":
            start_pos = annotation.start_pos + 13
            end_pos = annotation.end_pos + 13
        elif encoding == "clueweb12_bug":
            start_pos = annotation.start_pos - 24
            end_pos = annotation.end_pos - 24
        return start_pos, end_pos

    def find_entity_mention(self, payload, annotation, encoding, annotation_list,
                            annotation_bytes, encodings):
        """
        Finds the entity mention at the given byte offsets.
        Match the entity mention in the record with the annotation.
        If there is no match, we try with a different encoding.

        :param encodings:
        :param annotation_bytes:
        :param annotation_list:
        :param encoding:
        :param replaced_payload: Record payload with the replaced entity mentions
        :param payload: The records payload
        :param annotation: The annotation object with the entity mention.
        :return: Record payload with the replaced entity mentions, or False if no match was found.
        """
        problem_encodings = ["EUC_KR", "GB18030", "SHIFT_JIS", "ISO-8859-1", "BIG5", "EUC-JP", "GB", "GB18_bug", "win",
                             "GB18030_bug", "win_bug", "html_bug", "clueweb16_bug", "clueweb12_bug"]

        if encoding in problem_encodings:
            record_content = payload.decode("utf-8", 'replace')
            problem_encoding = True
        else:
            record_content = payload.decode(encoding, 'replace')
            problem_encoding = False

        if encoding == "utf-8-sig":
            record_content = bytearray(record_content, 'utf-8-sig')
        else:
            record_content = bytearray(record_content, 'utf-8')

        if problem_encoding:
            start_pos, end_pos = self.fix_annotation_offset(encoding, annotation)
        elif encoding == "ISO-8859-9":
            start_pos = annotation.start_pos + 3
            end_pos = annotation.end_pos + 3
        elif annotation.trec_id == "clueweb12-1601wb-96-13305":
            start_pos = annotation.start_pos - 18
            end_pos = annotation.end_pos - 18
        else:
            start_pos = annotation.start_pos
            end_pos = annotation.end_pos

        found_entity = record_content[start_pos:end_pos]

        try:
            cleaned_entity = self.clean_text(found_entity)
            cleaned_entity = self.remove_non_words(cleaned_entity)

            cleaned_ann = self.clean_text(annotation.entity_mention)
            cleaned_ann = self.remove_non_words(cleaned_ann)
        except UnicodeDecodeError:
            return False

        if cleaned_entity == cleaned_ann:
            # Update encoding in object, in case another encoding was used
            annotation.encoding = encoding
            # Use start_pos of annotation as key, and freebase_id as value
            annotation_list[start_pos] = bytearray(annotation.freebase_id)
            # Add the bytes used for the annotation
            annotation_bytes += range(start_pos, end_pos)
            return record_content, annotation_list, annotation_bytes
        else:
            if annotation.all_encodings_tried:
                annotation.start_pos += 32768
                annotation.end_pos += 32768
                found_entity = record_content[start_pos + 32768:end_pos + 32768]
                cleaned_entity = self.clean_text(found_entity)
                cleaned_entity = self.remove_non_words(cleaned_entity)
                logger.warn(str(annotation.trec_id) + ', ' + str(self.warc_path) + ', ' +
                            str(annotation.entity_mention) + ', ' + str(found_entity) +
                            ', ' + str(cleaned_entity) + ', ' + str(cleaned_ann))
            return False, annotation_list, annotation_bytes

    @staticmethod
    def create_replacement_content(record_content, annotation_list, annotation_bytes):
        replacement_content = bytearray()
        for count, byte in enumerate(record_content):
            # List containing all bytes in range(start_pos, end_pos) for the annotations
            if count not in annotation_bytes:
                replacement_content.append(byte)
            else:
                if count in annotation_list:  # dict containing start_pos of all annotations
                    replacement_content += annotation_list[count]
        return replacement_content

    @staticmethod
    def read_record(warc_reader):
        warc_reader.finish_reading_current_record()
        fileobj = warc_reader.fileobj
        try:
            header = warc_reader.read_header(fileobj)
        except IOError:
            return None, None, None

        if header is None:
            return None, None, None
        warc_reader.current_payload = FilePart(fileobj, header.content_length)
        payload = warc_reader.current_payload.read()
        record_id = header.get('WARC-TREC-ID', None)
        record = WARCRecord(header, warc_reader.current_payload, defaults=False)
        return record, payload, record_id

    def replace_entity_mentions(self):
        """ Traverses all records in a warc_file and finds the corresponding annotations.
        """
        entity_found_count = 0
        entity_not_found_count = 0

        replaced_payload = None
        ann_end = None
        ann_iter = iter(self.annotation_list)
        try:
            ann = ann_iter.next()
        except StopIteration:
            print("Annotation .tsv is empty")
            return False

        entities_record = ""
        output_data = []
        annotation_list = {}
        annotation_bytes = []
        record, warc_payload, record_id = self.read_record(self.reader)

        while record is not None:
            if record_id is None:
                record, warc_payload, record_id = self.read_record(self.reader)
                replaced_payload = None
            if record_id == ann.trec_id:
                entity_found, annotation_list, annotation_bytes = self.match_text(warc_payload, ann, annotation_list,
                                                                                  annotation_bytes)
                if entity_found:
                    replaced_payload = entity_found
                    entities_record += ann.freebase_id
                    entity_found_count += 1
                elif not entity_found:
                    entity_not_found_count += 1
                try:
                    ann = ann_iter.next()
                except StopIteration:
                    ann_end = True
            if ann.trec_id < record_id:
                try:
                    ann = ann_iter.next()
                except StopIteration:
                    ann_end = True
            if ann.trec_id > record_id or ann_end:
                if replaced_payload:
                    replacements = self.create_replacement_content(replaced_payload, annotation_list, annotation_bytes)
                    # Delete contents in annotation_list and annotation_bytes
                    annotation_list.clear()
                    del annotation_bytes[:]
                    cleaned_replaced_record = self.clean_full_text(replacements)
                    replaced_payload = None
                    replacements = None
                else:
                    if warc_payload is not None:
                        record_content = warc_payload.decode('utf-8', 'replace')
                        record_content = bytearray(record_content, 'utf-8')
                        cleaned_replaced_record = self.clean_full_text(record_content)
                        record_content = None
                if warc_payload is not None: 
                    cleaned_record = self.clean_full_text(warc_payload)
                    output_data.append({'record_id': record_id,
                                        'replaced_record': cleaned_replaced_record,
                                        'cleaned_record': cleaned_record,
                                        'entities_record' : entities_record})
                entities_record = ""
                cleaned_record = None
                cleaned_replaced_record = None
                warc_payload = None
                record, warc_payload, record_id = self.read_record(self.reader)

        print "Entities found: " + str(entity_found_count)
        print "Entities NOT found: " + str(entity_not_found_count)
        return output_data
