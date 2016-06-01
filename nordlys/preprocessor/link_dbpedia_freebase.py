"""
Replaces a qrels.txt file with dbpedia entities, with the corresponding freebase entities.

Input:
    -qrels Qrels.txt
    -freebase_links Freebase links
    -output_dir Output directory

Output:
    Qrels file with DBpedia entities replaced with freebase links
@author: Tino Hakim Lazreg
"""

import argparse


def load_file(file_path):
    """
    Processes a file containing dbpedia and freebase links, and stores it in a dict

    :param file_path: path to a file
    :return: Dict containing dbpedia_url in correct format, and freebase url
    """
    open_file = open(file_path, "r")
    links = {}
    for line in open_file:
        if line.startswith("#"):
            # Skip first and last line in freebase_links.nt that contains comments
            continue
        cols = line.split()
        # Need to do string manipulation to fix the formatting of dbpedia_url so it corresponds to qrels
        dbpedia_url = cols[0]
        dbpedia_url = dbpedia_url.split("/")
        dbpedia = "<dbpedia:" + dbpedia_url[4]
        freebase = cols[2]
        # dbpedia = urllib.unquote(dbpedia).decode('utf')
        links[dbpedia] = freebase
    return links


def replace_link(qrels_path, freebase_links_path, output_dir):
    """
    Replaces dbpedia urls in qrels with freebase urls, and writes to a file.

    :param qrels_path: Path to qrels file
    :param freebase_links_path: Path to freebase_links file
    :param output_dir: Path to output file
    """
    qrels = open(qrels_path)
    freebase_links = load_file(freebase_links_path)
    output_file = open(output_dir, "w")
    count = 0
    for line in qrels:
        cols = line.split("\t")
        dbpedia = cols[2]
        # dbpedia = urllib.unquote(dbpedia).decode('utf-8')
        if dbpedia in freebase_links:
            replaced_link = line.replace(dbpedia, freebase_links[dbpedia])
            output_file.write(replaced_link)
        else:
            count += 1
            print "Link not found for: " + str(dbpedia)
    print "Number of queries links were NOT found for" + str(count)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-qrels", help="qrels.txt path")
    parser.add_argument("-freebase_links", help="freebase_links.nt path")
    parser.add_argument("-output_dir", help="output path")
    args = parser.parse_args()

    replace_link(args.qrels, args.freebase_links, args.output_dir)


if __name__ == '__main__':
    main()
