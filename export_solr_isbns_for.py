#!/usr/bin/python3

import csv, datetime, os, re, sys, urllib.request, yaml
from openpyxl import Workbook

def get_available_styles():
    styles = []
    for filename in os.listdir('export_styles'):
        styles.append(filename[:-4])
    return styles


def print_usage(styles):
    print("""
Usage: python export_solr_isbns_for.py [export_style]

Available styles:""")
    for style in styles:
        print("    * " + style)
    sys.exit(2)

available_styles = get_available_styles()
if sys.argv[1] in available_styles:
    style_name = sys.argv[1]
else:
    print_usage(available_styles)

server = yaml.safe_load(open('server.yml'))
current_style = yaml.safe_load(open('export_styles/' + style_name + '.yml'))

filename_prefix = current_style.get('output_prefix', style_name) + datetime.datetime.now().strftime('%Y%m%d%H%M%S')
output_type = current_style.get('output_type', 'csv')
csv_delimiter = current_style.get('delimiter', ',')
isbns_per_chunk = current_style.get('isbns_per_chunk', 0)
include_header_row = current_style.get('include_header_row', False) # TODO: this doesn't work yet
write_filenames_to_stdout = current_style.get('write_filenames_to_stdout', False)

temp_csv_name = 'solr_isbns.csv.tmp'

solr_port = str(server.get('port', '8983'))
solr_hostname = server.get('hostname', 'localhost')
solr_core = server.get('core', 'blacklight-core')
max_rows_to_fetch = str(server.get('max_rows_to_fetch', 5000000))
fields_to_fetch = current_style['fields_containing_isbns']
if 'non_isbn_solr_fields' in current_style:
    for field in current_style['non_isbn_solr_fields']:
        if isinstance(field, dict):
            fields_to_fetch.append(list(field.keys())[0])
        else:
            fields_to_fetch.append(field)
if 'exclude_facet_values' in current_style:
    for field in current_style['exclude_facet_values']:
        fields_to_fetch.append(list(field.keys())[0])

data_url = 'http://' + solr_hostname + ':' + solr_port + '/solr/' + solr_core + '/select?rows=' + max_rows_to_fetch + '&fl=' + ','.join(fields_to_fetch) + '&wt=csv'
response = urllib.request.urlretrieve(data_url, temp_csv_name)

i = 0
if not isbns_per_chunk:
    output_name = filename_prefix + '.' + output_type
    if 'csv' == output_type:
        out = open(output_name, 'w', encoding='utf8')
        csv_writer = csv.writer(out, delimiter=csv_delimiter)
    elif 'xlsx' == output_type:
        xls = Workbook()
        ws = xls.active

with open(temp_csv_name, 'r', encoding='utf8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        for field in current_style['fields_containing_isbns']:
            for isbn in row[field].split(','):
               if re.match(r'^[X0-9]{10,13}', isbn) is not None:
                   if isbns_per_chunk:
                       if 0 == (i % isbns_per_chunk):
                           file_num = int(i / isbns_per_chunk) + 1
                           output_name = filename_prefix + '.' + str(file_num) + '.' + output_type
                           xls = Workbook()
                           ws = xls.active
                           if include_header_row:
                               ws.append(['ISBN'])
                               i += 1
                       elif (isbns_per_chunk - 1) == (i % isbns_per_chunk):
                           if 'xlsx' == output_type:
                               xls.save(output_name)
                           if write_filenames_to_stdout:
                               print(output_name)
                   output_row = [re.sub(r'^([X0-9]{10,13}).*', r"\1", isbn)]
                   if 'non_isbn_solr_fields' in current_style:
                       for field in current_style['non_isbn_solr_fields']:
                           if isinstance(field, dict):
                               for fieldname, metadata in field.items():
                                   if 'character_limit' in metadata:
                                       output_row.append(row[fieldname][:metadata['character_limit']-1])
                           else:
                               output_row.append(row[field])
                   if 'literal_fields' in current_style:
                       for field in current_style['literal_fields']:
                           for fieldname, value in field.items():
                               output_row.append(str(value))
                   if 'csv' == output_type:
                       csv_writer.writerow(output_row)
                   elif 'xlsx' == output_type:
                       ws.append(output_row)
                   i += 1

if 'xlsx' == output_type:
    xls.save(output_name)

os.remove(temp_csv_name)
