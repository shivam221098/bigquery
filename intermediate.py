import xmltodict
from threads import execute
import os
import logging
import csv
import time
from shutil import move
from datetime import datetime

logging.basicConfig(filename="logs.log", filemode='a', format='%(name)s - %(levelname)s - %(message)s')


def xml_to_dict(filename, in_dir):
    with open(in_dir + f"/{filename}", 'r', encoding='utf-8') as file:
        data = xmltodict.parse(file.read())
    return data


def run(filename, choice, bigquery, bg_upload_type, bg_project_id, bg_data_set, bg_table_name, in_dir, out_dir):
    start = time.time()

    data = xml_to_dict(filename, in_dir)
    data = data.get("PubmedArticleSet").get("PubmedArticle")

    upload_time = 0
    try:
        upload_time = execute(data=data, filename=filename, choice=choice, bigquery=bigquery,
                              bg_upload_type=bg_upload_type, out_dir=out_dir, bg_project_id=bg_project_id,
                              bg_data_set=bg_data_set, bg_table_name=bg_table_name)
        move(in_dir + f"/{filename}", in_dir + f"/converted_xml/{filename}")

    except Exception as e:
        logging.exception(f"Exception occurred! {e}")

    if bigquery:
        conversion_type = "BigQuery"
    else:
        conversion_type = "csv"

    file_name = filename.rstrip(".xml") + "_mesh" + " " + filename.rstrip(".xml")

    details = [filename, conversion_type, choice, file_name, str(round(time.time() - start, 2)),
               str(upload_time), datetime.now().strftime("%m/%d/%Y, %H:%M:%S")]

    print(f"{details[0][:10]}              {details[1]}         {details[2]}           {details[3][:15]}            "
          f"{details[4]}           {details[5]}     {details[6]}")
    print()

    return details
