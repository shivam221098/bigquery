import os
from shutil import rmtree
from intermediate import run
from sys import argv
import concurrent.futures
import json
import time
import csv
import logging

logging.basicConfig(filename="logs.log", filemode='a', format='%(name)s - %(levelname)s - %(message)s')


def create_execution_history():
    file_name = 'execution_history.csv'
    fields = ["xml_file_name", "conversion_type", "task_type", "converted_file_name", "time_taken",
              "upload_time", "run_date"]
    with open(os.path.join(file_name), "a") as csv_file:
        csv_writer = csv.writer(csv_file)

        csv_writer.writerow(fields)

    return True


def get_configuration():
    with open(os.path.join("configuration.json"), 'r', encoding="utf-8") as file:
        Configuration = json.loads(file.read())

    return Configuration


def prepare_directory():
    if "execution_history.csv" not in os.listdir():
        create_execution_history()

    try:
        rmtree(os.path.join("temporary"))
    except FileNotFoundError:
        pass

    try:
        os.mkdir(os.path.join("temporary"))
    except FileExistsError:
        pass


def upload(bigquery):
    if bigquery.lower() == 'y':
        return True
    return False


def main():
    args = argv
    details = []

    prepare_directory()  # This will prepare current directory for storing temporary files
    Configuration = get_configuration()  # This will get initials properties to be used

    dir_contents = os.listdir(Configuration.get(Configuration.get("execution_os") + "_in_dir"))
    dir_length = len(dir_contents)

    in_dir = Configuration.get(Configuration.get("execution_os") + "_in_dir")
    out_dir = Configuration.get(Configuration.get("execution_os") + "_out_dir")

    bigquery = None
    if Configuration.get("conversion_type").lower() == "csv":
        bigquery = False
    elif Configuration.get("conversion_type").lower() == "bigquery":
        bigquery = True

    bg_upload_type = Configuration.get("bg_upload_type")
    bg_data_set = Configuration.get("bg_data_set")
    bg_project_id = Configuration.get("bg_project_id")
    bg_table_name = Configuration.get("bg_table_name")

    try:
        os.mkdir(in_dir + "/converted_xml")
    except FileExistsError:
        pass

    if args[1].endswith(".xml"):
        filename = args[1]

        if filename not in dir_contents:
            print("File doesn't exists. ")
            exit(0)

        print("xml_file_name    conversion_type   task_type     converted_file_name      time_secs    upload_time    "
              "run_date")
        details = [run(filename, Configuration.get("choice"), bigquery, bg_upload_type, bg_project_id,
                       bg_data_set, bg_table_name, in_dir, out_dir)]

    elif args[1].isnumeric():
        total_files = int(args[1])

        if total_files > dir_length:
            print("Entered number is greater than number of files in directory... Processing all files")
            total_files = dir_length

        print("xml_file_name    conversion_type   task_type     converted_file_name      time_secs   "
              "upload_time    run_date")

        if Configuration.get("choice") == "memory":
            details = []
            for i in range(total_files):
                details.append(
                    run(dir_contents[i], Configuration.get('choice'), bigquery, bg_upload_type, bg_project_id,
                        bg_data_set,
                        bg_table_name, in_dir, out_dir))

        else:
            details = []
            with concurrent.futures.ProcessPoolExecutor() as executor:
                futures = []
                for i in range(total_files):
                    if dir_contents[i].endswith(".xml"):
                        futures.append(executor.submit(run, filename=dir_contents[i],
                                                       choice=Configuration.get("choice"),
                                                       bigquery=bigquery, bg_upload_type=bg_upload_type,
                                                       bg_project_id=bg_project_id, bg_data_set=bg_data_set,
                                                       bg_table_name=bg_table_name, in_dir=in_dir, out_dir=out_dir))
                        time.sleep(2)

                for future in concurrent.futures.as_completed(futures):
                    details.append(future.result())

    elif args[1].lower() == "all":
        print("Files in directory:", dir_length)

        print("xml_file_name    conversion_type   task_type     converted_file_name      time_secs   "
              "upload_time    run_date")

        if Configuration.get("choice") == "memory":
            details = []
            for xml in dir_contents:
                if xml.endswith(".xml"):
                    details.append(
                        run(xml, Configuration.get("choice"), bigquery, bg_upload_type, bg_project_id=bg_project_id,
                            bg_data_set=bg_data_set, bg_table_name=bg_table_name, in_dir=in_dir, out_dir=out_dir))

        else:
            details = []
            with concurrent.futures.ProcessPoolExecutor() as executor:
                futures = []
                for xml in dir_contents:
                    if xml.endswith(".xml"):
                        futures.append(executor.submit(run, filename=xml, choice=Configuration.get("choice"),
                                                       bigquery=bigquery, bg_upload_type=bg_upload_type,
                                                       bg_project_id=bg_project_id, bg_data_set=bg_data_set,
                                                       bg_table_name=bg_table_name, in_dir=in_dir, out_dir=out_dir))
                        time.sleep(2)

                for future in concurrent.futures.as_completed(futures):
                    details.append(future.result())

    try:
        rmtree(os.path.join("temporary"))
    except FileNotFoundError:
        pass

    with open(os.path.join("execution_history.csv"), "a") as csv_file:
        csv_writer = csv.writer(csv_file)

        csv_writer.writerows(details)


if __name__ == '__main__':
    start = time.time()
    try:
        main()
    except Exception as e:
        logging.exception(f"Exception occurred! {e}")
    print("\n\nTotal Time taken:", time.time() - start)
