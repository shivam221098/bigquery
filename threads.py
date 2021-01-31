import sqlite3
import pandas as pd
import pandas_gbq
import logging
import time
import os
import concurrent.futures
import json

logging.basicConfig(filename="logs.log", filemode='a', format='%(name)s - %(levelname)s - %(message)s')
connection = None


def connection_opener(choice, filename):
    global connection
    if choice.lower() == "memory":
        connection = sqlite3.connect(':memory:')
    else:
        connection = sqlite3.connect(os.path.join(f"temporary/{filename}.db"))


def major_descriptor(yn):
    if yn.lower() == 'y':
        return True
    return False


def create_year_month(year_month):
    if year_month.get("MedlineDate") is not None:
        year_month = year_month.get("MedlineDate")
        if year_month is None:
            Year, Month = None, None
        else:
            if len(year_month.split()) <= 2:
                if len(year_month.split()) == 2:
                    Year, Month = list(year_month.split())
                else:
                    Year = year_month
                    Month = None
            else:
                Year, Month = None, None
    else:
        Year = year_month.get("Year")
        Month = year_month.get("Month")

    return Year, Month


def create_issn(iss):
    if iss is not None:
        issn = iss.get("#text")
        issn_type = iss.get("@IssnType")
    else:
        issn = None
        issn_type = None

    return issn, issn_type


def author_list(auth_l):
    if auth_l is None:
        author = [{"Initials": None, "ForeName": None, "LastName": None}]
    else:
        author = auth_l.get("Author")

    return author


def create_date(d_m_y):
    if d_m_y is None:
        date = None
    else:
        date = d_m_y.get("Day") + '/' + \
               d_m_y.get("Month") + '/' + \
               d_m_y.get("Year")

    return date


def check_article(article):
    if type(article) is not str:
        if type(article) is dict:
            return article.get("#text")
        else:
            return None
    else:
        article = article.lstrip('[')
        return article.rstrip(']')


def create_date_revised(d_m_y):
    if d_m_y is None:
        date = None
    else:
        date = d_m_y.get("Day") + '/' + \
               d_m_y.get("Month") + '/' + \
               d_m_y.get("Year")

    return date


def create_title(title):
    if title is None:
        return None
    else:
        title = title.lstrip('[')
        return title.rstrip(']')


def create_affiliation(affiliation):
    if affiliation is None:
        return None
    else:
        if type(affiliation) is not list:
            return [affiliation]
        return affiliation


def database_setup(filename):
    # connection = sqlite3.connect(f"{filename}.db")
    cursor = connection.cursor()
    with connection:
        cursor.execute("CREATE TABLE pm_ext_authors_affiliations(pmid INTEGER, author_ordinality INTEGER, "
                       "initials TEXT, fore_name TEXT, last_name TEXT, "
                       "affiliation_ordinality INTEGER, affiliation TEXT, PRIMARY KEY(pmid, author_ordinality, "
                       "affiliation_ordinality), UNIQUE(pmid, author_ordinality, affiliation_ordinality))")

        cursor.execute("CREATE TABLE pm_ext_articles_revised_journals(pmid INTEGER PRIMARY KEY UNIQUE, "
                       "article_title TEXT, date_created TEXT, date_revised TEXT, issn TEXT, issn_type TEXT, "
                       "cited_medium TEXT, volume TEXT, issue TEXT, "
                       "year TEXT, month TEXT,title TEXT, iso_abbreviation TEXT, nlm_uid TEXT)")

        cursor.execute("CREATE TABLE pm_ext_mesh_headings(pmid INTEGER, descriptor_uid TEXT, major_descriptor TEXT,"
                       "PRIMARY KEY(pmid, descriptor_uid), UNIQUE(pmid, descriptor_uid))")

        cursor.execute("CREATE TABLE pm_ext_publication_types(pmid INTEGER, publication_type TEXT, "
                       "publication_type_ui TEXT, publication_type_ordinality INTEGER, "
                       "PRIMARY KEY(pmid, publication_type_ordinality), UNIQUE(pmid, publication_type_ordinality))")

    connection.commit()
    # connection.close()


def fed_database(data, filename):
    # connection = sqlite3.connect(f'{filename}.db')
    cursor = connection.cursor()

    with connection:
        for value in data:
            try:
                PMID = int(value.get("MedlineCitation").get("PMID").get("#text"))
                DescriptorUIDList = value.get("MedlineCitation").get("MeshHeadingList")

                ArticleTitle = value.get("MedlineCitation").get("Article").get('ArticleTitle')

                DateCreated = create_date(value.get("MedlineCitation").get("DateCompleted"))

                AuthorList = value.get("MedlineCitation").get("Article").get("AuthorList")

                Author = author_list(AuthorList)

                DateRevised = create_date_revised(value.get("MedlineCitation").get("DateRevised"))

                ISSN = value.get("MedlineCitation").get("Article").get('Journal').get("ISSN")

                issn, issn_type = create_issn(ISSN)

                CitedMedium = value.get("MedlineCitation").get("Article").get("Journal").get("JournalIssue").get(
                    "@CitedMedium")
                Volume = value.get("MedlineCitation").get("Article").get("Journal").get("JournalIssue").get("Volume")
                Issue = value.get("MedlineCitation").get("Article").get("Journal").get("JournalIssue").get("Issue")
                year_month = value.get("MedlineCitation").get("Article").get("Journal").get("JournalIssue").get(
                    "PubDate")

                Year, Month = create_year_month(year_month)

                Title = value.get("MedlineCitation").get("Article").get('Journal').get("Title")
                ISOAbbreviation = value.get("MedlineCitation").get("Article").get('Journal').get("ISOAbbreviation")
                NlmUniqueId = value.get("MedlineCitation").get("MedlineJournalInfo").get("NlmUniqueID")
                PublicationTypeList = value.get("MedlineCitation").get("Article").get("PublicationTypeList").get(
                    "PublicationType")

                if type(PublicationTypeList) is not list:
                    PublicationTypeList = [PublicationTypeList]

                if type(Author) is not list:
                    Author = [Author]

                if DescriptorUIDList is not None:
                    DescriptorUIDList = DescriptorUIDList.get("MeshHeading")

                    if type(DescriptorUIDList) is not list:
                        DescriptorUIDList = [DescriptorUIDList]

                    for description in DescriptorUIDList:
                        major = str(major_descriptor(description.get("DescriptorName").get("@MajorTopicYN")))
                        try:
                            cursor.execute("INSERT INTO pm_ext_mesh_headings(pmid, descriptor_uid, major_descriptor) "
                                           "VALUES (?, ?, ?)", (PMID, description.get("DescriptorName").get("@UI"),
                                                                str(major)))
                        except (sqlite3.IntegrityError, sqlite3.OperationalError):
                            continue

                ArticleTitle = check_article(ArticleTitle)
                Title = create_title(Title)
                cursor.execute(
                    "INSERT INTO pm_ext_articles_revised_journals(pmid, article_title, date_created, date_revised, "
                    "issn, issn_type, cited_medium, volume, issue, year, "
                    "month, title, iso_abbreviation, nlm_uid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    "?, ?)", (
                        PMID, ArticleTitle, DateCreated, DateRevised, issn, issn_type, CitedMedium, Volume, Issue,
                        Year, Month, Title, ISOAbbreviation,
                        NlmUniqueId))

                count = 1
                for publication in PublicationTypeList:
                    try:
                        cursor.execute("INSERT INTO pm_ext_publication_types(pmid, publication_type, "
                                       "publication_type_ui, publication_type_ordinality) VALUES"
                                       "(?, ?, ?, ?)", (PMID, publication.get("#text"), publication.get("@UI"), count))
                        count += 1
                    except (sqlite3.IntegrityError, sqlite3.OperationalError):
                        continue

                count = 1  # For author's ordinality
                for author in Author:
                    Affiliation = create_affiliation(author.get("AffiliationInfo"))
                    if Affiliation is not None:
                        count_1 = 1  # For affiliation's ordinality
                        for affiliation in Affiliation:
                            try:
                                cursor.execute("INSERT INTO pm_ext_authors_affiliations VALUES (?, ?, ?, ?, ?, ?, ?)", (
                                    PMID, count, author.get("Initials"), author.get("ForeName"), author.get("LastName"),
                                    count_1, affiliation.get("Affiliation")))
                                count_1 += 1
                            except (sqlite3.IntegrityError, sqlite3.OperationalError):
                                continue

                    else:
                        try:
                            cursor.execute("INSERT INTO pm_ext_authors_affiliations(pmid, author_ordinality, initials, "
                                           "fore_name, last_name, affiliation_ordinality, affiliation) "
                                           "VALUES (?, ?, ?, ?, ?, ?, ?)",
                                           (PMID, count, author.get("Initials"), author.get("ForeName"),
                                            author.get("LastName"), 0, None))
                        except (sqlite3.IntegrityError, sqlite3.OperationalError):
                            continue
                    count += 1

            except Exception as e:
                logging.exception(f"Exception occurred! {e}")
                continue

    connection.commit()
    # connection.close()


def execute(data, filename, choice, bigquery, bg_upload_type, bg_project_id, bg_data_set, bg_table_name, out_dir):
    connection_opener(choice, filename)
    database_setup(filename)
    fed_database(data, filename)

    # connection = sqlite3.connect(f"{filename}.db")
    answer = []

    df1 = create_mesh_csv(filename, bigquery, out_dir)
    df2 = create_csv(filename, bigquery, out_dir)
    if bigquery:
        with concurrent.futures.ThreadPoolExecutor() as executor:

            futures = [executor.submit(upload_mesh_csv, df=df1, filename=filename, bg_upload_type=bg_upload_type,
                                       bg_project_id=bg_project_id, bg_data_set=bg_data_set, bg_table_name=bg_table_name),
                       executor.submit(upload_csv, df=df2, filename=filename, bg_upload_type=bg_upload_type,
                                       bg_project_id=bg_project_id, bg_data_set=bg_data_set, bg_table_name=bg_table_name)]

            for future in concurrent.futures.as_completed(futures):
                answer.append(future.result())

    connection.close()

    if bigquery:
        return answer[0] if answer[0] > answer[1] else answer[1]
    return 0


def create_mesh_csv(filename, bigquery, out_dir):
    query = "SELECT pmid, descriptor_uid, major_descriptor FROM pm_ext_mesh_headings"

    sql_query = pd.read_sql_query(query, connection)
    df = pd.DataFrame(sql_query)

    df.insert(0, "filename", [filename for _ in range(len(df))])
    df.insert(0, "row_id", [i for i in range(1, len(df) + 1)])

    if bigquery:
        print("Uploading to BQ...")
        return df

    try:
        os.mkdir(out_dir + "/CSV")
    except FileExistsError:
        pass

    filename = filename.rstrip(".xml")
    df.to_csv(out_dir + f"/CSV/{filename}_mesh.csv", index=False)

    return True


def create_csv(filename, bigquery, out_dir):
    query = "SELECT pmid, article_title, date_created, affiliation, " \
            "affiliation_ordinality, author_ordinality, initials, fore_name, " \
            "last_name, date_revised, issn, issn_type, cited_medium, volume, issue, year, month, title, " \
            "iso_abbreviation, nlm_uid, publication_type, publication_type_ui, publication_type_ordinality " \
            "FROM pm_ext_articles_revised_journals LEFT JOIN pm_ext_authors_affiliations USING(pmid) " \
            "LEFT JOIN pm_ext_publication_types pet USING(pmid)"

    # query = "SELECT * FROM pm_ext_articles_revised_journals NATURAL JOIN pm_ext_mesh_headings"
    sql_query = pd.read_sql_query(query, connection)
    df = pd.DataFrame(sql_query)

    df.insert(0, "filename", [filename for _ in range(len(df))])
    df.insert(0, "row_id", [i for i in range(1, len(df) + 1)])

    if bigquery:
        print("Uploading to BQ...")
        return df

    try:
        os.mkdir(out_dir + "/CSV")
    except FileExistsError:
        pass

    filename = filename.rstrip(".xml")
    df.to_csv(out_dir + f"/CSV/{filename}.csv", index=False)

    return True


def upload_mesh_csv(df, filename, bg_upload_type, bg_project_id, bg_data_set, bg_table_name):

    if bg_table_name:
        filename = bg_table_name
        bg_upload_type = 'append'

    start = time.time()
    filename = filename.rstrip(".xml")
    pandas_gbq.to_gbq(dataframe=df, destination_table=f'{bg_data_set}.{filename}_mesh',
                      project_id=bg_project_id, if_exists=bg_upload_type)

    return time.time() - start


def upload_csv(df, filename, bg_upload_type, bg_project_id, bg_data_set, bg_table_name):

    if bg_table_name:
        filename = bg_table_name
        bg_upload_type = "append"

    start = time.time()
    filename = filename.rstrip(".xml")
    pandas_gbq.to_gbq(dataframe=df, destination_table=f'{bg_data_set}.{filename}',
                      project_id=bg_project_id, if_exists=bg_upload_type)

    return time.time() - start
