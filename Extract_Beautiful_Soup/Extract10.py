#Dieses Skript enthält den gesamten Prozess zur Extraktion der Daten.

import requests
from bs4 import BeautifulSoup
from itertools import chain
import re
import pandas as pd
import csv


def get_pages():
    '''
    Diese Funktion prüft die maximale Anzahl pages, die für den Link https://www.jobagent.ch/search?terms=Data%20Scientist
    im pagination slider auf Jobagent angeklickt werden kann
    :return: maximale anzahl pages als int
    '''

    myheaders = {'Accept-Language' : 'en-US,en;q=0.9',
                 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'}
    r = requests.get("https://www.jobagent.ch/search?terms=Data%20Scientist", headers = myheaders)

    soup = BeautifulSoup(r.content, "lxml")

    pagination = soup.find("ul", class_="pagination").text.strip() #suche nach pagination
    page_new = pagination.splitlines() #Splitte alle gefundenen strings in eine Liste. Dabei ist jeder Zeilenumbruch im text.strip() ein neuer Listeneintrag.
    #print(page_new) #Ergebnis z.B:: ['Seite 1', '', '', '', '1', '', '', '2', '', '', '3', '', '', '4', '', '', '5', '', '', '...', '', '', '', 'Nächste Seite']

    max_page_list = []
    for list_item in page_new:
        #Dieser For Loop, dass nur die Zahlen z.B. 1 oder 5 in die max_page_list aufgenommen werden.
        #'Seite 1', '', ... oder "Nächste Seite" sollen nicht aufgenommen werden
        if list_item.startswith("Seite"):
            "nothing"
        elif list_item.endswith("Seite"):
            "nothing"
        elif list_item == "":
            "nothing"
        elif list_item == "...":
            "nothing"
        else: max_page_list.append(int(list_item)) #Die Liste enthält nun folgende Einträge z.B. [1,2,3,4,5]

    max_page = max(max_page_list) #maximale anzahl pages wird als int übergeben für die main function
    return max_page

def get_pages_for_company_size():
    '''
    Diese Funktion prüft die maximale Anzahl pages, die für die 4 Links (z.B.: https://www.jobagent.ch/search?terms=Data+Scientist&sizes=57000001)
    im pagination slider auf Jobagent angeklickt werden kann. Die 4 Links sind anders, da das Attribut company_size nur beim Click auf einen Filter
    erzeugt werden kann.
    :return: Eine Liste pages mit vier Listelementen. Je Listeneintrag ist ein max_pages pro Link.
    '''

    myheaders = {'Accept-Language': 'en-US,en;q=0.9',
                 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'}

    r_cs1 = requests.get("https://www.jobagent.ch/search?terms=Data+Scientist&sizes=57000001", headers = myheaders)
    r_cs2 = requests.get("https://www.jobagent.ch/search?terms=Data+Scientist&sizes=57000002", headers = myheaders)
    r_cs3 = requests.get("https://www.jobagent.ch/search?terms=Data+Scientist&sizes=57000003", headers = myheaders)
    r_cs4 = requests.get("https://www.jobagent.ch/search?terms=Data+Scientist&sizes=57000004", headers = myheaders)

    r_all = [r_cs1, r_cs2, r_cs3, r_cs4]
    pages = []

    #Jeder Link wird geprüft:
    for r in r_all:
        soup = BeautifulSoup(r.content, "lxml")

        try:
            #Selbe Logik wie bei get_pages(). Für Erklärungen dort schauen.
            #Neu sind lediglich die try und except statements. Die werden gebraucht, da nicht alle Links einen pagination haben.
            #Wenn eine Seite nur eine anklickbare Seite hat, dann hat Jobagent keinen pagination-slider.

            pagination = soup.find("ul", class_="pagination").text.strip()
            page_new = pagination.splitlines()
            max_page_list = []
            for list_item in page_new:
                if list_item.startswith("Seite"):
                    "nothing"
                elif list_item.endswith("Seite"):
                    "nothing"
                elif list_item == "":
                    "nothing"
                elif list_item == "...":
                    "nothing"
                else:
                    max_page_list.append(int(list_item))
            max_page = max(max_page_list)

        except:
            max_page = 1

        pages.append(max_page)

    return (pages)


def get_request(page):
    '''
    :param page: page bis zur maximalen anzahl pages, welche mit der main function als param übergeben wird
    :return: Eine get_request item für get_job_links und get_job_first_infos.
    '''
    myheaders = {'Accept-Language': 'en-US,en;q=0.9',
                 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'}
    cookies = {'enwiki_session': '17ab96bd8ffbe8ca58a78657a918558'}
    r = requests.get(f"https://www.jobagent.ch/search?terms=Data%20Scientist&page={page}", headers = myheaders, cookies= cookies)

    return r

def get_job_links(r):
    '''
    :param r: request items pro page (!) aus der funktion get_request zum iterieren.
    :return: liste mit links. jeder listeneintrag ist ein neuer link und damit eine neue stelle.
    '''
    soup = BeautifulSoup(r.content, "lxml")
    jobs = soup.find_all("li", class_ ="item")
    links = []

    for job in jobs:
        #Dieser For Loop iteriert durch jeden "Container" auf der Seite Jobagent. Jeder Container enthält maximal ein Job.
        try:
            link = job.find("a",href=True)["href"] #Da die Suppe aus einem "li" item besteht, was eine liste ist, kan man slicen nach "href"
            links.append(link)
        except:
            link = "Kein Link"  #Manche Seiten haben Link in der Suppe. Das ist der Fall bei Werbeanzeigen, die ausgeschlossen werden sollen.
    #print(links)
    print("Es wurden insgesamt", len(links), "Links gefunden")
    return links

def get_job_first_infos(r):
    '''
    Diese Funktion kümmert sich darum alle Infos zu scrapen, die auf der Landing Page verfügbar sind
    für den Link https://www.jobagent.ch/search?terms=Data%20Scientist.
    Diese Infos sind: Primärschlüssel, Stellentitel, Arbeitgeber, Arbeitsort, Veröffentlichungsdatum, Arbeitgeber_Bewertung, Link
    :param r: request items pro page (!) aus der funktion get_request zum iterieren.
    :return: ein liste mit first_infos. jeder listeneintrag besteht aus einem dictionary.
    '''

    soup = BeautifulSoup(r.content, "lxml")
    jobs = soup.find_all("li", class_="item")
    first_infos_list = []

    for job in jobs:
        # Dieser For Loop iteriert durch jeden "Container" auf der Seite Jobagent. Jeder Container enthält maximal ein Job.
        #print(job)
        try:
            titel = job.find("span", class_ = "jobtitle").text.strip()  #im li suchen nach der klasse "jobtitle"
            arbeitsort = job.find("span", class_ ="location").text.strip()
            arbeitgeber = job.find("span", class_ = "company").text.strip()
            try:
                arbeitgeber_bewertung = job.find("b").text.strip()
            except: #Es gibt Jobs, die einen Titel haben, aber keine Arbeitgeber Bewertung. Es braucht einen Exception Handler
                arbeitgeber_bewertung = "NA"
            veroeffentlichungsdatum = "NA" #Das Veröffentlichungsdatum ist bei Jobagent nicht verfügbar. Aber wir benötigen dieses Attribut später zum Mergen.
            link = job.find("a", href=True)["href"]

        except: #Es gibt Jobs, wo alle Attribute leer sind haben. Dies ist der Fall bei den Werbeanzeigen.
            # Es braucht einen Exception Handler, damit der Code durchläuft und Python versteht, was in einem solchen Fall gemacht werden muss
            titel = "NA"
            arbeitsort = "NA"
            arbeitgeber = "NA"
            arbeitgeber_bewertung = "NA"
            veroeffentlichungsdatum = "NA"
            link = "NA"

        infos_per_job = {
            "Primärschlüssel": titel + "-" + arbeitsort + "-" + arbeitgeber,
            'Stellentitel': titel,
            'Arbeitsort': arbeitsort,
            'Arbeitgeber': arbeitgeber,
            'Arbeitgeber_Bewertung': arbeitgeber_bewertung,
            'Datum_Veröffentlichung': veroeffentlichungsdatum,
            'URL_First_Infos' : link
        }
        if link != "NA": #Wir geben Python die Anweisung keine Werbeanzeigen (aus dem Exception Handler) hinzuzufügen:
            #Logik: Nur wenn der link nicht leer ist, sollen Daten zur list hinzugefügt werden
            first_infos_list.append(infos_per_job)

    print("Es wurden ingesamt:", len(first_infos_list), "Stellen gefunden")
    return first_infos_list


def get_job_detail_infos(urls):
    '''
    Diese Funktion kümmert sich darum detaillierte Infos zu scrapen, die pro individueller URL verfügbar sind.
    Konkret kümmert sich die Funktion darum, Programmier-und Softwarekenntnisse zu finden.
    :param urls: liste mit links zum iterieren.
    :return: ein liste mit detailed_infos. jeder listeneintrag besteht aus einem dictionary.
    '''
    myheaders = {'Accept-Language': 'en-US,en;q=0.9',
                 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'}
    cookies = {'enwiki_session': '17ab96bd8ffbe8ca58a78657a918558'}
    details_infos_list = []

    for url in urls:
        r = requests.get(url, headers = myheaders, cookies= cookies)
        soup = BeautifulSoup(r.content, "lxml")

        pattern = ['Anaconda', 'Beautiful Soup', 'BI', 'BPMN', 'Bayesian Statistics', 'Business Intelligence', 'CAFM', 'Cloud Computing', 'Crawling', 'DWH', 'Data-Warehouse',
                   'Deep Learning', 'ERP', 'ER', 'ETL', 'Excel', 'GDPR', 'Git', 'JS', 'Java', 'JavaScript', 'Jupyter Notebook', 'Linear Algebra', 'Linux',
                   'Machine Learning', 'Microsoft Office', 'NoSQL', 'NonSQL', 'Numpy', 'Pandas', 'PHP', 'Power BI', 'PowerBI', 'PowerPoint', 'PyCharm', 'Python',
                   'Qlik Sense', 'RStudio', 'SAP', 'SAS', 'Selenium', 'SQL', 'Scraping', 'Tableau', 'Time Series', 'UML', 'Word','\\sR\\s']

        complete_text = soup.find("div", class_ = "content").text.strip() # Der komplette Text pro geöffneter URL.

        list_skills = []
        for i in pattern:
            # Dieser For Loop prüft, ob ein Listeneintrag im kompletten Text der geöffneten URL vorkommt.
            # z.B. Taucht das Wort "Python" irgendwo im kompletten Text auf?
            skill = re.findall(i,complete_text)
            #print(skill)
            if skill == []:
                continue #Wenn nichts gefunden wurde, dann prüfe den nächsten Eintrag im pattern.
                # z.B.Nach Python = Qlik Sense.
            else:
                list_skills.append(skill[0].strip()) #Wenn gefunden wurde, dann z.B. Python zur Liste hinzufügen
        set_skills = set(list_skills) #Da Python mehrfach im Text vorkommen könnte, wird daraus ein Set gemacht

        details_per_job = {
            "URL_Detailed_Infos" : url,
            'Programmier-und Softwarekenntnisse': set_skills #nun wird das Set zu einem Dictionary hinzugefügt
        }
        details_infos_list.append(details_per_job) #am Ende wird das dictionary in eine Liste übergeben. Jeder Listeneintrag ist ein neuer Job.

    print("Es wurden ingesamt:", len(details_infos_list), "Programmier- und Softwarekenntnisse gefunden")
    return details_infos_list


def get_job_branch_and_pensum(urls):
    '''
    Diese Funktion kümmert sich darum detaillierte Infos zu scrapen, die pro individueller URL verfügbar sind.
    Konkret kümmert sich die Funktion darum, das Arbeitspensum und die Branche zu finden.
    :param urls: liste mit links zum iterieren.
    :return: ein liste mit branche und pensum. jeder listeneintrag besteht aus einem dictionary.
    '''

    myheaders = {'Accept-Language': 'en-US,en;q=0.9',
                 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'}
    cookies = {'enwiki_session': '17ab96bd8ffbe8ca58a78657a918558'}
    branch_pensum_list = []

    for url in urls:
        r = requests.get(url, headers = myheaders, cookies= cookies)
        soup = BeautifulSoup(r.content, "lxml")

        try:
            #Die Infos für branch und pensum sind in den spans industry und workload verfügbar. das macht die Arbeit leicht.
            branch = soup.find("span", class_="industry").text.strip()
            pensum = soup.find("span", class_="workload").text.strip()

        except:
            #Manche URLS haben keine Infos zu Branche und Pensum, daher der Exception Handler.
            branch = "NA"
            pensum = "NA"


        box_intro_per_job = {
           # "URL_branche" : url,
            'Branche': branch,
            'Arbeitspensum': pensum
        }

        #print(box_intro_per_job)
        branch_pensum_list.append(box_intro_per_job)

    print("Es wurden ingesamt:", len(branch_pensum_list), "Branchen und Pensen gefunden")
    return branch_pensum_list


def combine_all_infos(first_infos, detail_infos, branch_pensum):
    '''
    Diese Funktion kümmert sich darum, dass alle Infos aus den first_infos, detail_infos, branch_pensum zusammengeführt werden.
    Da die Position einer Stelle in der Liste für alle drei gleich ist, können sie mittels update command vom dict zusammengeführt werden.
    :param first_infos
    :param detail_infos
    :param branch_pensum
    :return: all_infos
    '''

    #Iterators erstellen:
    first_infos_iterator = iter(first_infos)
    detail_infos_iterator = iter(detail_infos)
    branch_pensum_iterator = iter(branch_pensum)

    all_infos = []

    while first_infos_iterator and detail_infos_iterator and branch_pensum_iterator:
        try:
            #Mit dem Iterator wird mit jedem next, der jeweilige dictionary aus der Liste herausgenommen
            first_infos_d = next(first_infos_iterator) #Dictionary
            detail_infos_d = next(detail_infos_iterator) # Dictionary
            branch_pensum_d = next(branch_pensum_iterator) #Dictinoary

            #Da es drei dictionaries sind, kann an das erste dictionary mittels update hinzugefügt werden.
            #Ergebnis: Ein dictionary pro Stelle, statt 3 pro Stelle.
            first_infos_d.update(detail_infos_d)
            first_infos_d.update(branch_pensum_d)

        except StopIteration:
            break #Wenn kein next mehr da ist, dann soll er aus dem while loop raus.
        all_infos.append(first_infos_d) #am Ende alles in die Liste all_infos packen
    return all_infos


def get_job_company_size(page):
    '''
    Diese Funktion kümmert sich darum die vier verschiedenen Links für die Unternehmensgrössen zu scrapen.
    Jeder Link enthält einen anderen Counter, der dann benutzt wird, um
    :param page:
    :return:
    '''
    myheaders = {'Accept-Language': 'en-US,en;q=0.9',
                 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'}
    r_cs1 = requests.get(f"https://www.jobagent.ch/search?terms=Data+Scientist&sizes=57000001&page={page[0]}", headers = myheaders)
    r_cs2 = requests.get(f"https://www.jobagent.ch/search?terms=Data+Scientist&sizes=57000002&page={page[1]}", headers = myheaders)
    r_cs3 = requests.get(f"https://www.jobagent.ch/search?terms=Data+Scientist&sizes=57000003&page={page[2]}", headers = myheaders)
    r_cs4 = requests.get(f"https://www.jobagent.ch/search?terms=Data+Scientist&sizes=57000004&page={page[3]}", headers = myheaders)

    r_all = [r_cs1, r_cs2, r_cs3, r_cs4]
    counter = 0
    company_size = []

    for r in r_all:
        #Äußerer For-Loop: Einen Request Link öffnen. z.B. r_cs1 = Dieser steht für den Filter 1 bis 10 Mitarbeitende
        # und dem Link https://www.jobagent.ch/search?terms=Data+Scientist&sizes=57000001&page
        soup = BeautifulSoup(r.content, "lxml")
        jobs = soup.find_all("li", class_="item")
        counter = counter +1
        #Wenn r_cs1 göffnet ist dann counter = 1,
        #Wenn r_cs2 geöffnet ist dann counter = 2, usw.

        for job in jobs:
            #Innerer For-Loop: Selbe Logik wie bei first_infos. Dieser For Loop iteriert durch jeden "Container" auf der Seite Jobagent.
            # Jeder Container enthält maximal ein Job.
            try:
                titel = job.find("span", class_="jobtitle").text.strip() #wird benötigt, da es Primärschlüssel ist
                arbeitsort = job.find("span", class_="location").text.strip() #wird benötigt, da es Primärschlüssel ist
                arbeitgeber = job.find("span", class_="company").text.strip() #wird benötigt, da es Primärschlüssel ist
                link = job.find("a", href=True)["href"]  # Die Links werden gesucht, aber nicht zum dict hinzugefügt.

            except:
                titel = "NA"
                arbeitsort = "NA"
                arbeitgeber = "NA"
                link = "NA"

            if counter == 1:    #Manuelles Zuweisen des Filterwertes" wenn Counter erfüllt ist.
                infos_per_job = {
                    'Primärschlüssel': titel + "-" + arbeitsort + "-" + arbeitgeber,
                    'Unternehmensgrösse': "1 bis 10 Mitarbeitende"
                }


            elif counter == 2:
                infos_per_job = {
                    'Primärschlüssel': titel + "-" + arbeitsort + "-" + arbeitgeber,
                    'Unternehmensgrösse': "11 bis 100 Mitarbeitende"
                }


            elif counter == 3:
                infos_per_job = {
                'Primärschlüssel': titel + "-" + arbeitsort + "-" + arbeitgeber,
                'Unternehmensgrösse': "101 bis 1000 Mitarbeitende"
                }


            elif counter == 4:
                infos_per_job = {
                'Primärschlüssel': titel + "-" + arbeitsort + "-" + arbeitgeber,
                'Unternehmensgrösse': "mehr als 100 Mitarbeitende"
                }

            if link != "NA":  # Nur wenn der link nicht leer ist, sollen Daten zum dict hinzugefügt werden
                company_size.append(infos_per_job)

    print("Es wurden ingesamt:", len(company_size), "Stellen  mit unterschiedlichen Unternehmensgrössen gefunden")
    #print(company_size)
    return company_size

def save_csv_company_size(company_size):
    '''
    Diese Funktion erzeugt ein CSV namens JJobAgent_src_company_size.csv
    :param company_size: Input als Liste aus obiger Funktion
    :return: Kein return notwendig.
    '''

    keys = company_size[2].keys() #company size ist eine liste, die aus der abfrage get_job_company_size stammt.
    # Daher kann die Liste an irgendeiner Stelle gesliced werden und an dieser Stelle muss im dict Eintrag die Keys herausgefunden werden.
    #print(keys)

    #CSV für Company Size erzeugen:
    with open('JobAgent_src_company_size.csv', 'w') as f:
        dict_writer = csv.DictWriter(f, keys)
        dict_writer.writeheader()
        dict_writer.writerows(company_size)

    print("* The result is in the file 'JobAgent_src_company_size.csv'!  *")


def get_canton_wiki():
    '''
    Es wird wikipedia gescraped, wie es in Block_03 unter 05_Interaction_with_HTML_Forms vorgestellt wurde.
    Ein CSV namens Wiki_src.csv wird erzeugt mit allen Informationen zu Schweizer Gemeinden und insbes. in welchem Kanton sie sich befinden.
    :return: Kein return notwendig.
    '''

    #Scraping von Wikipedia:
    wiki_all = pd.read_html('https://de.wikipedia.org/wiki/Liste_Schweizer_Gemeinden')
    df = wiki_all[0]  # eine liste nur mit einem eintrag. inhalt ist ein data frame!
    # print(df.head())
    print("Wikipedia wurde gescraped")

    #CSV wird direkt erzeugt in dieser Funktion:
    df.to_csv('Wiki_src.csv', index=True)
    print("* The result is in the file 'Wiki_src.csv'!  *")

def save_csv_JobAgent(df):
    '''
    Diese Funktion erzeugt ein CSV namens JobAgent_src.csv. --> Die Hauptquelle.
    :param df: Input ist das dataframe, was über chain.from_iterable(main() der main_function übergeben wird
    :return: Kein return notwendig.
    '''
    print("Overview of the first 10 data entries:")
    print(df.head(10))
    df.to_csv('JobAgent_src.csv', index=True)
    print("* The result is in the file 'JobAgent_src.csv'!  *")

def main():
    results = []
    # Settings für PyCharm
    desired_width = 320
    pd.set_option('display.width', desired_width)
    pd.set_option('display.max_columns', 10)
    print(20*"*")
    print("Start Extraction")

    for x in range(1,get_pages()+1):
        urls = get_job_links(get_request(x))
        #print(urls)
        first_infos = get_job_first_infos(get_request(x))
        #print(first_infos)
        detail_infos = get_job_detail_infos(urls)
        #print(detail_infos)
        branch_pensum = get_job_branch_and_pensum(urls)
        #print(branch_pensum)
        print(f'Page {x} Completed.')
        all_infos = combine_all_infos(first_infos,detail_infos,branch_pensum)
        #print(all_infos)
        results.append(all_infos)
        #print(results)
    company_size = save_csv_company_size(get_job_company_size(get_pages_for_company_size()))
    get_canton_wiki()
    print("End Extraction")
    print(20*"*")
    return results

def create_pd_JobAgent():
    #Diese Funktion erzeugt ein dataframe und übergibt dies als Parameter für save_csv_JobAgent
    df = pd.DataFrame(list(chain.from_iterable(main())))
    return df

if __name__ == '__main__':
    df = create_pd_JobAgent()
    save_csv_JobAgent(df)

