from sqlite3 import connect
from pandas import read_csv, Series, read_json, json_normalize, read_sql
import re

class RelationalProcessor(object):
    dbPath = ""

    # Returns the path of the database
    def getDbPath(self):
        return self.dbPath

    # Enables to set a new path for the database
    def setDbPath(self, newPath):
        self.dbPath = newPath
        
class RelationalDataProcessor(RelationalProcessor):
    pass 
    
    def uploadData(self, path):
        
        # Uploading json file
        if re.match(r"(.*)\.json$", path): 
            jsonData = read_json(path)

            # Retrieving authors data
            authors = jsonData[["authors"]].dropna()
            authors_list = []
            for idx, values in authors.iterrows():
                array_of_values = values.values[0]
                for value in array_of_values:
                    value["publicationId"] = idx
                    authors_list.append(value)
            authors = json_normalize(authors_list)
            
            # Data frame connecting person to publication (columns: orcid, publicationId)
            person_to_publication = authors[["orcid", "publicationId"]]
            # Data frame of authors (columns: family, given, orcid)
            authors = authors[["family", "given", "orcid"]].drop_duplicates()

            # Retrieving venues id data
            venues_id = jsonData[["venues_id"]].dropna()
            venues_id_list = []
            for idx, values in venues_id.iterrows():
                array_of_values = values.values[0]
                for value in array_of_values:
                    venue = { "venueId": value, "publicationId": idx }
                    venues_id_list.append(venue)

            # Data frame of venues_id (columns: venueId, publicationId)
            venues_id = json_normalize(venues_id_list)

            # Retrieving references data
            references = jsonData[["references"]].dropna()
            references_list = []
            for idx, values in references.iterrows():
                array_of_values = values.values[0]
                for value in array_of_values:
                    reference = { "originPublicationId": idx, "referencePublicationId": value }
                    references_list.append(reference)
            
            # Data frame of references (columns: originPublicationId, referencePublicationId)
            references = json_normalize(references_list)

            

            # Retrieving publishers data
            publishers = jsonData[["publishers"]].dropna()
            publishers_list = []
            for idx, values in publishers.iterrows():
                publishers_list.append(values.values[0])
            
            # Data frame of publishers (columns: id, name)
            publishers = json_normalize(publishers_list)

            # Populating the database
            with connect(self.getDbPath()) as con:
                publishers.to_sql("Publisher", con, if_exists="replace", index=False)
                authors.to_sql("Author", con, if_exists="replace", index=False)
                references.to_sql("References", con, if_exists="replace", index=False)
                venues_id.to_sql("VenuesId", con, if_exists="replace", index=False)
                person_to_publication.to_sql("PersonPublication", con, if_exists="replace", index=False)

        # Uploading csv file
        if re.match(r"(.*)\.csv$", path):
            publications = read_csv(path, 
                  keep_default_na=False,
                  dtype={
                      "id": "string",
                      "title": "string",
                      "type": "string",
                      "publication_year": "int",
                      "issue": "string",
                      "volume": "string",
                      "chapter": "string",
                      "publication_venue": "string",
                      "venue_type": "string",
                      "publisher": "string",
                      "event": "string"
                  })

            # Creating a new column with internal identifiers for each publication and adding it to the data frame Publications
            publication_internal_id = []
            for idx, row in publications.iterrows():
                publication_internal_id.append("publication-" + str(idx))
            publications.insert(0, "internalPublicationId", Series(publication_internal_id, dtype="string"))
            publications = publications.rename(columns={"publication_year": "publicationYear", "publication_venue": "publicationVenue", "venue_type": "venueType"})

            # Data frame of journal articles
            journal_articles = publications.query("type == 'journal-article'")
            journal_articles = journal_articles[["internalPublicationId", "id",  "title", "publicationYear", "publicationVenue", "issue", "volume"]]

            # Data frame of book chapters
            book_chapters = publications.query("type == 'book-chapter'")
            book_chapters = book_chapters[["internalPublicationId", "id", "title", "publicationYear", "publicationVenue", "chapter"]]

            # Data frame of proceedings papers
            proceedings_papers = publications.query("type == 'proceedings-paper'")
            proceedings_papers = proceedings_papers[["internalPublicationId", "id", "title", "publicationYear", "publicationVenue"]]

            # Creating data frame of venues (unique) and generating a list of internal identifiers for the them
            venues = publications[["publicationVenue", "venueType", "publisher", "event"]].drop_duplicates()             
            venue_internal_id = []
            for idx, row in venues.iterrows():
                venue_internal_id.append("venue-" + str(idx))
            venues.insert(0, "internalVenueId", venue_internal_id)
            venues = venues.rename(columns={"publicationVenue": "title"})
            
            # Data frame of journals
            journals = venues.query("venueType == 'journal'")
            journals = journals[["internalVenueId", "title", "publisher"]]            

            # Data frame of books
            books = venues.query("venueType == 'book'")
            books = books[["internalVenueId", "title", "publisher"]]

            # Data frame of proceedings
            proceedings = venues.query("venueType == 'proceeding'")
            proceedings = proceedings[["internalVenueId", "title", "event", "publisher"]]

            # Populating the database
            with connect(self.getDbPath()) as con:
                publications.to_sql("Publications", con, if_exists="replace", index=False)
                journal_articles.to_sql("JournalArticles", con, if_exists="replace", index=False)
                book_chapters.to_sql("BookChapters", con, if_exists="replace", index=False)
                proceedings_papers.to_sql("ProceedingsPapers", con, if_exists="replace", index=False)
                venues.to_sql("Venues", con, if_exists="replace", index=False)
                journals.to_sql("Journals", con, if_exists="replace", index=False)
                books.to_sql("Books", con, if_exists="replace", index=False)
                proceedings.to_sql("Proceedings", con, if_exists="replace", index=False)


class RelationalQueryProcessor(RelationalProcessor):
    pass
    
    # Returns a data frame with all the publications that have been published in the input year
    def getPublicationsPublishedInYear(self, year):
        with connect(self.getDbPath()) as con:
            return read_sql("SELECT * FROM Publications WHERE publicationYear = ?", con, params=[year])
    
    # Returns a data frame with all the publications that have been authored by the person having the identifier specified as input
    def getPublicationsByAuthorId(self, id):
        with connect(self.getDbPath()) as con:
            return read_sql("SELECT Publications.* FROM Publications LEFT JOIN PersonPublication ON Publications.id == PersonPublication.publicationId WHERE orcid = ?", con, params=[id])
    
    # Returns a data frame with all the publications that have received the most number of citations by other publications
    def getMostCitedPublication(self):
        with connect(self.getDbPath()) as con:
            return read_sql("SELECT *, (SELECT COUNT(originPublicationId) From 'References' Where referencePublicationId = Publications.id ) as CitationsCount FROM Publications WHERE CitationsCount > 0 Order by CitationsCount DESC", con)

    # Returns a data frame with all the venues containing the publications that, overall, have received the most number of citations by other publications
    def getMostCitedVenue(self):
        with connect(self.getDbPath()) as con:
            return read_sql("SELECT Venues.*, (SELECT COUNT(originPublicationId) From 'References' Where referencePublicationId = Publications.id) as CitationsCount FROM Publications LEFT JOIN Venues ON Publications.publicationVenue == Venues.title WHERE CitationsCount > 0  GROUP BY Venues.internalVenueId Order by CitationsCount DESC", con)

    # Returns a data frame with all the venues that have been published by the organisation having the identifier specified as input
    def getVenuesByPublisherId(self, id):
        with connect(self.getDbPath()) as con:
            return read_sql("SELECT * FROM Venues WHERE publisher = ?", con, params=[id])

    # Returns a data frame with all the publications that have been included in the venue having the identifier specified as input
    def getPublicationInVenue(self, venueId):
        with connect(self.getDbPath()) as con:
            return read_sql("SELECT Publications.* FROM Publications LEFT JOIN VenuesId ON Publications.id == VenuesId.publicationId WHERE venueId = ?", con, params=[venueId])

    # Returns a data frame with all the journal articles that have been included in the input issue of the input volume of the journal having the identifier specified as input
    def getJournalArticlesInIssue(self, issue, volume, journal_id):
        with connect(self.getDbPath()) as con:
            return read_sql("SELECT JournalArticles.* FROM JournalArticles LEFT JOIN VenuesId ON JournalArticles.id == VenuesId.publicationId WHERE issue = ? AND volume = ? AND venueId = ? ", con, params=[issue, volume, journal_id])

    # Returns a data frame with all the journal articles that have been included, independently from the issue, in input volume of the journal having the identifier specified as input
    def getJournalArticlesInVolume(self, volume, journal_id):
        with connect(self.getDbPath()) as con:
            return read_sql("SELECT JournalArticles.* FROM JournalArticles LEFT JOIN VenuesId ON JournalArticles.id == VenuesId.publicationId WHERE volume = ? AND venueId = ? ", con, params=[volume, journal_id])

    # Returns a data frame with all the journal articles that have been included, independently from the issue and the volume, in the journal having the identifier specified as input
    def getJournalArticlesInJournal(self, journal_id):
        with connect(self.getDbPath()) as con:
            return read_sql("SELECT JournalArticles.* FROM JournalArticles LEFT JOIN VenuesId ON JournalArticles.id == VenuesId.publicationId WHERE venueId = ? ", con, params=[journal_id])

    # Returns a data frame with all the proceedings that refer to the events that match (in lowercase), even partially, with the name specified as input 
    def getProceedingsByEvent(self, event_name):
        with connect(self.getDbPath()) as con:
            return read_sql("SELECT Proceedings.* FROM Proceedings WHERE event LIKE  '%" + event_name + "%'", con)

    # Returns a data frame with all the authors of the publication with the identifier specified as input 
    def getPublicationAuthors(self, publicationId):
        with connect(self.getDbPath()) as con:
            return read_sql("SELECT Author.* FROM Publications LEFT JOIN PersonPublication ON Publications.id == PersonPublication.publicationId LEFT JOIN Author ON PersonPublication.orcid == Author.orcid WHERE id = ?", con, params=[publicationId])

    # Returns a data frame with all the publications that have been authored by the people having their name matching (in lowercase), even partially, with the name specified as input 
    def getPublicationsByAuthorName(self, name):
        with connect(self.getDbPath()) as con:
            return read_sql("SELECT Publications.* FROM Publications LEFT JOIN PersonPublication ON Publications.id  == PersonPublication.publicationId LEFT JOIN Author ON PersonPublication.orcid == Author.orcid WHERE family LIKE  '%" + name + "%' OR given LIKE  '%" + name + "%'", con)

    # Returns a data frame with all the distinct publishers that have published the venues of the publications with identifiers those specified as input
    def getDistinctPublisherOfPublications(self, publication_id_list):
        publication_id_params = ""
        for publication_id in publication_id_list:
            if len(publication_id_params) > 0:
                publication_id_params += ", "
            publication_id_params += "'" + publication_id + "'"
        with connect(self.getDbPath()) as con:
            return read_sql("SELECT Publisher.* FROM Publications LEFT JOIN Publisher ON Publications.publisher = Publisher.id Where Publications.id IN (" + publication_id_params +  ")", con)

    # Additional methods

    # Returns a dataframe with all the publications that have been referenced in the publication with the publication_id specified as input
    def getPublicationsCitations(self, publication_id):
        with connect(self.getDbPath()) as con:
            return read_sql("SELECT * FROM Publications WHERE id in (SELECT referencePublicationId From 'References' WHERE originPublicationId = '" + publication_id +  "')", con)
    
    # Returns a dataframe with all venue ids associated with a venue having title specified as input
    def getVenuesIdByTitle(self, venue_title):
        with connect(self.getDbPath()) as con:
            return read_sql("SELECT VenuesId.venueId  FROM Publications JOIN VenuesId ON Publications.id == VenuesId.publicationId WHERE Publications.publicationVenue  = '" + venue_title + "'", con)

    # Returns a dataframe with all the publishers that published a venue having title specified as input
    def getPublisherByVenueTitle(self, venue_title):
        with connect(self.getDbPath()) as con:
            return read_sql("SELECT Publisher.* FROM Publisher JOIN Venues ON Venues.publisher == Publisher.id WHERE Venues.title  = '" + venue_title + "'", con)

