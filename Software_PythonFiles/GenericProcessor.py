from DataModel import Organization, Publication, Person, Proceedings, JournalArticle, Venue
from pandas import concat


class GenericQueryProcessor(object):
    queryProcessor = []

    # Cleans the list queryProcessor from all the queryProcessor objects it includes
    def cleanQueryProcessors(self):
        self.queryProcessor = []

    # Appends the input QueryProcessor object to the list queryProcessor
    def addQueryProcessor(self, processor):
        self.queryProcessor.append(processor)

    #  Returns a list of Publication objects referring to all the publications that have been published in the input year
    def getPublicationsPublishedInYear(self, year):
        processor_list = []
        result = []
        for processor in self.queryProcessor:           
            processor_list.append(processor.getPublicationsPublishedInYear(year))
        df_union = concat(processor_list, ignore_index=True)
        df_union_no_duplicates = df_union.drop_duplicates(subset=["id"])
        for idx, row in df_union_no_duplicates.iterrows():
            authors = self.getPublicationAuthors(row["id"])
            сitations = self.getPublicationsCitations(row["id"])
            venue = self.getVenueByTitle(row["publicationVenue"])
            publication = (Publication([row["id"]], row["publicationYear"], row["title"], authors, venue, сitations))
            result.append(publication)        
        return result

    # Returns a list of Publication objects referring to all the publications that have been authored by the person having the identifier specified as input
    def getPublicationsByAuthorId(self, authorId):
        processor_list = []
        result = []
        for processor in self.queryProcessor:
            processor_list.append(processor.getPublicationsByAuthorId(authorId))
        df_union = concat(processor_list, ignore_index=True)
        df_union_no_duplicates = df_union.drop_duplicates(subset=["id"])
        for idx, row in df_union_no_duplicates.iterrows():
            authors = self.getPublicationAuthors(row["id"])
            сitations = self.getPublicationsCitations(row["id"])
            venue = self.getVenueByTitle(row["publicationVenue"])
            publication = (Publication([row["id"]], row["publicationYear"], row["title"], authors, venue, сitations))
            result.append(publication)        
        return result

    # Returns the Publication object that has received the most number of citations by other publications
    def getMostCitedPublication(self):
        result = []
        for processor in self.queryProcessor:
            processorResult = processor.getMostCitedPublication()
            for idx, row in processorResult.iterrows():
                if len(result) > 0:
                    if result[0]["CitationsCount"] > row["CitationsCount"]:
                        result.append(row)
                    else:
                        result.insert(0, row)
                else:
                    result.append(row)
        if len(result) > 0:
            publication = result[0]
            authors = self.getPublicationAuthors(publication["id"])
            сitations = self.getPublicationsCitations(publication["id"])
            venue = self.getVenueByTitle(publication["publicationVenue"])
            return Publication([publication["id"]], publication["publicationYear"], publication["title"], authors, venue, сitations)
        else:
            return None

    # Returns the Venue object containing the publications that, overall, have received the most number of citations by other publications
    def getMostCitedVenue(self):
        result = []
        for processor in self.queryProcessor:
            processorResult = processor.getMostCitedVenue()
            for idx, row in processorResult.iterrows():
                if len(result) > 0:
                    if result[0]["CitationsCount"] > row["CitationsCount"]:
                        result.append(row)
                    else:
                        result.insert(0, row)
                else:
                    result.append(row)
        if len(result) > 0:
            venue = result[0]
            venueIds = self.getVenuesIdByTitle(venue["title"])
            publisher = self.getPublisherByVenueTitle(row["title"])
            return Venue(venueIds, venue["title"], publisher)
        else:
            return None

    # Returns a list of Venue objects referring to all the venues that have been published by the organisation having the identifier specified as input
    def getVenuesByPublisherId(self, publisherId):
        processor_list = []
        result = []
        for processor in self.queryProcessor:
            processor_list.append(processor.getVenuesByPublisherId(publisherId))
        df_union = concat(processor_list, ignore_index=True)
        df_union_no_duplicates = df_union.drop_duplicates(subset=["title"])
        for idx, row in df_union_no_duplicates.iterrows():
            venueIds = self.getVenuesIdByTitle(row["title"])
            publisher = self.getPublisherByVenueTitle(row["title"])
            venue = Venue(venueIds, row["title"], publisher)
            result.append(venue)
        return result

    # Returns a list of Publication objects referring to all the publications that have been included in the venue having the identifier specified as input 
    def getPublicationInVenue(self, venueId):
        processor_list = []
        result = []
        for processor in self.queryProcessor:
            processor_list.append(processor.getPublicationInVenue(venueId))
        df_union = concat(processor_list, ignore_index=True)
        df_union_no_duplicates = df_union.drop_duplicates(subset=["id"])
        for idx, row in df_union_no_duplicates.iterrows():
            authors = self.getPublicationAuthors(row["id"])
            сitations = self.getPublicationsCitations(row["id"])
            venue = self.getVenueByTitle(row["publicationVenue"])
            publication = Publication([row["id"]], row["publicationYear"], row["title"], authors, venue, сitations)
            result.append(publication)
        return result

    # Returns a list of JournalArticle objects referring to all the journal articles that have been included in the input issue of the input volume of the journal having the identifier specified as input
    def getJournalArticlesInIssue(self, issue, volume, journal_id):
        processor_list = []
        result = []
        for processor in self.queryProcessor:
            processor_list.append(processor.getJournalArticlesInIssue(issue, volume, journal_id))
        df_union = concat(processor_list, ignore_index=True)
        df_union_no_duplicates = df_union.drop_duplicates(subset=["id"])
        for idx, row in df_union_no_duplicates.iterrows():
            authors = self.getPublicationAuthors(row["id"])
            сitations = self.getPublicationsCitations(row["id"])
            venue = self.getVenueByTitle(row["publicationVenue"])
            journal = JournalArticle([row["id"]], row["publicationYear"], row["title"], authors, venue, сitations, row["issue"], row["volume"])
            result.append(journal)
        return result

    # Returns a list of JournalArticle objects referring to all the journal articles that have been included, independently from the issue, in input volume of the journal having the identifier specified as input
    def getJournalArticlesInVolume(self, volume, journal_id):
        processor_list = []
        result = []
        for processor in self.queryProcessor:
            processor_list.append(processor.getJournalArticlesInVolume(volume, journal_id))
        df_union = concat(processor_list, ignore_index=True)
        df_union_no_duplicates = df_union.drop_duplicates(subset=["id"])
        for idx, row in df_union_no_duplicates.iterrows():
            authors = self.getPublicationAuthors(row["id"])
            сitations = self.getPublicationsCitations(row["id"])
            venue = self.getVenueByTitle(row["publicationVenue"])
            journal = JournalArticle([row["id"]], row["publicationYear"], row["title"], authors, venue, сitations, row["issue"], row["volume"])
            result.append(journal)
        return result

    # Returns a list of JournalArticle objects referring to all the journal articles that have been included, independently from the issue and the volume, in the journal having the identifier specified as input
    def getJournalArticlesInJournal(self, journal_id):
        processor_list = []
        result = []
        for processor in self.queryProcessor:
            processor_list.append(processor.getJournalArticlesInJournal(journal_id))
        df_union = concat(processor_list, ignore_index=True)
        df_union_no_duplicates = df_union.drop_duplicates(subset=["id"])
        for idx, row in df_union_no_duplicates.iterrows():
            authors = self.getPublicationAuthors(row["id"])
            сitations = self.getPublicationsCitations(row["id"])
            venue = self.getVenueByTitle(row["publicationVenue"])
            journal = JournalArticle([row["id"]], row["publicationYear"], row["title"], authors, venue, сitations, row["issue"], row["volume"])
            result.append(journal)
        return result

    # Returns a list of Proceedings objects referring to all the proceedings that refer to the events that match (in lowercase), even partially, with the name specified as input
    def getProceedingsByEvent(self, eventName):
        processor_list = []
        result = []
        for processor in self.queryProcessor:
            processor_list.append(processor.getProceedingsByEvent(eventName))
        df_union = concat(processor_list, ignore_index=True)
        df_union_no_duplicates = df_union.drop_duplicates(subset=["title"])
        for idx, row in df_union_no_duplicates.iterrows():
            venueIds = self.getVenuesIdByTitle(row["title"])
            publisher = self.getPublisherByVenueTitle(row["title"])
            proceeding = Proceedings(venueIds, row["event"], row["title"], publisher)
            result.append(proceeding)
        return result

    # Returns a list of Person objects referring to all the authors of the publication with the identifier specified as input
    def getPublicationAuthors(self, publicationId):
        processor_list = []
        result = []
        for processor in self.queryProcessor:
            processor_list.append(processor.getPublicationAuthors(publicationId))
        df_union = concat(processor_list, ignore_index=True)
        df_union_no_duplicates = df_union.drop_duplicates(subset=["orcid"])
        for idx, row in df_union_no_duplicates.iterrows():
            person = Person([row["orcid"]], row["given"], row["family"])
            result.append(person)
        return result

    # Returns a list of Publication objects referring to all the publications that have been authored by the people having their name matching (in lowercase), even partially, with the name specified as input
    def getPublicationsByAuthorName(self, authorName):
        processor_list = []
        result = []
        for processor in self.queryProcessor:
            processor_list.append(processor.getPublicationsByAuthorName(authorName))
        df_union = concat(processor_list, ignore_index=True)
        df_union_no_duplicates = df_union.drop_duplicates(subset=["id"])
        for idx, row in df_union_no_duplicates.iterrows():
            authors = self.getPublicationAuthors(row["id"])
            сitations = self.getPublicationsCitations(row["id"])
            venue = self.getVenueByTitle(row["publicationVenue"])
            publication = Publication([row["id"]], row["publicationYear"], row["title"], authors, venue, сitations)
            result.append(publication)
        return result

    # Returns a list of Organization objects referring to all the distinct publishers that have published the venues of the publications with identifiers those specified as input
    def getDistinctPublisherOfPublications(self, publicationIds):
        processor_list = []
        result = []
        for processor in self.queryProcessor:
            processor_list.append(processor.getDistinctPublisherOfPublications(publicationIds))
        df_union = concat(processor_list, ignore_index=True)
        df_union_no_duplicates = df_union.drop_duplicates(subset=["id"])
        for idx, row in df_union_no_duplicates.iterrows():
            organization = Organization([row["id"]], row["name"])
            result.append(organization)
        return result

    # Returns a list of Publication objects with all the publications that have been referenced in the publication with the publication_id specified as input
    def getPublicationsCitations(self, publicationId):
        processor_list = []
        result = []
        for processor in self.queryProcessor:
            processor_list.append(processor.getPublicationsCitations(publicationId))
        df_union = concat(processor_list, ignore_index=True)
        df_union_no_duplicates = df_union.drop_duplicates(subset=["id"])
        for idx, row in df_union_no_duplicates.iterrows():
            authors = self.getPublicationAuthors(row["id"])
            venue = self.getVenueByTitle(row["publicationVenue"])
#             A line below is recursive get citations, commented due to slow work. If you decide to uncomment the line, please pass сitations as last parameter to Publication(..., сitations)
#             сitations = self.getPublicationsCitations(row["id"])
            publication = Publication([row["id"]], row["publicationYear"], row["title"], authors, venue, [])
            result.append(publication)
        return result

    # Returns a list of objects with all venue ids associated with a venue having title specified as input
    def getVenuesIdByTitle(self, venue_title):
        processor_list = []
        result = []
        for processor in self.queryProcessor:
            processor_list.append(processor.getVenuesIdByTitle(venue_title))
        df_union = concat(processor_list, ignore_index=True)
        df_union_no_duplicates = df_union.drop_duplicates(subset=["venueId"])
        for idx, row in df_union_no_duplicates.iterrows():
            result.append(row["venueId"])
        return result

    # Returns Publisher object that published a venue having title specified as input
    def getPublisherByVenueTitle(self, venue_title):
        processor_list = []
        result = []
        for processor in self.queryProcessor:
            processor_list.append(processor.getPublisherByVenueTitle(venue_title))
        df_union = concat(processor_list, ignore_index=True)
        df_union_no_duplicates = df_union.drop_duplicates(subset=["id"])
        for idx, row in df_union_no_duplicates.iterrows():
            organization = Organization([row["id"]], row["name"])
            result.append(organization)

        if len(result) > 0:
            return result[0]
        else:
            return None

    # Returns Venue object having title specified as input
    def getVenueByTitle(self, venue_title):
        venueIds = self.getVenuesIdByTitle(venue_title)
        publisher = self.getPublisherByVenueTitle(venue_title)
        return Venue(venueIds, venue_title, publisher)