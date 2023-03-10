from rdflib import Graph, URIRef, Literal, RDF
import re
from pandas import read_csv, Series, read_json, json_normalize, merge
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get


class TriplestoreProcessor(object):
    endpointUrl = ""

    # Returns the URL of the SPARQL endpoint of the triplestore
    def getEndpointUrl(self):
        return self.endpointUrl

    # Enables to set a new URL for the SPARQL endpoint of the triplestore
    def setEndpointUrl(self, newURL):
        self.endpointUrl = newURL

class TriplestoreDataProcessor(TriplestoreProcessor):
    pass

    def uploadData(self, path):
        
        # Defining literals
        a_string = Literal("a string with this string")
        a_number = Literal(42)
        a_boolean = Literal(True)

        # Defining classes of resources
        JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
        BookChapter = URIRef("https://schema.org/Chapter")
        Proceedingpapers= URIRef("https://schema.org/CreativeWork")
        Journal = URIRef("https://schema.org/Periodical")
        Book = URIRef("https://schema.org/Book")
        Proceedings= URIRef("https://schema.org/EducationEvent")  
        Person = URIRef("https://schema.org/Person")
        Organization = URIRef("https://schema.org/Organization")

        # Defining attributes related to classes
        publicationYear = URIRef("https://schema.org/datePublished")
        title = URIRef("https://schema.org/name")
        issue = URIRef("https://schema.org/issueNumber")
        volume = URIRef("https://schema.org/volumeNumber")
        identifier = URIRef("https://schema.org/identifier") # used for publication_id, publisher_id
        issn = URIRef("https://schema.org/issn")             # used for venue_id
        orcid = URIRef("https://schema.org/identifier")      # used for person_id
        name = URIRef("https://schema.org/name")
        event = URIRef("https://schema.org/event")
        position = URIRef("https://schema.org/position")
        family = URIRef("https://schema.org/familyName")
        given = URIRef("https://schema.org/givenName")

        # Defining relations among classes
        publicationVenue = URIRef("https://schema.org/isPartOf")
        publisher = URIRef("https://schema.org/publisher")
        author = URIRef("https://schema.org/author")
        cites = URIRef("https://schema.org/citation")

        # Defining the graph
        graph = Graph()

        # Uploading json file
        if re.match(r"(.*)\.json$", path):
            jsonData = read_json(path)

            # Retrieving authors data
            authors = jsonData[["authors"]].dropna()
            for idx, values in authors.iterrows():
                array_of_values = values.values[0]
                for value in array_of_values:
                    publicationSubj = URIRef(self.getEndpointUrl() + str(idx))
                    personSubj = URIRef(self.getEndpointUrl() + str(value["orcid"]))
                    
                    # Adding triples to the Graph
                    graph.add((publicationSubj, author, personSubj))

                    graph.add((personSubj, RDF.type, Person))
                    graph.add((personSubj, family, Literal(value["family"])))
                    graph.add((personSubj, given, Literal(value["given"])))
                    graph.add((personSubj, orcid, Literal(value["orcid"])))

            # Retrieving publishers data
            publishers = jsonData[["publishers"]].dropna()
            for idx, values in publishers.iterrows():
                publisher = values.values[0]
                subj = URIRef(self.getEndpointUrl() + publisher["id"])

                # Adding triples to the Graph
                graph.add((subj, RDF.type, Organization))
                graph.add((subj, identifier, Literal(publisher["id"])))
                graph.add((subj, name, Literal(publisher["name"])))

            # Retrieving references data
            references = jsonData[["references"]].dropna()
            for idx, values in references.iterrows():
                array_of_values = values.values[0]
                for value in array_of_values:
                    reference = { "origin_publication_id": idx, "reference_publication_id": value }
                    originalSubj = URIRef(self.getEndpointUrl() + str(idx))
                    referenceSubj = URIRef(self.getEndpointUrl() + str(value))

                    # Adding triples to the Graph
                    graph.add((originalSubj, cites, referenceSubj))

            # Retrieving references data
            venues_id = jsonData[["venues_id"]].dropna()
            for idx, values in venues_id.iterrows():
                array_of_values = values.values[0]
                for value in array_of_values:
                    venue = { "venue_id": value, "publication_id": idx }
                    subj = URIRef(self.getEndpointUrl() + str(idx))

                    # Adding triples to the Graph
                    graph.add((subj, issn, Literal(str(value))))

        # Uploading csv file
        if re.match(r"(.*)\.csv$", path):
            publication_read = read_csv(path,
                                        keep_default_na=False,
                                        dtype={"id": "string",
                                        "title": "string",
                                        "type": "string",
                                        "publication_year": "int",
                                        "issue": "string",
                                        "volume": "string",
                                        "chapter": "string",
                                        "publication_venue": "string",
                                        "publisher": "string",
                                        "event": "string",
                                        "venue_type" : "string"})

            # Creating data frame of venues (unique)
            venues = publication_read[["publication_venue", "venue_type", "publisher", "event"]].drop_duplicates()
            venues = venues.rename(columns={"publication_venue": "title"})

            # Iterating on data about venues
            venue_internal_id = {}
            for idx, row in venues.iterrows():
                local_id = "venue-" + str(idx)
                subj = URIRef(self.getEndpointUrl() + local_id)
                venue_internal_id[row["title"]] = subj

                # Adding triples to the Graph
                if row["venue_type"] == "journal":
                   graph.add((subj, RDF.type, Journal))
                elif row["venue_type"] == "book":
                    graph.add((subj, RDF.type, Book))
                else:
                   graph.add((subj, RDF.type, Proceedings))
                   graph.add((subj, event, Literal(row["event"])))

                graph.add((subj, title, Literal(row["title"])))
                publisherSubj = URIRef(self.getEndpointUrl() + str(row["publisher"]))
                graph.add((subj, publisher, publisherSubj))

            # Iterating on data about publications
            for idx, row in publication_read.iterrows():
                subj = URIRef(self.getEndpointUrl() + str(row["id"]))

                # Adding triples to the Graph
                if row["type"] == "journal-article":
                    graph.add((subj, RDF.type, JournalArticle))

                    graph.add((subj, issue, Literal(row["issue"])))
                    graph.add((subj, volume, Literal(row["volume"])))
                elif row["type"] == "book-chapter":
                    graph.add((subj, RDF.type, BookChapter))
                    graph.add((subj, position, Literal(row["chapter"])))

                else:
                    graph.add((subj, RDF.type, Proceedingpapers))

                graph.add((subj, publicationYear, Literal(str(row["publication_year"]))))
                graph.add((subj, title, Literal(row["title"])))
                graph.add((subj, identifier, Literal(row["id"])))
                graph.add((subj, publicationVenue, venue_internal_id[row["publication_venue"]]))

        # Populating the database
        store = SPARQLUpdateStore()
        store.open((self.getEndpointUrl(), self.getEndpointUrl()))

        for triple in graph.triples((None, None, None)):
            store.add(triple)

        store.close()



class TriplestoreQueryProcessor(TriplestoreProcessor):
    pass

    # Returns a data frame with all the publications that have been published in the input year
    def getPublicationsPublishedInYear(self, year):
        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?id ?publicationYear ?publicationVenue ?title ?issue ?volume ?orcid ?referencePublicationId
        WHERE {
            VALUES ?type {
                schema:ScholarlyArticle
                schema:Chapter
                schema:CreativeWork
            }

            ?internalId rdf:type ?type .
            ?internalId schema:identifier ?id .
            ?internalId schema:datePublished ?publicationYear .
            ?internalId schema:name ?title .
            ?internalId schema:isPartOf ?venue .
            ?venue schema:name ?publicationVenue .
            ?internalId schema:author ?author .
            ?author schema:identifier ?orcid .
            ?internalId schema:citation ?referencePublicationId .

            OPTIONAL {
                ?internalId schema:issueNumber ?issue .
                ?internalId schema:volumeNumber ?volume .
            }
          	Filter (?publicationYear = '""" + str(year) + """')
        }
        """
        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql

    # Returns a data frame with all the publications that have been authored by the person having the identifier specified as input
    def getPublicationsByAuthorId(self, id):
        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?id ?publicationYear ?title ?issue ?volume
        WHERE {
          VALUES ?type {
                schema:ScholarlyArticle
                schema:Chapter
                schema:CreativeWork
          }

          ?internalId rdf:type ?type .
          ?internalId schema:identifier ?id .
          ?internalId schema:datePublished ?publicationYear .
          ?internalId schema:name ?title .
          ?internalId schema:isPartOf ?venue .
          ?venue schema:name ?publicationVenue .
          ?internalId schema:author ?author .
          ?author schema:identifier ?orcid .

          OPTIONAL {
            ?internalId schema:issueNumber ?issue .
            ?internalId schema:volumeNumber ?volume .
          }
          Filter (?orcid = '""" + str(id) + """')
        }
        """
        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql

    # Returns a data frame with all the publications that have received the most number of citations by other publications
    def getMostCitedPublication(self):
        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?publicationYear ?publicationVenue ?title ?issue ?volume (COUNT (?citation) AS ?CitationsCount)
        WHERE {
          VALUES ?type {
            schema:ScholarlyArticle
            schema:Chapter
            schema:CreativeWork
          }

          ?internalId rdf:type ?type .
          ?internalId schema:identifier ?id .
          ?internalId schema:datePublished ?publicationYear .
          ?internalId schema:name ?title .
          ?internalId schema:isPartOf ?venue .
          ?venue schema:name ?publicationVenue .
          ?internalId ^schema:citation ?citation .
          OPTIONAL {
            ?internalId schema:issueNumber ?issue .
            ?internalId schema:volumeNumber ?volume .
          }
        }
        GROUP BY ?id ?publicationYear ?publicationVenue ?title ?issue ?volume
        ORDER BY DESC(?CitationsCount)
        """
        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql

    # Returns a data frame with all the venues containing the publications that, overall, have received the most number of citations by other publications
    def getMostCitedVenue(self):
        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?title (COUNT (?citation) AS ?CitationsCount)
        WHERE {
          VALUES ?type {
            schema:ScholarlyArticle
            schema:Chapter
            schema:CreativeWork
          }

          ?internalId rdf:type ?type .
          ?internalId schema:isPartOf ?venue .
          ?venue schema:name ?title .
          ?internalId ^schema:citation ?citation .
        }
        GROUP BY ?id ?title
        ORDER BY DESC(?CitationsCount)
        """
        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql

    # Returns a data frame with all the venues that have been published by the organisation having the identifier specified as input
    def getVenuesByPublisherId(self, id):
        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?title
        WHERE {
          VALUES ?type {
            schema:Periodical
            schema:Book
            schema:EducationEvent
          }

          ?internalId rdf:type ?type .
          ?internalId schema:name ?title .
          ?internalId schema:publisher ?publisher .
          ?publisher schema:identifier ?publisherId .

          Filter (?publisherId = '""" + str(id) + """')
        }
        """
        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql

    # Returns a data frame with all the publications that have been included in the venue having the identifier specified as input
    def getPublicationInVenue(self, venueId):
        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?id ?publicationYear ?publicationVenue ?title ?issue ?volume
        WHERE {
        VALUES ?type {
            schema:ScholarlyArticle
            schema:Chapter
            schema:CreativeWork
        }

        ?internalId rdf:type ?type .
        ?internalId schema:identifier ?id .
        ?internalId schema:datePublished ?publicationYear .
        ?internalId schema:name ?title .
        ?internalId schema:isPartOf ?venue .
        ?venue schema:name ?publicationVenue .
        ?internalId schema:author ?author .
        ?internalId schema:issn ?issn .
        
        OPTIONAL {
            ?internalId schema:issueNumber ?issue .
            ?internalId schema:volumeNumber ?volume .
        }
        Filter (?issn = '""" + str(venueId) + """')
        }
        """
        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql

    # Returns a data frame with all the journal articles that have been included in the input issue of the input volume of the journal having the identifier specified as input
    def getJournalArticlesInIssue(self, issue, volume, journal_id):
        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?id ?publicationYear ?publicationVenue ?title ?issue ?volume
        WHERE {
          VALUES ?type {
            schema:ScholarlyArticle
          }

          ?internalId rdf:type ?type .
          ?internalId schema:identifier ?id .
          ?internalId schema:datePublished ?publicationYear .
          ?internalId schema:name ?title .
          ?internalId schema:isPartOf ?venue .
          ?venue schema:name ?publicationVenue .
          ?internalId schema:issueNumber ?issue .
          ?internalId schema:volumeNumber ?volume .
          ?internalId schema:issn ?issn .

          Filter (?issue = '""" + str(issue) + """')
          Filter (?volume = '""" + str(volume) + """')
          Filter (?issn = '""" + str(journal_id) + """')
        }
        """
        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql

    # Returns a data frame with all the journal articles that have been included, independently from the issue, in input volume of the journal having the identifier specified as input
    def getJournalArticlesInVolume(self, volume, journal_id):
        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?id ?publicationYear ?publicationVenue ?title ?issue ?volume
        WHERE {
          VALUES ?type {
            schema:ScholarlyArticle
          }

          ?internalId rdf:type ?type .
          ?internalId schema:identifier ?id .
          ?internalId schema:datePublished ?publicationYear .
          ?internalId schema:name ?title .
          ?internalId schema:isPartOf ?venue .
          ?venue schema:name ?publicationVenue .
          ?internalId schema:volumeNumber ?volume .
          ?internalId schema:issn ?issn .

          OPTIONAL {
            ?internalId schema:issueNumber ?issue .            
          }
          Filter (?volume = '""" + str(volume) + """')
          Filter (?issn = '""" + str(journal_id) + """')
        }
        """
        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql

    # Returns a data frame with all the journal articles that have been included, independently from the issue and the volume, in the journal having the identifier specified as input
    def getJournalArticlesInJournal(self, journal_id):
        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?id ?publicationYear ?publicationVenue ?title ?issue ?volume
        WHERE {
          VALUES ?type {
            schema:ScholarlyArticle
          }

          ?internalId rdf:type ?type .
          ?internalId schema:identifier ?id .
          ?internalId schema:datePublished ?publicationYear .
          ?internalId schema:name ?title .
          ?internalId schema:isPartOf ?venue .
          ?venue schema:name ?publicationVenue .
          ?internalId schema:issn ?issn .

          OPTIONAL {
            ?internalId schema:issueNumber ?issue .
            ?internalId schema:volumeNumber ?volume .
          }
          Filter (?issn = '""" + str(journal_id) + """')
        }
        """
        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql


    # Returns a data frame with all the proceedings that refer to the events that match (in lowercase), even partially, with the name specified as input 
    def getProceedingsByEvent(self, event_name):
        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?event ?title ?issn
        WHERE {
          VALUES ?type {
            schema:EducationEvent
          }

          ?internalId rdf:type ?type .
          ?internalId schema:name ?title .
          ?internalId schema:event ?event .
          ?internalId schema:issn ?issn .

          Filter regex(?event, '^""" + str(event_name) + """$', 'i')
        }
        """
        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql

    # Returns a data frame with all the authors of the publication with the identifier specified as input 
    def getPublicationAuthors(self, publicationId): 
        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?orcid ?family ?given
        WHERE {
          VALUES ?type {
                schema:ScholarlyArticle
                schema:Chapter
                schema:CreativeWork
          }

          ?internalId rdf:type ?type .
          ?internalId schema:identifier ?publicationId .
          ?internalId schema:author ?author .
          ?author schema:identifier ?orcid .
          ?author schema:familyName ?family .
          ?author schema:givenName ?given .

          Filter (?publicationId = '""" + str(publicationId) + """')
        }
        """
        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql

    # Returns a data frame with all the publications that have been authored by the people having their name matching (in lowercase), even partially, with the name specified as input 
    def getPublicationsByAuthorName(self, name):
        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?id ?publicationYear ?publicationVenue ?title ?issue ?volume
        WHERE {
          VALUES ?type {
                schema:ScholarlyArticle
                schema:Chapter
                schema:CreativeWork
          }

          ?internalId rdf:type ?type .
          ?internalId schema:identifier ?id .
          ?internalId schema:datePublished ?publicationYear .
          ?internalId schema:name ?title .
          ?internalId schema:isPartOf ?venue .
          ?venue schema:name ?publicationVenue .
          ?internalId schema:author ?author .
          ?author schema:familyName ?family .
          ?author schema:givenName ?given .

          OPTIONAL {
            ?internalId schema:issueNumber ?issue .
            ?internalId schema:volumeNumber ?volume .
          }
          Filter (regex(?family, '.*""" + str(name) + """.*', 'i') || regex(?given, '.*""" + str(name) + """.*', 'i'))
        }
        """
        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql

    # Returns a data frame with all the distinct publishers that have published the venues of the publications with identifiers those specified as input
    def getDistinctPublisherOfPublications(self, publication_id_list):
        publication_id_params = ""
        for publication_id in publication_id_list:
            if (len(publication_id_params) > 1):
                publication_id_params += "|"
            publication_id_params += str(publication_id)
        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?id ?name
        WHERE {
          VALUES ?type {
            schema:ScholarlyArticle
            schema:Chapter
            schema:CreativeWork
          }

          ?internalId rdf:type ?type .
          ?internalId schema:identifier ?publicationId .
          ?internalId schema:isPartOf ?venue .
          ?venue schema:publisher ?publisher .
          ?publisher schema:identifier ?id .
          ?publisher schema:name ?name .
          Filter regex(?publicationId, '^(""" + publication_id_params + """)$', 'i')
        }
        """
        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql

    # Additional methods

    # Returns a dataframe with all the publications that have been referenced in the publication with the publication_id specified as input
    def getPublicationsCitations(self, publication_id):
        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?id ?publicationYear ?publicationVenue ?title ?issue ?volume ?orcid ?referencePublicationId
        WHERE {
            VALUES ?type {
                schema:ScholarlyArticle
                schema:Chapter
                schema:CreativeWork
            }

            ?internalId rdf:type ?type .
            ?internalId schema:identifier ?id .
            ?internalId schema:datePublished ?publicationYear .
            ?internalId schema:name ?title .
            ?internalId schema:isPartOf ?venue .
            ?venue schema:name ?publicationVenue .
            ?internalId schema:author ?author .
            ?author schema:identifier ?orcid .
            ?internalId ^schema:citation ?citation .
            ?citation schema:identifier ?citationId .

            OPTIONAL {
                ?internalId schema:issueNumber ?issue .
                ?internalId schema:volumeNumber ?volume .
            }
            Filter (?citationId = '""" + str(publication_id) + """')
        }
        """
        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql

    # Returns a dataframe with all venue ids associated with a venue having title specified as input
    def getVenuesIdByTitle(self, venue_title):
        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?venueId
        WHERE {
          VALUES ?type {
            schema:ScholarlyArticle
            schema:Chapter
            schema:CreativeWork
          }

          ?internalId rdf:type ?type .
          ?internalId schema:issn ?venueId .
          ?internalId schema:isPartOf ?venue .
          ?venue schema:name ?publicationVenue .

          Filter (?publication_venue = '""" + str(venue_title) + """')
        }
        """
        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql

    # Returns a dataframe with all the publishers that published a venue having title specified as input
    def getPublisherByVenueTitle(self, venue_title):
        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?id ?name
        WHERE {
          VALUES ?type {
            schema:Periodical
            schema:Book
            schema:EducationEvent
          }

          ?internalId rdf:type ?type .
          ?internalId schema:name ?title .
          ?internalId schema:publisher ?publisher .
          ?publisher schema:identifier ?id .
          ?publisher schema:name ?name .

          Filter (?title = '""" + str(venue_title) + """')
        }
        """
        df_sparql = get(self.getEndpointUrl(), query, True)
        return df_sparql
