#Defining classes of DataModel 
class IdentifiableEntity(object):
    def __init__(self, identifiers):
        self.id = set()
        for identifier in identifiers:
          self.id.add(identifier)

    def getIds(self):
        result = []
        for identifier in self.id:
            result.append(identifier)
        return result

class Person(IdentifiableEntity):
    def __init__(self, identifiers, givenName, familyName):
        super().__init__(identifiers)

        self.givenName = givenName
        self.familyName = familyName

    def getGivenName(self):
        return self.givenName

    def getFamilyName(self):
        return self.familyName

class Publication(IdentifiableEntity):
    def __init__(self, identifiers, publicationYear, title, authors, publicationVenue, citedPublications):
        super().__init__(identifiers)

        self.publicationYear = publicationYear
        self.title = title
        self.publicationVenue = publicationVenue

        self.author = set()
        for a in authors:
            self.author.add(a)

        self.citedPublication = []
        for p in citedPublications:
            self.citedPublication.append(p)

    def getPublicationYear(self):
        if type(self.publicationYear) == int:
            return self.publicationYear
        else:
            return None

    def getTitle(self):
        return self.title

    def getPublicationVenue(self):
        return self.publicationVenue

    def getAuthors(self):
        result = set()
        for a in self.author:
            result.add(a)
        return result

    def getCitedPublications(self):
        result = []
        for p in self.citedPublication:
            result.append(p)
        return result

class JournalArticle(Publication):
    def __init__(self, identifiers, publicationYear, title, authors, publicationVenue, citedPublications, issue, volume):
        super().__init__(identifiers, publicationYear, title, authors, publicationVenue, citedPublications)

        self.issue = issue
        self.volume = volume

    def getIssue(self):
        if type(self.issue) == str:
            return self.issue
        else:
            return None

    def getVolume(self):
        if type(self.volume) == str:
            return self.volume
        else:
            return None

class BookChapter(Publication):
    def __init__(self, identifiers, publicationYear, title, authors, publicationVenue, citedPublications, chapterNumber):
        super().__init__(identifiers, publicationYear, title, authors, publicationVenue, citedPublications)

        self.chapterNumber = chapterNumber

    def getchapterNumber(self):
        return self.chapterNumber

class ProceedingsPapers(Publication):
    pass

class Venue(IdentifiableEntity):
    def __init__(self, identifiers, title, publisher):
        super().__init__(identifiers)

        self.title = title
        self.publisher = publisher

    def getTitle(self):
        return self.title

    def getPublisher(self):
        return self.publisher

class Proceedings(Venue):
    def __init__(self, identifiers, event, title, publisher):
        super().__init__(identifiers, title, publisher)

        self.event = event

    def getEvent(self):
        return self.event

class Journal(Venue):
    pass

class Book (Venue):
    pass

class Organization(IdentifiableEntity):
    def __init__(self, identifiers, name):
        super().__init__(identifiers)

        self.name = name

    def getName(self):
        return self.name





