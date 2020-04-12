from collections import deque
from bs4 import BeautifulSoup
import urllib.request
import urllib.robotparser
import urllib.parse
import validators


class SequentialWebCrawler:
    def __init__(self, inputUrls, userAgent, visitLimit):
        self.__inputUrls = inputUrls
        self.__userAgent = userAgent
        self.__visitLimit = visitLimit

    def crawl(self):
        visited = {}
        queue = deque()
        exceptions = []
        for inUrl in self.__inputUrls:
            cleanInUrl = self.__getCleanUrl(inUrl)
            if cleanInUrl != None:
                queue.append(cleanInUrl)
        while len(queue) > 0 and len(visited) < self.__visitLimit:
            elem = queue.popleft()
            if visited.get(elem) != None:
                continue
            visited[elem] = {}
            try:
                robotParser = urllib.robotparser.RobotFileParser()
                robotParser.set_url(urllib.request.urljoin(elem, 'robots.txt'))
                robotParser.read()
                if robotParser.can_fetch(self.__userAgent, elem) == False:
                    continue
                htmlPage = urllib.request.urlopen(elem)
                soup = BeautifulSoup(htmlPage, 'html.parser')
                for link in soup.find_all('a', href=True):
                    url = link.get('href')
                    cleanUrl = self.__getCleanUrl(url)
                    if cleanUrl != None:
                        queue.append(cleanUrl)
                        visited[elem][cleanUrl] = True
            except Exception as e:
                exceptions.append([elem, e])
        return [visited, exceptions]

    def __getCleanUrl(self, url):
        parsed = urllib.parse.urlparse(url)
        cleanUrl = parsed.scheme + '://' + parsed.netloc + (parsed.path if parsed.path != '' else '/') + (('?' + parsed.query) if parsed.query != '' else '')
        if validators.url(cleanUrl) == True:
            return cleanUrl
        return None

def demo():
    urls = ['https://www.youtube.com/watch?v=TQMbvJNRpLE']
    crawler = SequentialWebCrawler(urls, 'researchBot', 2)
    [graph, exceptions] = crawler.crawl()
    for node in graph:
        print(node, graph[node])
    print('\n\nexceptions: ', exceptions)
