import BackLinksMapReduce as blmr
import SequentialWebCrawler as swc

a = swc.SequentialWebCrawler(['http://www.youtube.com/'], 'researchBot', 3)
b = blmr.BackLinksMapReduce()

[graph, exceptions] = a.crawl()

if len(exceptions) == 0:
    backLinks = b.getBackLinks('workDir', graph)
    print(backLinks)
    for idx in backLinks:
        print(idx, backLinks[idx])
else:
    print('Exceptions occured!')
    print(exceptions)