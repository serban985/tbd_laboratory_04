import shutil
import os
from pathlib import Path
from ThreadPool import ThreadPool

class BackLinksMapReduce:
    def __getStringIndexDict(self, adjacencyLists):
        dict = {}
        idx = 0
        for node in adjacencyLists:
            if dict.get(node) == None:
                dict[node] = idx
                idx += 1
            for link in adjacencyLists[node]:
                if dict.get(link) == None:
                    dict[link] = idx
                    idx += 1
        return dict

    def __getBackLinksFromDirStruct(self, workDir):
        backLinksGraphByIndex = {}
        workdirItems = os.listdir(workDir)
        for dest in workdirItems:
            destDir = workDir + '/' + dest
            if os.path.isdir(destDir):
                backLinksGraphByIndex[dest] = {}
                for src in os.listdir(destDir):
                    backLinksGraphByIndex[dest][src] = True
        return backLinksGraphByIndex

    def __getBackLinksGraph(self, backLinksGraphByIndex, linksIndexes):
        linksArr = [None] * len(linksIndexes)
        for key, val in linksIndexes.items():
            linksArr[val] = key
        graph = {}
        for dest in backLinksGraphByIndex:
            destIdx = linksArr[int(dest)]
            graph[destIdx] = {}
            for src in backLinksGraphByIndex[dest]:
                srcIdx = linksArr[int(src)]
                graph[destIdx][srcIdx] = True
        return graph

    def __map(self, argsDict):
        f = open(argsDict['dir'] + '/' + str(argsDict['destLink']) + '_' + str(argsDict['srcLink']), 'w')
        f.write('_')
        f.close()

    def __reduce(self, argsDict):
        [dest, src] = argsDict['pair'].split('_')
        Path(argsDict['dir'] + '/' + dest).mkdir(parents=True, exist_ok=True)
        f = open(argsDict['dir'] + '/' + dest + '/' + src, 'w')
        f.write('_')
        f.close()

    def __mapStage(self, linksGraph, linksIndexes, tp, workDir):
        shutil.rmtree(workDir, ignore_errors=True)
        Path(workDir).mkdir(parents=True)
        for src in linksGraph:
            for dest in linksGraph[src]:
                tp.startWorker(self.__map, {'destLink': linksIndexes[dest], 'srcLink': linksIndexes[src], 'dir': workDir})
        tp.joinAll()

    def __reduceStage(self, tp, workDir):
        pairs = os.listdir(workDir)
        for pair in pairs:
            tp.startWorker(self.__reduce, {'pair': pair, 'dir': workDir})
        tp.joinAll()
        
    def getBackLinks(self, workDir, linksGraph):
        tp = ThreadPool(3)
        linksIndexes = self.__getStringIndexDict(linksGraph)
        self.__mapStage(linksGraph, linksIndexes, tp, workDir)
        self.__reduceStage(tp, workDir)
        backLinksIndexesGraph = self.__getBackLinksFromDirStruct(workDir)
        backLinksGraph = self.__getBackLinksGraph(backLinksIndexesGraph, linksIndexes)
        return backLinksGraph

def demo():
    linksGraph = {
        "B": {"C": True, "D": True},
        "A": {"B": True, "C": True},
        "C": {"D": True},
        "D": {},
        "E": {"A": True},
        "F": {"C": True, "E": True}
    }
    blmp = BackLinksMapReduce()
    graph = blmp.getBackLinks('work', linksGraph)
    for dest in graph:
        print(dest, graph[dest])