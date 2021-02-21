#ifndef SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_

#include <atomic>
#include <mutex>
#include <thread>

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "immutable/network.hpp"
#include "immutable/pageIdAndRank.hpp"
#include "immutable/pageRankComputer.hpp"

class MultiThreadedPageRankComputer : public PageRankComputer {
public:
    MultiThreadedPageRankComputer(uint32_t numThreadsArg)
        : numThreads(numThreadsArg) {};

    std::vector<PageIdAndRank> computeForNetwork(Network const& network, double alpha, uint32_t iterations, double tolerance) const
    {
        std::unordered_map<PageId, PageRank, PageIdHash> pageHashMap;
        std::unordered_map<PageId, uint32_t, PageIdHash> numLinks;
        std::vector<PageId> danglingNodes;
    	std::unordered_map<PageId, std::vector<PageId>, PageIdHash> edges;

        auto& pages = network.getPages();
        auto idGeneratorWorker = [
            &network, &pages
        ](size_t start, size_t end) {
            for (size_t i = start; i < end; i++) {
                pages[i].generateId(network.getGenerator());
            }
        };

        std::thread threads[numThreads];
        size_t last_index = 0;
        for (uint32_t i = 0; i < numThreads; i++) {
            threads[i] = std::thread {
                idGeneratorWorker,
                last_index,
                last_index + network.getSize() / numThreads + (i < network.getSize() % numThreads ? 1 : 0)
            };

            last_index += network.getSize() / numThreads + (i < network.getSize() % numThreads ? 1 : 0);
        }
        for (uint32_t i = 0; i < numThreads; i++) {
            threads[i].join();
        }

        for (auto const& page : network.getPages()) {
            pageHashMap[page.getId()] = 1.0 / network.getSize();
        }

        for (auto page : network.getPages()) {
            numLinks[page.getId()] = page.getLinks().size();
        }

        for (auto page : network.getPages()) {
            if (page.getLinks().size() == 0) {
                danglingNodes.push_back(page.getId());
            }
        }

        for (auto page : network.getPages()) {
            for (auto link : page.getLinks()) {
                edges[link].push_back(page.getId());
            }
        }

        std::unordered_map<PageId, PageRank, PageIdHash> previousPageHashMap;
        double dangleSum;
        double difference;

        std::mutex dangleSumMutex;
        std::mutex differenceMutex;

        auto dangleSumWorker = [&dangleSum, &dangleSumMutex, &pageHashMap, &danglingNodes](size_t start, size_t end) {
            double localDangleSum = 0.0;
            for (size_t i = start; i < end; i++) {
                localDangleSum += pageHashMap[danglingNodes[i]];
            }
            {
                std::lock_guard<std::mutex> lock(dangleSumMutex);
                dangleSum += localDangleSum;
            }
        };

        auto pageRankWorker = [
            &network, &pages, &alpha, &dangleSum, &difference, &differenceMutex,
            &pageHashMap, &previousPageHashMap, &edges, &numLinks
        ](size_t start, size_t end) {
            double localDifference = 0.0;
            for (size_t i = start; i < end; i++) {
                PageId pageId = pages[i].getId();

                double danglingWeight = 1.0 / network.getSize();
                pageHashMap[pageId] = dangleSum * danglingWeight + (1.0 - alpha) / network.getSize();

                if (edges.count(pageId) > 0) {
                    for (auto link : edges[pageId]) {
                        pageHashMap[pageId] += alpha * previousPageHashMap[link] / numLinks[link];
                    }
                }

                localDifference += std::abs(previousPageHashMap[pages[i].getId()] - pageHashMap[pages[i].getId()]);
            }
            {
                std::lock_guard<std::mutex> lock(differenceMutex);
                difference += localDifference;
            }
        };

        for (uint32_t i = 0; i < iterations; i++) {
            previousPageHashMap = pageHashMap;
            dangleSum = 0;
            difference = 0;

            last_index = 0;
            for (uint32_t t = 0; t < numThreads; t++) {
                threads[t] = std::thread {
                    dangleSumWorker,
                    last_index,
                    last_index + danglingNodes.size() / numThreads + (t < danglingNodes.size() % numThreads ? 1 : 0)
                };

                last_index += danglingNodes.size() / numThreads + (t < danglingNodes.size() % numThreads ? 1 : 0);
            }
            for (uint32_t t = 0; t < numThreads; t++) {
                threads[t].join();
            }
            dangleSum *= alpha;

            last_index = 0;
            for (uint32_t t = 0; t < numThreads; t++) {
                threads[t] = std::thread {
                    pageRankWorker,
                    last_index,
                    last_index + network.getSize() / numThreads + (t < network.getSize() % numThreads ? 1 : 0)
                };

                last_index += network.getSize() / numThreads + (t < network.getSize() % numThreads ? 1 : 0);
            }
            for (uint32_t t = 0; t < numThreads; t++) {
                threads[t].join();
            }

            if (difference < tolerance) {
                std::vector<PageIdAndRank> result;
                for (auto iter : pageHashMap) {
                    result.push_back(PageIdAndRank(iter.first, iter.second));
                }

                ASSERT(result.size() == network.getSize(), "Invalid result size=" << result.size() << ", for network" << network);

                return result;
            }
        }

        ASSERT(false, "Not able to find result in iterations=" << iterations);
    }

    std::string getName() const
    {
        return "MultiThreadedPageRankComputer[" + std::to_string(this->numThreads) + "]";
    }

private:
    uint32_t numThreads;
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
