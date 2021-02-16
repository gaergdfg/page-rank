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
        std::unordered_set<PageId, PageIdHash> danglingNodes;
    	std::unordered_map<PageId, std::vector<PageId>, PageIdHash> edges;

        // TODO: are the mutexes necessary since we dont modify index's value twice?
        std::mutex pageHashMapMutex;
        std::mutex numLinksMutex;
        std::mutex danglingNodesMutex;
        std::mutex edgesMutex;

        auto preprocessWorker = [&](size_t start, size_t end) {
            auto pages = network.getPages();
            for (size_t i = start; i < end; i++) {
                pages[i].generateId(network.getGenerator());
                {
                    std::lock_guard<std::mutex> lock(pageHashMapMutex);
                    pageHashMap[pages[i].getId()] = 1.0 / network.getSize();
                }
            }

            for (size_t i = start; i < end; i++) {
                {
                    std::lock_guard<std::mutex> lock(numLinksMutex);
                    numLinks[pages[i].getId()] = pages[i].getLinks().size();
                }
            }

            for (size_t i = start; i < end; i++) {
                if (pages[i].getLinks().size() == 0) {
                    {
                        std::lock_guard<std::mutex> lock(danglingNodesMutex);
                        danglingNodes.insert(pages[i].getId());
                    }
                }
            }

            for (size_t i = start; i < end; i++) {
                for (auto link : pages[i].getLinks()) {
                    {
                        std::lock_guard<std::mutex> lock(edgesMutex);
                        edges[link].push_back(pages[i].getId());
                    }
                }
            }
        };

        std::thread threads[numThreads];
        size_t last_index = 0;
        for (uint32_t i = 0; i < numThreads; i++) {
            threads[i] = std::thread{
                preprocessWorker,
                last_index,
                last_index + network.getSize() / numThreads + (i < network.getSize() % numThreads ? 1 : 0)
            };

            last_index += network.getSize() / numThreads + (i < network.getSize() % numThreads ? 1 : 0);
        }
        for (uint32_t i = 0; i < numThreads; i++) {
            threads[i].join();
        }

        std::unordered_map<PageId, PageRank, PageIdHash> previousPageHashMap;
        std::atomic<double> dangleSum;
        std::atomic<double> difference;

        auto dangleSumWorker = [&](size_t start, size_t end) {
            auto pages = network.getPages();

            for (size_t i = start; i < end; i++) {
                if (pages[i].getLinks().size() == 0) {
                    for (
                        double currDangleSum = dangleSum;
                        !dangleSum.compare_exchange_weak(currDangleSum, currDangleSum + pageHashMap[pages[i].getId()]);
                    );
                }
            }
        };

        auto pageRankWorker = [&](size_t start, size_t end) {
            auto pages = network.getPages();

            for (size_t i = start; i < end; i++) {
                PageId pageId = pages[i].getId();

                double danglingWeight = 1.0 / network.getSize();
                pageHashMap[pages[i].getId()] = dangleSum * danglingWeight + (1.0 - alpha) / network.getSize();

                if (edges.count(pageId) > 0) {
                    for (auto link : edges[pageId]) {
                        pageHashMap[pages[i].getId()] += alpha * previousPageHashMap[link] / numLinks[link];
                    }
                }
                for (
                    double currDifference = difference;
                    !dangleSum.compare_exchange_weak(currDifference, std::abs(previousPageHashMap[pageId] - pageHashMap[pageId]));
                );
            }
        };

        for (uint32_t i = 0; i < iterations; i++) {
            previousPageHashMap = pageHashMap;
            dangleSum = 0;
            difference = 0;

            last_index = 0;
            for (uint32_t t = 0; t < numThreads; t++) {
                threads[t] = std::thread{
                    dangleSumWorker,
                    last_index,
                    last_index + network.getSize() / numThreads + (t < network.getSize() % numThreads ? 1 : 0)
                };

                last_index += network.getSize() / numThreads + (t < network.getSize() % numThreads ? 1 : 0);
            }
            for (uint32_t t = 0; t < numThreads; t++) {
                threads[t].join();
            }

            last_index = 0;
            for (uint32_t t = 0; t < numThreads; t++) {
                threads[t] = std::thread{
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
