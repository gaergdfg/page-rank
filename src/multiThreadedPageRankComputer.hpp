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
private:
    uint32_t numThreads;

    // preprocessWorker data
    static std::unordered_map<PageId, PageRank, PageIdHash> pageHashMap;
    static std::unordered_map<PageId, uint32_t, PageIdHash> numLinks;
    static std::vector<PageId> danglingNodes;
    static std::unordered_map<PageId, std::vector<PageId>, PageIdHash> edges;

    static std::mutex pageHashMapMutex;
    static std::mutex numLinksMutex;
    static std::mutex danglingNodesMutex;
    static std::mutex edgesMutex;

    // dangleSumWorker/pageRankWorker data
    // std::unordered_map<PageId, PageRank, PageIdHash> previousPageHashMap;
    // double dangleSum;
    // double difference;

    static std::mutex previousPageHashMapMutex;
    static std::mutex dangleSumMutex;
    static std::mutex differenceMutex;

    static void preprocessWorker(Network const& network, /*std::vector<Page>& pages,*/ size_t start, size_t end) {
        double res;
        auto& pages = network.getPages();
        for (size_t i = start; i < end; i++) {
            pages[i].generateId(network.getGenerator());
            res = 1.0 / network.getSize();
            {
                std::lock_guard<std::mutex> lock(pageHashMapMutex);
                pageHashMap[pages[i].getId()] = res;
            }
        }

        uint32_t res2;
        for (size_t i = start; i < end; i++) {
            res2 = pages[i].getLinks().size();
            {
                std::lock_guard<std::mutex> lock(numLinksMutex);
                numLinks[pages[i].getId()] = res2;
            }
        }

        PageId res3{""};
        for (size_t i = start; i < end; i++) {
            if (pages[i].getLinks().size() == 0) {
                res3 = pages[i].getId();
                {
                    std::lock_guard<std::mutex> lock(danglingNodesMutex);
                    danglingNodes.push_back(res3);
                }
            }
        }

        for (size_t i = start; i < end; i++) {
            for (auto link : pages[i].getLinks()) {
                res3 = pages[i].getId();
                {
                    std::lock_guard<std::mutex> lock(edgesMutex);
                    edges[link].push_back(res3);
                }
            }
        }
    }

    // static void dangleSumWorker(double& dangleSum, size_t start, size_t end) {
    //     for (size_t i = start; i < end; i++) {
    //         {
    //             std::lock_guard<std::mutex> lock(dangleSumMutex);
    //             dangleSum += pageHashMap[danglingNodes[i]];
    //         }
    //     }
    // }

    // static void pageRankWorker(
    //     Network const& network, std::vector<Page>& pages, double alpha,
    //     std::unordered_map<PageId, PageRank, PageIdHash>& previousPageHashMap,
    //     double& dangleSum, double& difference,
    //     size_t start, size_t end
    // ) {
    //     for (size_t i = start; i < end; i++) {
    //         PageId pageId = pages[i].getId();

    //         double danglingWeight = 1.0 / network.getSize();
    //         pageHashMap[pageId] = dangleSum * danglingWeight + (1.0 - alpha) / network.getSize();

    //         if (edges.count(pageId) > 0) {
    //             for (auto link : edges[pageId]) {
    //                 pageHashMap[pageId] += alpha * previousPageHashMap[link] / numLinks[link];
    //             }
    //         }
    //         {
    //             std::lock_guard<std::mutex> lock(differenceMutex);
    //             difference += std::abs(previousPageHashMap[pageId] - pageHashMap[pageId]);
    //         }
    //     }
    // }

    // static void pageHashMapCopyWorker(
    //     std::vector<Page>& pages,
    //     std::unordered_map<PageId, PageRank, PageIdHash>& previousPageHashMap,
    //     size_t start, size_t end
    // ) {
    //     double res;
    //     for (size_t i = start; i < end; i++) {
    //         res = pageHashMap[pages[i].getId()];
    //         {
    //             std::lock_guard<std::mutex> lock(previousPageHashMapMutex);
    //             previousPageHashMap[pages[i].getId()] = res;
    //         }
    //     }
    // }

public:
    MultiThreadedPageRankComputer(uint32_t numThreadsArg)
        : numThreads(numThreadsArg) {};

    std::vector<PageIdAndRank> computeForNetwork(Network const& network, double alpha, uint32_t iterations, double tolerance) const
    {
        // auto& pages = network.getPages();

        std::thread threads[numThreads];
        size_t last_index = 0;
        for (uint32_t i = 0; i < numThreads; i++) {
            threads[i] = std::thread {
                preprocessWorker,
                std::ref(network),
                // std::ref(pages),
                last_index,
                last_index + network.getSize() / numThreads + (i < network.getSize() % numThreads)
            };

            last_index += network.getSize() / numThreads + (i < network.getSize() % numThreads);
        }
        for (uint32_t i = 0; i < numThreads; i++) {
            threads[i].join();
        }

        alpha += 1.;
        iterations++;
        tolerance += 1.;

        std::vector<PageIdAndRank> res;
        return res;

        // std::unordered_map<PageId, PageRank, PageIdHash> previousPageHashMap = pageHashMap;
        // for (uint32_t i = 0; i < iterations; i++) {
        //     double dangleSum = 0;
        //     double difference = 0;

        //     last_index = 0;
        //     for (uint32_t t = 0; t < numThreads; t++) {
        //         threads[t] = std::thread {
        //             MultiThreadedPageRankComputer::dangleSumWorker,
        //             std::ref(dangleSum),
        //             last_index,
        //             last_index + danglingNodes.size() / numThreads + (t < danglingNodes.size() % numThreads ? 1 : 0)
        //         };

        //         last_index += danglingNodes.size() / numThreads + (t < danglingNodes.size() % numThreads ? 1 : 0);
        //     }
        //     for (uint32_t t = 0; t < numThreads; t++) {
        //         threads[t].join();
        //     }
        //     dangleSum *= alpha;

        //     last_index = 0;
        //     for (uint32_t t = 0; t < numThreads; t++) {
        //         threads[t] = std::thread {
        //             MultiThreadedPageRankComputer::pageRankWorker,
        //             std::ref(network), std::ref(pages), std::ref(alpha),
        //             std::ref(previousPageHashMap),
        //             std::ref(dangleSum), std::ref(difference),
        //             last_index,
        //             last_index + network.getSize() / numThreads + (t < network.getSize() % numThreads ? 1 : 0)
        //         };

        //         last_index += network.getSize() / numThreads + (t < network.getSize() % numThreads ? 1 : 0);
        //     }
        //     for (uint32_t t = 0; t < numThreads; t++) {
        //         threads[t].join();
        //     }

        //     if (difference < tolerance) {
        //         std::vector<PageIdAndRank> result;
        //         for (auto iter : pageHashMap) {
        //             result.push_back(PageIdAndRank(iter.first, iter.second));
        //         }

        //         ASSERT(result.size() == network.getSize(), "Invalid result size=" << result.size() << ", for network" << network);

        //         return result;  
        //     }

        //     last_index = 0;
        //     for (uint32_t t = 0; t < numThreads; t++) {
        //         threads[t] = std::thread {
        //             MultiThreadedPageRankComputer::pageHashMapCopyWorker,
        //             std::ref(pages),
        //             std::ref(previousPageHashMap),
        //             last_index,
        //             last_index + network.getSize() / numThreads + (t < network.getSize() % numThreads ? 1 : 0)
        //         };

        //         last_index += network.getSize() / numThreads + (t < network.getSize() % numThreads ? 1 : 0);
        //     }
        //     for (uint32_t t = 0; t < numThreads; t++) {
        //         threads[t].join();
        //     }
        // }

        // ASSERT(false, "Not able to find result in iterations=" << iterations);
    }

    std::string getName() const
    {
        return "MultiThreadedPageRankComputer[" + std::to_string(this->numThreads) + "]";
    }
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
