#ifndef SRC_SHA256IDGENERATOR_HPP_
#define SRC_SHA256IDGENERATOR_HPP_

#include "immutable/idGenerator.hpp"
#include "immutable/pageId.hpp"

class Sha256IdGenerator : public IdGenerator {
public:
    virtual PageId generateId(std::string const& content) const /*override*/
    {
        return PageId(getCommandOutput("printf \"" + content + "\" | sha256sum"));
    }

private:
    #define BUFFER_SIZE 128

    std::string getCommandOutput(std::string const& command) const {
        char buffer[BUFFER_SIZE];
        std::string result = "";

        FILE *file = popen(command.c_str(), "r");
        while (!feof(file)) {
            if (fgets(buffer, BUFFER_SIZE, file) != NULL) {
                result += buffer;
            }
        }
        pclose(file);

        return result.substr(0, result.length() - 4);
    }
};

#endif /* SRC_SHA256IDGENERATOR_HPP_ */
