#pragma once

#include <boost/filesystem.hpp>
#include <string>

namespace bfs = boost::filesystem;

class BulkLoader {

public:
    BulkLoader(bfs::path inputFile); // Constructor
    virtual ~BulkLoader() = default; // Defaulted virtual destructor

    void bulkLoad();

private:
    virtual std::string generateCreateTableCommand() = 0;
    virtual std::string generateCopyIntoCommand() = 0;
};
