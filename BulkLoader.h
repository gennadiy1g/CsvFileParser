#pragma once

#include <string>

class BulkLoader {

public:
    BulkLoader() = default; // Constructor
    virtual ~BulkLoader() = default; // Defaulted virtual destructor

    void bulkLoad();

private:
    virtual std::string generateCreateTableCommand() = 0;
    virtual std::string generateCopyIntoCommand() = 0;
};
