#pragma once

#include "BulkLoader.h"

class MonetDBBulkLoader : public BulkLoader {

public:
    MonetDBBulkLoader(const bfs::path& inputFile); // Constructor
    virtual ~MonetDBBulkLoader() = default; // Defaulted virtual destructor

    // Disallow assignment and pass-by-value.
    MonetDBBulkLoader(const MonetDBBulkLoader& src) = delete;
    MonetDBBulkLoader& operator=(const MonetDBBulkLoader& rhs) = delete;

    // Explicitly default move constructor and move assignment operator.
    MonetDBBulkLoader(MonetDBBulkLoader&& src) = default;
    MonetDBBulkLoader& operator=(MonetDBBulkLoader&& rhs) = default;

private:
    virtual std::wstring generateCreateTableCommand() const;
    virtual std::wstring generateCopyIntoCommand() const;
};
