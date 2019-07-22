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

    virtual void loadOnClient(std::string_view host, int port, std::string_view database, std::string_view table, std::string_view user, std::string_view password) const {};
    virtual std::wstring generateDropTableCommand(const std::wstring_view table) const;
    virtual std::wstring generateCreateTableCommand(const std::wstring_view table) const;
    virtual std::wstring generateCopyIntoCommand(const std::wstring_view table) const;
    virtual std::optional<std::size_t> rejectedRecords(const nanodbc::connection& connecion) const;
};
