#pragma once

#include "BulkLoader.h"

class MonetDBBulkLoader : public BulkLoader {

public:
    explicit MonetDBBulkLoader(const bfs::path& inputFile); // Constructor
    virtual ~MonetDBBulkLoader() = default; // Defaulted virtual destructor

    // Disallow assignment and pass-by-value.
    MonetDBBulkLoader(const MonetDBBulkLoader& src) = delete;
    MonetDBBulkLoader& operator=(const MonetDBBulkLoader& rhs) = delete;

    // Explicitly default move constructor and move assignment operator.
    MonetDBBulkLoader(MonetDBBulkLoader&& src) = default;
    MonetDBBulkLoader& operator=(MonetDBBulkLoader&& rhs) = default;

    virtual std::wstring generateDropTableCommand(const std::wstring_view table) const override;
    virtual std::wstring generateCreateTableCommand(const std::wstring_view table) const override;
    virtual std::wstring generateCopyIntoCommand(const std::wstring_view table) const override;
    virtual std::optional<std::size_t> getRejectedRecords(nanodbc::connection& connection) const override;
};

class NanodbcMonetDBBulkLoader : public MonetDBBulkLoader {
public:
    explicit NanodbcMonetDBBulkLoader(const bfs::path& inputFile); // Constructor
    virtual ~NanodbcMonetDBBulkLoader() = default; // Defaulted virtual destructor

    // Disallow assignment and pass-by-value.
    NanodbcMonetDBBulkLoader(const NanodbcMonetDBBulkLoader& src) = delete;
    NanodbcMonetDBBulkLoader& operator=(const NanodbcMonetDBBulkLoader& rhs) = delete;

    // Explicitly default move constructor and move assignment operator.
    NanodbcMonetDBBulkLoader(NanodbcMonetDBBulkLoader&& src) = default;
    NanodbcMonetDBBulkLoader& operator=(NanodbcMonetDBBulkLoader&& rhs) = default;

    std::wstring getConnectionString() const;
    virtual std::optional<std::size_t> load(std::wstring_view table = L""sv) const override;
};

class MclientMonetDBBulkLoader : public MonetDBBulkLoader {
public:
    explicit MclientMonetDBBulkLoader(const bfs::path& inputFile); // Constructor
    virtual ~MclientMonetDBBulkLoader() = default; // Defaulted virtual destructor

    // Disallow assignment and pass-by-value.
    MclientMonetDBBulkLoader(const MclientMonetDBBulkLoader& src) = delete;
    MclientMonetDBBulkLoader& operator=(const MclientMonetDBBulkLoader& rhs) = delete;

    // Explicitly default move constructor and move assignment operator.
    MclientMonetDBBulkLoader(MclientMonetDBBulkLoader&& src) = default;
    MclientMonetDBBulkLoader& operator=(MclientMonetDBBulkLoader&& rhs) = default;

    virtual std::optional<std::size_t> load(std::wstring_view table = L""sv) const override;
};
