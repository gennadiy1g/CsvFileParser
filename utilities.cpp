#include <boost/filesystem.hpp>
#include <boost/locale.hpp>

#include "log.h"

namespace bfs = boost::filesystem;
namespace blocale = boost::locale;

void initLocalization()
{
    // Get global backend, and select winapi backend as default for all categories
    blocale::localization_backend_manager::global().select("winapi");

    /* Create and install global locale. Non UTF-8 encodings are not supported by winapi backend.
     * https://www.boost.org/doc/libs/1_69_0/libs/locale/doc/html/using_localization_backends.html */
    std::locale::global(blocale::generator().generate("en_US.UTF-8"));

    // This is needed to prevent C library to
    // convert strings to narrow
    // instead of C++ on some platforms
    std::ios_base::sync_with_stdio(false);

    // Use the new global locale for future wide character output
    // https://en.cppreference.com/w/cpp/locale/locale
    std::wcout.imbue(std::locale());
}

void initLogging()
{
    blog::add_file_log(
#ifdef NDEBUG
        blkeywords::file_name = bfs::path(bfs::temp_directory_path() / "BulkLoadCsv.log"),
#else
        blkeywords::file_name = "trace.log",
#endif
        blkeywords::format = (blexpressions::stream
            << blexpressions::attr<unsigned int>("LineID") << ' ' << bltrivial::severity << ' '
            << blexpressions::format_date_time<boost::posix_time::ptime>("TimeStamp", " %Y-%m-%d %H:%M:%S.%f ")
            << blexpressions::attr<blog::thread_id>("ThreadID") << ' ' << blexpressions::message));
    blog::add_common_attributes();
#ifdef NDEBUG
    blog::core::get()->set_filter(bltrivial::severity >= bltrivial::info);
#endif
}
