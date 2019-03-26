#include <boost/log/attributes/attribute.hpp>
#include <boost/log/attributes/current_thread_id.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/expressions/formatters/date_time.hpp>
#include <boost/log/sources/global_logger_storage.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/file.hpp>

#include <iostream>
#include <thread>

namespace lgg = boost::log;
namespace src = boost::log::sources;
namespace kwr = boost::log::keywords;
namespace trv = boost::log::trivial;
namespace expr = boost::log::expressions;

BOOST_LOG_INLINE_GLOBAL_LOGGER_DEFAULT(gLogger, src::severity_logger_mt<trv::severity_level>)

void initLogging()
{
    lgg::add_file_log(
        kwr::file_name = "trace.log",
        kwr::format = (expr::stream
            << expr::attr<unsigned int>("LineID") << ' ' << lgg::trivial::severity << ' '
            << expr::format_date_time<boost::posix_time::ptime>("TimeStamp", " %Y-%m-%d %H:%M:%S:%f ")
            << expr::attr<boost::log::thread_id>("ThreadID") << ' ' << expr::message));
    lgg::add_common_attributes();
}

int main(int argc, char** argv)
{
    initLogging();
    src::severity_logger_mt<trv::severity_level>& slg = gLogger::get();
    BOOST_LOG_SEV(slg, trv::trace) << "Greetings from the global logger!";

    unsigned int n = std::thread::hardware_concurrency();
    std::cout << n << " concurrent threads are supported.\n";
    return 0;
}
