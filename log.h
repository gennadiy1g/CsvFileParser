#pragma once

#include <boost/log/expressions.hpp>
#include <boost/log/sources/global_logger_storage.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/file.hpp>

namespace src = boost::log::sources;
namespace trivia = boost::log::trivial;
BOOST_LOG_INLINE_GLOBAL_LOGGER_DEFAULT(GlobalLogger, src::wseverity_logger_mt<trivia::severity_level>)

#define FUNCTION_FILE_LINE __FUNCTION__ << " (" << __FILE__ << ", " << __LINE__ << ')'
