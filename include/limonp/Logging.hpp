#ifndef LIMONP_LOGGING_HPP
#define LIMONP_LOGGING_HPP
#include <string.h>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <cassert>
#include <cstdlib>
#include <ctime>
#include <mutex>
#include <fstream>
#include <chrono>
#include <stdarg.h>
#include "UtilFile.hpp"

#define FILE_BASENAME strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : strrchr(__FILE__, '\\')? strrchr(__FILE__, '\\') + 1 : __FILE__
#define XCHECK(exp) if(!(exp)) XLOG(FATAL) << "exp: ["#exp << "] false. "

#define LOG_FMT(type,fmt, ...) Logger(LogLevel::LL_DEBUG, type,fmt，FILE_BASENAME, __LINE__, fmt, ## __VA_ARGS__) 
#define XLOG(level,type)       Logger(LogLevel::LL_##level,type, FILE_BASENAME, __LINE__).Stream() 

#define LOG_DEBUG(type)      XLOG(DEBUG,type)
#define LOG_INFO(type)       XLOG(INFO,type)
#define LOG_WARNING(type)    XLOG(WARNING,type)
#define LOG_ERROR(type)      XLOG(ERROR,type)
#define LOG_FATAL(type)      XLOG(FATAL,type)




class log_global
{
public:
	static log_global* instance()
	{
		static log_global obj;
		return &obj;
	}
	log_global() : init(false) , _level(0),logfileptr_(nullptr){}
	~log_global() {}

	void inits(const std::string& basepath,	const std::string& basename,size_t rollSize,bool threadSafe,int flushInterval,int checkEveryN)
	{		
		if (init)
			return;

		logfileptr_.reset(new LogFile(basepath, basename, rollSize, threadSafe, flushInterval, checkEveryN));
		init = true;
	}
	bool writeLogFile(size_t level,const std::string& s_str)
	{
		if (level < _level)
			return false;

		if (nullptr == logfileptr_)
			return false;

		logfileptr_->append(s_str.c_str(), s_str.size());

		return true;
	}
	void setLevel(size_t level_)
	{
		_level = level_;
	}
	size_t getLevel()
	{
		return _level;
	}
private:
	bool init;
	size_t _level;
	std::shared_ptr<LogFile> logfileptr_;	
};

inline void initLogFile(const std::string& basepath,
	const std::string& basename = "log",
	size_t rollSize = FILE_MAX_SIZE,
	bool threadSafe = true,
	int flushInterval = 3,
	int checkEveryN = 0)
{
	log_global::instance()->inits(basepath, basename, rollSize, threadSafe, flushInterval, checkEveryN);
}
inline bool writeLogFile(size_t level, const std::string& s_str)
{
	return log_global::instance()->writeLogFile(level,s_str);
}
inline void setLevel(size_t level)
{
	return log_global::instance()->setLevel(level);
}
inline size_t getLevel()
{
	return log_global::instance()->getLevel();
}


static const char* LOG_LEVEL_ARRAY[] = { "DEBUG","INFO","WARN","ERROR","FATAL" };
static const char* LOG_TIME_FORMAT = "%Y-%m-%d %H:%M:%S";
enum LogLevel {
	LL_DEBUG = 0,
	LL_INFO = 1,
	LL_WARNING = 2,
	LL_ERROR = 3,
	LL_FATAL = 4,
};

//日志输出的方式
enum OUTPUT_TYPE
{
	OUT_FILE=1,		//输出到本地文件
	OUT_STDOUT,	    //直接打印
	OUT_ALL,		//输出全部
};
inline void get_tm(struct tm& _Tm)
{
	time_t _time = time(nullptr);
#ifdef _MSC_VER	
	localtime_s(&_Tm, &_time);
#else
	localtime_r(&_time, &_Tm);
#endif	
}
class Logger
{
public:
	Logger(size_t level,int type, const char* filename, int lineno) : level_(level), _type(type)
	{
		if (level < getLevel())
			return;

		assert(level_ <= sizeof(LOG_LEVEL_ARRAY) / sizeof(*LOG_LEVEL_ARRAY));
		char buf[32];
		struct tm _Tm;
		get_tm(_Tm);
		strftime(buf, sizeof(buf), LOG_TIME_FORMAT, &_Tm);
		stream_ << buf
			<< " [ "
			<< filename
			<< ":" << lineno
			<< " " << LOG_LEVEL_ARRAY[level_]
			<< " ]"
			<< " ";
	}

	Logger(size_t level, int type, const char* filename, int lineno, const char* const fmt, ...) : level_(level), _type(type)
	{
		if (level < getLevel())
			return;

		int size = 256;
		std::string msg;
		va_list ap;
		while (1) {
			msg.resize(size);
			va_start(ap, fmt);
			int n = vsnprintf((char*)msg.c_str(), size, fmt, ap);
			va_end(ap);
			if (n > -1 && n < size) {
				msg.resize(n);
				break;
			}
			if (n > -1)
				size = n + 1;
			else
				size *= 2;
		}

		char buf[32];
		struct tm _Tm;
		get_tm(_Tm);
		strftime(buf, sizeof(buf), LOG_TIME_FORMAT, &_Tm);
		stream_ << buf
			<< " [ "
			<< filename
			<< ":" << lineno
			<< " ]"
			<< " "
			<< msg
			<< " ";
	}


	~Logger()
	{
		if (level_ < getLevel())
			return;

		stream_ << "\n";
		switch (_type)
		{
		case OUTPUT_TYPE::OUT_FILE:
			writeLogFile(level_, stream_.str());
			break;
		case OUTPUT_TYPE::OUT_STDOUT:
			std::cerr << stream_.str() << std::endl;
			break;
		case OUTPUT_TYPE::OUT_ALL:
		{
			std::cerr << stream_.str() << std::endl;
			writeLogFile(level_, stream_.str());
		}
		break;	
		default:
			std::cerr << stream_.str() << std::endl;
			break;
		}		

		if (level_ >= LogLevel::LL_FATAL)
		{
			abort();
		}
	}

	std::ostream& Stream()
	{
		return stream_;
	}


private:
	std::ostringstream stream_;
	size_t level_;
	int _type;

private:
	Logger(const Logger&) = delete;
	const Logger& operator =(const Logger&) = delete;
	Logger(Logger&&) = delete;
	const Logger& operator =(Logger&&) = delete;

}; // class Logger






#endif // LIMONP_LOGGING_HPP
