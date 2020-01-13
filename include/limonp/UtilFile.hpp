#pragma once
#include <string.h>
#include <string>
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <memory>
#include <thread>
#include<cstdio>
#define FILE_MAX_SIZE 100 * 1024 * 1024
#ifdef _MSC_VER
#include <windows.h>
#define _JOIN_ "\\"
#else
#define _JOIN_ "/"
#endif
class  FileUtil
{
public:
	explicit FileUtil(std::string& filename,time_t _flushInterval) :  writtenBytes_(0), _isinit(false), _isstop(false),flushInterval_(_flushInterval)
	{
#ifdef _MSC_VER
		fp_ = ::fopen(filename.c_str(), "a+");
		setvbuf(fp_, buffer_, _IOFBF, sizeof(buffer_));
#else
		fp_ = ::fopen(filename.c_str(), "ae");
		setbuffer(fp_, buffer_, sizeof buffer_);
#endif
		if (nullptr == fp_)
			abort();		
		
		_isinit = true;
		std::thread thread_(&FileUtil::threadFunc, this);
		thread_.detach();
	}
	~FileUtil() 
	{
		_isinit = false; 		
		while (!_isstop)
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		flush();
		::fclose(fp_); 
	}

	void flush() 
	{ 
		::fflush(fp_);
	}
	size_t writtenBytes() const 
	{
		return writtenBytes_;
	}

	void append(const char* logline, const size_t len)
	{
		size_t n = write(logline, len);
		size_t remain = len - n;
		while (remain > 0)
		{
			size_t x = write(logline + n, remain);
			if (x == 0)
			{
				int err = ferror(fp_);
				if (err)
				{					
					fprintf(stderr, "AppendFile::append() failed %d\n", err);
				}
				break;
			}
			n += x;
			remain = len - n; // remain -= x
		}

		writtenBytes_ += len;
	}

private:
	size_t write(const char* logline, size_t len) 
	{

#ifdef _MSC_VER
		return ::_fwrite_nolock(logline, 1, len, fp_);
#else
		return ::fwrite_unlocked(logline, 1, len, fp_);
#endif
		
	}
	void threadFunc()
	{
		static time_t _lastFlush_s = 0;
		while (_isinit)
		{			
			time_t now = ::time(NULL);
			if (now - _lastFlush_s > flushInterval_ || 0 == flushInterval_)
			{
				_lastFlush_s = now;
				flush();
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(flushInterval_ * 500));
		}
		_isstop = true;
	}
private:
	FILE* fp_;
	char buffer_[64 * 1024];
	size_t writtenBytes_;
	bool _isinit;
	bool _isstop;
	time_t flushInterval_;
private:
	FileUtil() = delete;
	const FileUtil& operator=(const FileUtil&) = delete;

};

class LogFile
{
public:
	explicit LogFile(const std::string& basepath= "log",
		const std::string& basename="log",
		size_t rollSize = FILE_MAX_SIZE, 
		bool threadSafe = true,
		int flushInterval = 3,
		int checkEveryN = 0)
		: basepath_(basepath),
		basename_(basename),
		rollSize_(rollSize),
		flushInterval_(flushInterval),		
		checkEveryN_(checkEveryN),
		_threadSafe(threadSafe),
		count_(0)
	{
		assert(basename.find(_JOIN_) == std::string::npos);
		if (basepath_.find(_JOIN_) == std::string::npos)
		{
			basepath_.append(_JOIN_);
		}
		rollFile();		
	}
	~LogFile() {}
	
	void append(const char* logline, size_t len)
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (_threadSafe)
		{			
			append_unlocked(logline, len);
		}
		else
		{
			append_unlocked(logline, len);
		}
	}
	void flush()
	{
		if (_threadSafe)
		{
			std::lock_guard<std::mutex> lock(mutex_);
			file_->flush();
		}
		else
		{
			file_->flush();
		}
	}
	bool rollFile()
	{
		static  time_t lastRoll_ = 0;
		time_t now = time(NULL);
		if (now != lastRoll_)
		{
			startOfPeriod_ = (now + k8HourToSeconds_) / kRollPerSeconds_ * kRollPerSeconds_;

			lastRoll_ = now;

			std::string filename = getLogFileName(basepath_, basename_, now);	
			std::string createDir;
			std::size_t pos = filename.find_last_of(_JOIN_);
			if (pos != std::string::npos)
			{
				createDir = filename.substr(0, pos);
#ifdef _MSC_VER	
				std::string command = "mkdir " + createDir;				
				system(command.c_str());
				//CreateDirectoryA(createDir.c_str(), NULL); //flag 为 true 说明创建成功
		 
#else
				std::string command = "mkdir -p " + createDir;
				system(command.c_str());
				//mkdir(createDir.c_str(), 0755); // 返回 0 表示创建成功，-1 表示失败	 
#endif				
			}

			file_.reset(new FileUtil(filename, flushInterval_));
			return true;
		}
		return false;
	}

private:
	void append_unlocked(const char* logline, size_t len)
	{
		file_->append(logline, len);

		if (file_->writtenBytes() > rollSize_)
		{
			rollFile();
		}
		else if (++count_ >= checkEveryN_)
		{				
			count_ = 0;				
			time_t now = ::time(NULL);
			time_t thisPeriod_ = (now + k8HourToSeconds_) / kRollPerSeconds_ * kRollPerSeconds_;
			if (thisPeriod_ != startOfPeriod_)
			{
				rollFile();
			}
		}
	}
	static void get_tm(time_t& _time,struct tm& _Tm)
	{
#ifdef _MSC_VER	
		localtime_s(&_Tm, &_time);
#else
		localtime_r(&_time, &_Tm);
#endif	
	}
	static std::string getLogFileName(const std::string& basepath, const std::string& basename, time_t now)
	{
		std::string filename;
		filename.reserve(basepath.size() + basename.size() + 64);
		filename = basepath;

		char datebuf[16] = {0};
		char timebuf[16] = {0};

		struct tm tm;
		get_tm(now, tm);

		strftime(datebuf, sizeof datebuf, "%Y%m%d", &tm);
		strcat(datebuf, _JOIN_);
		strftime(timebuf, sizeof timebuf, "-%H%M%S", &tm);

		filename.append(datebuf).append(basename).append(timebuf).append(".log");
		return filename;
	}
	
private:
	std::string basepath_;
	const std::string basename_;
	const size_t rollSize_;
	const int flushInterval_;
	const int checkEveryN_;

	bool _threadSafe;	
	int count_;
	std::mutex mutex_;
	
	std::unique_ptr<FileUtil> file_;	
	const static int kRollPerSeconds_ = 60 * 60 * 24;
	const static int k8HourToSeconds_ = 60 * 60 * 8;

	time_t startOfPeriod_ = 0;

	private:
		LogFile() = delete;
		const LogFile& operator=(const LogFile&) = delete;

};