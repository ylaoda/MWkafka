#ifndef LIMONP_STR_FUNCTS_H
#define LIMONP_STR_FUNCTS_H
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <cctype>
#include <map>
#include <stdint.h>
#include <stdio.h>
#include <memory.h>
#include <functional>
#include <locale>
#include <sstream>
#include <sys/types.h>
#include <iterator>
#include <algorithm>
#include <stdarg.h>
#include <chrono>
#include <thread>
namespace limonp {

using namespace std;
inline string StringFormat(const char* fmt, ...)
{
	int size = 256;
	std::string str;
	va_list ap;
	while (1)
	{
		str.resize(size);
		va_start(ap, fmt);
		int n = vsnprintf((char *)str.c_str(), size, fmt, ap);
		va_end(ap);
		if (n > -1 && n < size)
		{
			str.resize(n);
			return str;
		}
		if (n > -1)
			size = n + 1;
		else
			size *= 2;
	}
	return str;
}

template<class T>
void Join(T begin, T end, string& res, const string& connector) 
{
	if (begin == end) 
	{
		return;
	}
	stringstream ss;
	ss << *begin;
	begin++;
	while (begin != end) 
	{
		ss << connector << *begin;
		begin++;
	}
	res = ss.str();
}

template<class T>
string Join(T begin, T end, const string& connector) 
{
	string res;
	Join(begin, end, res, connector);
	return res;
}

inline string& Upper(string& str) 
{
	transform(str.begin(), str.end(), str.begin(), (int(*)(int))toupper);
	return str;
}

inline string& Lower(string& str) 
{
	transform(str.begin(), str.end(), str.begin(), (int(*)(int))tolower);
	return str;
}

inline bool IsSpace(unsigned c) 
{
	return c > 0xff ? false : std::isspace(c & 0xff);
}

inline std::string& LTrim(std::string &s) 
{
	s.erase(s.begin(), std::find_if(s.begin(), s.end(), std::not1(std::ptr_fun<unsigned, bool>(IsSpace))));
	return s;
}

inline std::string& RTrim(std::string &s) 
{
	s.erase(std::find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<unsigned, bool>(IsSpace))).base(), s.end());
	return s;
}

inline std::string& Trim(std::string &s) 
{
	return LTrim(RTrim(s));
}

inline std::string& LTrim(std::string & s, char x) 
{
	s.erase(s.begin(), std::find_if(s.begin(), s.end(), std::not1(std::bind2nd(std::equal_to<char>(), x))));
	return s;
}

inline std::string& RTrim(std::string & s, char x) 
{
	s.erase(std::find_if(s.rbegin(), s.rend(), std::not1(std::bind2nd(std::equal_to<char>(), x))).base(), s.end());
	return s;
}

inline std::string& Trim(std::string &s, char x)
{
	return LTrim(RTrim(s, x), x);
}

inline std::string& TrimS(std::string &value, char x)
{
	value.erase(std::find_if(value.begin(), value.end(), [=](char ch) {return ch == x; }), value.end());
	return value;
}


inline void Split(const string& src, vector<string>& res, const string& pattern, size_t maxsplit = string::npos) 
{
	res.clear();
	size_t Start = 0;
	size_t end = 0;
	string sub;
	while (Start < src.size()) 
	{
		end = src.find_first_of(pattern, Start);
		if (string::npos == end || res.size() >= maxsplit) 
		{
			sub = src.substr(Start);
			res.push_back(sub);
			return;
		}
		sub = src.substr(Start, end - Start);
		res.push_back(sub);
		Start = end + pattern.size();
	}
	return;
}

inline vector<string> Split(const string& src, const string& pattern, size_t maxsplit = string::npos)
{
	vector<string> res;
	Split(src, res, pattern, maxsplit);
	return res;
}


inline bool StartsWith(const string& str, const string& prefix)
{
	if (prefix.length() > str.length())
	{
		return false;
	}
	return 0 == str.compare(0, prefix.length(), prefix);
}

inline bool EndsWith(const string& str, const string& suffix) 
{
	if (suffix.length() > str.length())
	{
		return false;
	}
	return 0 == str.compare(str.length() - suffix.length(), suffix.length(), suffix);
}

inline bool IsInStr(const string& str, char ch)
{
	return str.find(ch) != string::npos;
}


/*
 * format example: "%Y-%m-%d %H:%M:%S"
 */
inline void GetTime(const string& format, string&  timeStr) 
{
	time_t timeNow;
	time(&timeNow);
	timeStr.resize(64);
	size_t len = strftime((char*)timeStr.c_str(), timeStr.size(), format.c_str(), localtime(&timeNow));
	timeStr.resize(len);
}

inline string PathJoin(const string& path1, const string& path2) 
{
	if (EndsWith(path1, "/"))
	{
		return path1 + path2;
	}
	return path1 + "/" + path2;
}

inline std::string GetSubPrarms(const std::string& strSrc, const std::string& TagBeg = "[", const std::string& TagEnd = "]")
{
	if (strSrc.size() <= 0)
	{
		printf("strPrarms.size() <= 0");
		return "";
	}

	std::string SubPrarms;
	size_t iPos1 = 0;
	size_t iPos2 = 0;

	iPos1 = strSrc.find(TagBeg, iPos1);
	if (iPos1 == std::string::npos)
	{
		return "";
	}

	iPos1 += TagBeg.length();

	iPos2 = strSrc.find(TagEnd, iPos1);
	if (iPos2 == std::string::npos)
	{
		return "";
	}

	SubPrarms = strSrc.substr(iPos1, iPos2 - iPos1);

	return SubPrarms;
}
inline int64_t GetCurrentTimeMs()
{
	auto time_now = std::chrono::system_clock::now();
	auto duration_in_ms = std::chrono::duration_cast<std::chrono::milliseconds>(time_now.time_since_epoch());
	return duration_in_ms.count();
}
inline int64_t GetDiffMs(int64_t last_time)
{
	auto time_now = std::chrono::system_clock::now();
	auto duration_in_ms = std::chrono::duration_cast<std::chrono::milliseconds>(time_now.time_since_epoch());
	return duration_in_ms.count() - last_time;
}
inline std::string GetClientId()
{
	const char* c_name = "MWkfk";
	std::string clientId;

	time_t tt = time(NULL);
	tm* t = localtime(&tt);
	if (NULL != t)
	{
		clientId = StringFormat("%s_%ld_%d-%02d-%02d-%02d-%02d-%02d",
			c_name,
			std::this_thread::get_id(),
			t->tm_year + 1900,
			t->tm_mon + 1,
			t->tm_mday,
			t->tm_hour,
			t->tm_min,
			t->tm_sec);
	}
	else
	{
		clientId = StringFormat("%s_%ld", c_name, std::this_thread::get_id());
	}	
	return clientId;
}
//djb hashÀ„∑®
inline unsigned int djb_hash(const char* str, size_t len)
{
	unsigned int hash = 5381;
	for (size_t i = 0; i < len; i++)
	{
		hash = ((hash << 5) + hash) + str[i];
	}
	return hash;
}
template <typename T, typename = typename std::enable_if<std::is_integral<T>::value>::type>
inline T get_rand(T nMax)
{
	std::random_device rd;
	return rd() % nMax;
}

} // namespace limonp

#endif
