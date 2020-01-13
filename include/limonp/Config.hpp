#ifndef LIMONP_CONFIG_H
#define LIMONP_CONFIG_H

#include <map>

#include <fstream>
#include <iostream>
#include <assert.h>
#include "StringUtil.hpp"
namespace limonp {
class Config
{
public:
	explicit Config(const std::string& filePath)
	{
		if (!filePath.empty())
		{
			loadFile_(filePath);
		}		
	}

	const std::string Gets(const std::string& key, const std::string& root = "_root_") const
	{
		auto _root = map_.find(root);
		if (_root != map_.end())
		{
			auto iter = _root->second.find(key);
			if (iter != _root->second.end())
			{
				return iter->second;
			}
		}
		return "";
	}
	int Geti(const std::string& key, const std::string& root = "_root_",int value = -1) const
	{
		if (Gets(key, root).empty())
		{
			return value;
		}
		return atoi(Gets(key).c_str());
	}
	double Getf(const std::string& key, const std::string& root = "_root_", double value = 0.0) const
	{
		if (Gets(key, root).empty())
		{
			return value;
		}
		return atof(Gets(key).c_str());
	}
	std::map<std::string, std::string> operator [] (const char* key) const
	{
		auto it = map_.find(key);
		if (it != map_.end())
		{
			return it->second;
		}
		return std::map<std::string, std::string>();
	}
	std::map<std::string, std::string> operator [] (const std::string& key) const
	{
		auto it = map_.find(key);
		if (it != map_.end())
		{
			return it->second;
		}
		return std::map<std::string, std::string>();
	}
	std::map<std::string, std::string> getItem(const std::string& key) const
	{
		auto iter = map_.find(key);
		if (iter != map_.end())
		{
			return iter->second;
		}
		return std::map<std::string, std::string>();
	}
	operator bool()
	{
		return !map_.empty();
	}
private:
	bool isNotes(std::string& line) //是否是注释
	{		
		if (line.empty())
			return false;		

		if (line[0] != '#')
			return false;

		return true;
	}
	bool isNodes(std::string& line)//是否是节点
	{
		if (line.empty())
			return false;

		if (line[0] != '[' || line[line.size() - 1] != ']')
			return false;

		return true;
	}
	void loadFile_(const std::string& filePath)
	{
		std::ifstream ifs(filePath.c_str());		
		std::string line;
		std::string _node;
		while (getline(ifs, line))
		{
			limonp::Trim(line);

			if (isNotes(line))//注释
				continue;

			if (isNodes(line))//是节点的话先取出节点
				_node = limonp::GetSubPrarms(line);
		
			if (_node.empty())
				_node = "_root_";			
			
			std::vector<std::string> vecBuf;
			limonp::Split(line, vecBuf, "=");
			if (2 != vecBuf.size())
				continue;

			std::string key =   vecBuf[0];
			std::string value = vecBuf[1];			
			limonp::Trim(key);
			limonp::Trim(value);

			map_[_node][key] = value;			
		}
	}

private:
	std::map<std::string, std::map<std::string,std::string>> map_;
};

} // namespace limonp

#endif // LIMONP_CONFIG_H
