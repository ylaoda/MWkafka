#pragma once

#include <string>
#include <set>
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include "../include/librdkafka/rdkafka.h"
#include "../include/limonp/Logging.hpp"
#include "../include/limonp/StringUtil.hpp"
#include "struct.h"

class Kafka_Conf
{
public:

	void setLogFile(const std::string& log_path, size_t level_)
	{
		initLogFile(log_path);
		setLevel(level_);
	}
protected:
	explicit Kafka_Conf(const ConfigOption& config) :m_kafka_conf(rd_kafka_conf_new(),&rd_kafka_conf_destroy)
	{
		set_conf(config);
	}
	virtual ~Kafka_Conf()
	{
		LOG_WARNING(OUT_ALL) << "~Kafka_Conf";
	}
	void set_conf(const std::vector<Option>& _conf)
	{
		for (auto iter : _conf)
		{
			if (iter.get_key().find("sdk.") != std::string::npos)
			{
				sdk_info[iter.get_key()] = iter.get_value();
			}
			else
			{
				set_conf_base(iter.get_key().c_str(), iter.get_value().c_str());
			}			
		}
	}
	bool set_conf_base(const char* item, const char* value)
	{
		if (NULL == m_kafka_conf)
		{
			LOG_ERROR(OUT_ALL) << "rd_kafka_conf_new is fail";
			return false;
		}			

		char err_str[512] = { 0 };
		rd_kafka_conf_res_t res_t = rd_kafka_conf_set(m_kafka_conf.get(), item, value, err_str, sizeof(err_str));
		if (RD_KAFKA_CONF_OK != res_t)
		{
			LOG_ERROR(OUT_ALL) <<
				"rd_kafka_conf_set is fail" <<
				"\n item:     " << item <<
				"\n value:    " << value <<
				"\n err_str:  " << err_str;
			return false;
		}
		return true;
	}
	std::string get_conf_base(const std::string& name) const
	{		
		std::string out_str="";		
		if (name.find("sdk.") != std::string::npos)
		{
			auto iter = sdk_info.find(name);
			if (iter!= sdk_info.end())
			{
				out_str = iter->second;
			}	
		}
		else
		{
			size_t size = 0;
			auto result = rd_kafka_conf_get(m_kafka_conf.get(), name.c_str(), nullptr, &size);
			if (result != RD_KAFKA_CONF_OK)
			{
				LOG_ERROR(OUT_ALL) <<
					"rd_kafka_conf_get is fail" <<
					"\n name: " << name;
				return "";
			}
			std::vector<char> buffer(size);
			rd_kafka_conf_get(m_kafka_conf.get(), name.c_str(), buffer.data(), &size);
			out_str = buffer.data();
		}		
		return out_str;
	}
	std::map<std::string, std::string> get_conf_all() const 
	{
		size_t count = 0;
		const char** all = rd_kafka_conf_dump(m_kafka_conf.get(), &count);
		auto output = parse_dump(all, count);
		rd_kafka_conf_dump_free(all, count);
		return output;
	}

	rd_kafka_conf_t* get_conf_handle() const
	{
		return m_kafka_conf.get();
	}

private:
	
	static std::map<std::string, std::string> parse_dump(const char** values, size_t count)
	{
		std::map<std::string, std::string> output;
		for (size_t i = 0; i < count; i += 2)
		{
			output[values[i]] = values[i + 1];
		}
		return output;
	}
private:
	Kafka_Conf(const Kafka_Conf&) = delete;
	Kafka_Conf(Kafka_Conf&&) = delete;
	Kafka_Conf& operator=(const Kafka_Conf&) = delete;
	Kafka_Conf& operator=(Kafka_Conf&&) = delete;

private:

	std::unique_ptr<rd_kafka_conf_t, decltype(&rd_kafka_conf_destroy)> m_kafka_conf;

	std::map<std::string, std::string>sdk_info;
}; 



class Kafka_Topic_Conf
{
public:
	
protected:
	explicit  Kafka_Topic_Conf(const ConfigOption& Topic_config):m_kafka_topic_conf(rd_kafka_topic_conf_new(), &rd_kafka_topic_conf_destroy)
	{
		set_topic_conf(Topic_config);
	}
	virtual ~Kafka_Topic_Conf()
	{
		LOG_WARNING(OUT_ALL) << "~Kafka_Topic_Conf";
	}
	void set_topic_conf(const std::vector<Option>& _conf)
	{
		for (auto iter : _conf)
		{
			set_topic_conf_base(iter.get_key().c_str(), iter.get_value().c_str());
		}
	}
	bool set_topic_conf_base(const char* item, const char* value)
	{
		if (NULL == m_kafka_topic_conf.get())
		{
			LOG_ERROR(OUT_ALL) << "rd_kafka_topic_conf_new is fail";
			return false;
		}			

		char err_str[512] = { 0 };
		rd_kafka_conf_res_t res_t = rd_kafka_topic_conf_set(m_kafka_topic_conf.get(), item, value, err_str, sizeof(err_str));
		if (RD_KAFKA_CONF_OK != res_t)
		{
			LOG_ERROR(OUT_ALL) <<
				"set_topic_conf_base is fail" <<
				"\n item:     " << item <<
				"\n value:    " << value <<
				"\n err_str:  " << err_str;
			return false;
		}
		return true;
	}
	std::string get_topic_conf_base(const std::string& name) const
	{
		size_t size = 0;
		auto result = rd_kafka_topic_conf_get(m_kafka_topic_conf.get(), name.c_str(), nullptr, &size);
		if (result != RD_KAFKA_CONF_OK)
		{
			LOG_ERROR(OUT_ALL) <<
				"rd_kafka_topic_conf_get is fail" <<
				"\n name: " << name;
			return "";
		}
		std::vector<char> buffer(size);
		rd_kafka_topic_conf_get(m_kafka_topic_conf.get(), name.data(), buffer.data(), &size);
		return std::string(buffer.data());
	}
	std::map<std::string, std::string> get_topic_conf_all() const
	{
		size_t count = 0;
		const char** all = rd_kafka_topic_conf_dump(m_kafka_topic_conf.get(), &count);
		auto output = parse_dump(all, count);
		rd_kafka_conf_dump_free(all, count);
		return output;
	}

	rd_kafka_topic_conf_t* get_topic_conf_handle() const 
	{
		return m_kafka_topic_conf.get();
	}
private:
	
	static std::map<std::string, std::string> parse_dump(const char** values, size_t count)
	{
		std::map<std::string, std::string> output;
		for (size_t i = 0; i < count; i += 2)
		{
			output[values[i]] = values[i + 1];
		}
		return output;
	}

private:
	Kafka_Topic_Conf(const Kafka_Topic_Conf&) = delete;
	Kafka_Topic_Conf(Kafka_Topic_Conf&&) = delete;
	Kafka_Topic_Conf& operator=(const Kafka_Topic_Conf&) = delete;
	Kafka_Topic_Conf& operator=(Kafka_Topic_Conf&&) = delete;

private:	
	std::unique_ptr<rd_kafka_topic_conf_t, decltype(&rd_kafka_topic_conf_destroy)>m_kafka_topic_conf;	
};

