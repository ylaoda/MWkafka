#pragma once
#ifndef __KAFKA_HEADER__
#define __KAFKA_HEADER__
#include <string>
#include <set>
#include <map>
#include <vector>
#include <memory>
#include <functional>

#define MWKFK_VERSION  "V1.0"
#define MWKFK_VERSION_NUM  20200108

#ifdef KAFKA_LIB
# ifndef KAFKA_LIB
#  define KAFKA_LIB
# endif
#elif defined(KAFKA_DLL) || defined(_WINDLL)
# if defined(KAFKA_EXPORTS) || defined(kafka_EXPORTS)
#  ifndef KAFKA_LIB
#   define KAFKA_LIB __declspec(dllexport)
#  endif
# elif !defined(KAFKA_LIB)
#  define KAFKA_LIB __declspec(dllimport)
# endif
#elif !defined(KAFKA_LIB)
# define KAFKA_LIB
#endif

enum Offset
{
	OFFSET_BEGINNING = -2,
	OFFSET_END = -1,
	OFFSET_STORED = -1000,
	OFFSET_INVALID = -1001
};
enum CALL_BACK_TYPE
{
	CONSUMED = 1,
	OFFSETCOMMIT,
	DELIVERED,
	ONERROR
};
struct topic_info_t
{
	std::string topic;
	int32_t partition;
	int64_t offset;

	topic_info_t(const std::string& topic_name) :topic_info_t(topic_name, -1) {}
	topic_info_t(const std::string& topic_name, int32_t _partition) :topic_info_t(topic_name, _partition, OFFSET_INVALID) {}
	topic_info_t(const std::string& topic_name, int32_t _partition, int64_t _offset) :topic(topic_name), partition(_partition), offset(_offset) {}
};
struct data_message_t
{
	std::string topic;		//消息topic_name
	std::string data;		//消息的内容,按size()长度取值
	std::string key;		//消息的key,按size()长度取值

	int32_t partition;		//消息所在分区
	int64_t offset;			//消息所在分区上的偏移量
	int errcode;			//错误码 0:成功       非0:失败,原因见errmsg
	std::string errmsg;		//错误码描述
	int status;				//Producer消息对应的投递状态 0:NOT_PERSISTED 1:POSSIBLY_PERSISTED 2:PERSISTED
							//errcode=0&&status=2时表示成功,其他状态表示失败	

	int64_t producer_tm_ms;	        //该消息调用produce接口的时间
	int64_t producer_cb_tm_ms;		//该消息调用produce回调的时间

	int64_t consumer_tm_ms;	        //消费到该消息的时间	
	int64_t commit_offset_tm_ms;	//该消息offset提交的时间
	int64_t committed_tm_ms;        //该消息offset成功提交回执的时间

	int64_t retry_cnt_;

	const data_message_t& operator==(const data_message_t& other)
	{
		this->topic = other.topic;
		this->data = other.data;
		this->key = other.key;

		this->partition = other.partition;
		this->offset = other.offset;
		this->errcode = other.errcode;
		this->errmsg = other.errmsg;
		this->status = other.status;
		this->consumer_tm_ms = other.consumer_tm_ms;
		this->producer_tm_ms = other.producer_tm_ms;
		this->producer_cb_tm_ms = other.producer_cb_tm_ms;
		this->commit_offset_tm_ms = other.commit_offset_tm_ms;
		this->committed_tm_ms = other.committed_tm_ms;
		this->retry_cnt_ = other.retry_cnt_;
		return *this;
	}
};
using MessagePtr = std::shared_ptr<data_message_t>;
using DoneCallback = std::function<void(void* pInvoker, const MessagePtr& cb_msg_ptr)>;

class Option
{
public:
	Option(const std::string& key, const std::string& value) : key_(key), value_(value) {}
	Option(const std::string& key, const char* value) : key_(key), value_(value) {}
	Option(const std::string& key, bool value) : key_(key), value_(value ? "true" : "false") {}

	template <typename T, typename = typename std::enable_if<std::is_integral<T>::value>::type>
	Option(const std::string & key, T value) : Option(key, std::to_string(value)) {}
	const std::string& get_key() const { return key_; }
	const std::string& get_value() const { return value_; }
private:
	std::string key_;
	std::string value_;
};
using ConfigOption = std::vector<Option>;
#endif

