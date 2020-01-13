#pragma once
#include <string>
#include <set>
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include <string.h>
#include "handle_base.hpp"

class Producer : public KafkaHandle
{
public:
	~Producer()
	{
		isInit = false;
		Stop(5000);		
	}
	Producer(const ConfigOption& config) :Producer(config, {}){}
	Producer(const std::string& conf_path) :KafkaHandle(load("global", conf_path), load("topic", conf_path)) {}
	Producer(const ConfigOption& config, const ConfigOption& Topic_config) : KafkaHandle(config, Topic_config), 
		pInvoker_(nullptr), isInit(false), _isstop(false), max_rd_kafka_outq_len_(10000){}

public:

	bool Stop(int timeout_ms)
	{
		rd_kafka_t* handle = get_handle();
		if (handle)
		{
			while (rd_kafka_outq_len(handle) > 0)
			{
				if (RD_KAFKA_RESP_ERR_NO_ERROR == rd_kafka_flush(handle, timeout_ms))
					break;

				const char* err = rd_kafka_err2str(rd_kafka_last_error());
				LOG_INFO(OUT_ALL) << " | Stopping...:"
					<< " | outque: " << rd_kafka_outq_len(handle)
					<< " | errmsg: " << (err ? err : "unknown err");
			}
		}
		_isstop = true;
		return true;
	}
	void poll(int timeout_ms)
	{
		while (!_isstop && isInit)
		{
			Poll(timeout_ms);
			std::this_thread::sleep_for(std::chrono::milliseconds(limonp::get_rand(1000)));
		}
	}
	void Poll(int timeout_ms)
	{		
		rd_kafka_t* handle = get_handle();
		if (handle)
		{
			rd_kafka_poll(handle, timeout_ms);
		}		
	}

	int Produce(const Topic_Partition& _topic_info,const std::string& _buffer, const std::string& _key="")
	{
		rd_kafka_topic_t* rkt = get_topic_handle(_topic_info);
		if (nullptr == rkt)
		{
			LOG_ERROR(OUT_ALL) << "Produce get_topic_handle is fail";
			return -2;
		}
		if (is_callback())
		{
			data_message_t* pMsg = new data_message_t();
			if (nullptr == pMsg)
			{
				LOG_ERROR(OUT_ALL) << "Produce new data_message_t is fail";
				return -2;
			}
			pMsg->topic = _topic_info.get_topic();
			pMsg->data.resize(_buffer.size());
			pMsg->data = _buffer;
			pMsg->key = _key;
			pMsg->retry_cnt_ = 0;
			pMsg->producer_tm_ms = limonp::GetCurrentTimeMs();
			void* msg_opaque = static_cast<void*>(const_cast<data_message_t*>(pMsg));
			return InternalProduce(rkt,_topic_info, pMsg->data.c_str(), pMsg->data.size(), pMsg->key.c_str(), pMsg->key.size(), msg_opaque);
		}	
		return InternalProduce(rkt,_topic_info, _buffer.c_str(), _buffer.size(), _key.c_str(), _key.size(), nullptr);
	}

	bool AddTopics(const std::string& topic_info)
	{
		std::lock_guard<std::mutex> lock(lock_init_);
		if (!init_handle())
		{
			LOG_ERROR(OUT_ALL) << "AddTopic init is fail";
			return false;
		}
		AddTopic({ topic_info });
		return true;
	}
	//设置最大允许RDKAFKA OUTQUE的大小
	void SetMaxOutQueSize(size_t size)
	{
		max_rd_kafka_outq_len_ = size;
	}
	
private:

	int InternalProduce(rd_kafka_topic_t* rkt, const Topic_Partition& _topic_info, const char* data, size_t data_len, const char* key, size_t key_len, void* msg_opaque)
	{	
		////如果超过设定的队列大小,通知rdkafka将消息发往kafka,并等待一段时间,若在等待期间一直未投递完,则超时返回
		int loop_cnt = 30;
		while (rd_kafka_outq_len(get_handle()) >= (int)max_rd_kafka_outq_len_ && --loop_cnt >= 0)
		{	
			LOG_WARNING(OUT_ALL) <<  "outque len > " << max_rd_kafka_outq_len_ << " wait...";
			rd_kafka_poll(get_handle(), 100);
		}	
		int msgflag = msg_opaque ? 0x00 : RD_KAFKA_MSG_F_COPY;
		 
		int iRet = rd_kafka_produce(rkt,
			_topic_info.get_partition(),
			msgflag,
			static_cast<void*>(const_cast<char*>(data)),
			data_len,
			static_cast<void*>(const_cast<char*>(key)),
			key_len,
			msg_opaque);

		if (iRet == -1)
		{
			rd_kafka_resp_err_t err = rd_kafka_last_error();

			if (err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION)
			{
				LOG_ERROR(OUT_ALL) << "Error: No such partition: " << _topic_info.get_partition() << "  error: " << rd_kafka_err2str(err) << "  ret : " << err;
			}
			else
			{
				LOG_ERROR(OUT_ALL) << "Error: InternalProduce error: " << rd_kafka_err2str(err) << "  ret : " << err;
			}

			rd_kafka_poll(get_handle(), 10); //Poll to handle delivery reports
			return -2;
		}	

		// 通知rdkafka及时提交信息到kafka
		rd_kafka_poll(get_handle(), 0);
		return iRet;
	}
	
	bool init_handle()
	{
		if (isInit == false)
		{			
			if (get_conf_base("sdk.set_partitioner_cb") == "true")
			{
				set_partitioner_cb();
			}
			isInit = create_handle(RD_KAFKA_PRODUCER);
			if (!isInit)
			{
				LOG_ERROR(OUT_ALL) << "RD_KAFKA_PRODUCER create_handle is fail ret";
				return false;
			}
			std::thread ts(&Producer::poll, this, 500);
			ts.detach();
		}
		return true;
	}
private:		
	void* pInvoker_;
	bool isInit;
	bool _isstop;

	//允许rdkafka内部最大的缓冲队列大小
	size_t  max_rd_kafka_outq_len_;

	std::mutex lock_init_;
};
