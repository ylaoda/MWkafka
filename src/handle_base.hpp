#pragma once
#include <string>
#include <set>
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include <random>
#include <algorithm>
#include "topic_partition.hpp"
#include "config_base.hpp"
#include "../include/limonp/Config.hpp"


class KafkaHandle :public Kafka_Conf, public Kafka_Topic_Conf
{	
	using HandlePtr = std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)>;
public:
	void SetInvoker(void* pInvoker) { pInvoker_ = pInvoker; }	

	void AddCallBack(int _type, DoneCallback pCallBackCb)
	{
		std::lock_guard<std::mutex> lock(lock_callback);
		_callback[_type] = pCallBackCb;	
		switch (_type)
		{
		case CONSUMED:
			set_rebalance_cb();
			break;
		case OFFSETCOMMIT:
			set_offset_commit_cb();
			break;
		case DELIVERED:
			set_dr_msg_cb();
			break;
		case ONERROR:
			break;
		default:
			break;
		}
	}	

	int AddBrokers(const std::string& brokers)
	{
		int cnt = 0;
		cnt = rd_kafka_brokers_add(get_handle(), brokers.c_str());
		if (cnt > 0)
		{
			if (broker_list_.size() > 0)
			{
				broker_list_ += ",";
			}

			broker_list_ += brokers;
			LOG_DEBUG(OUT_ALL) << "add broker : " << brokers << " success, all brokers : " << broker_list_;
		}
		else
		{
			cnt = 0;
			LOG_DEBUG(OUT_ALL) << "add broker : " << brokers << " fail,errmsg:  : " << rd_kafka_err2str(rd_kafka_last_error());
		}
		return cnt;
	}
protected:		
	explicit KafkaHandle(const ConfigOption& config, const ConfigOption& Topic_config) : Kafka_Conf(config), Kafka_Topic_Conf(Topic_config),
		handle_(nullptr, nullptr),pInvoker_(nullptr), set_callback(false)
	{	
		LOG_INFO(OUT_ALL) << "version : " << MWKFK_VERSION << "[" << MWKFK_VERSION_NUM << "]";
		set_opaque_cb();
		set_log_cb();
	}
	
	virtual ~KafkaHandle()
	{	
		LOG_WARNING(OUT_ALL) << "~KafkaHandle";
	}
	
	bool create_handle(rd_kafka_type_t _type)
	{	
		auto config = get_conf_all();
		LOG_DEBUG(OUT_ALL) << "config is :";
		for (auto iter : config)
		{
			LOG_DEBUG(OUT_ALL) << iter.first << " : " <<iter.second;
		}
		auto topicconfig = get_topic_conf_all();
		LOG_DEBUG(OUT_ALL) << "topicconfig is :";
		for (auto iter : topicconfig)
		{
			LOG_DEBUG(OUT_ALL) << iter.first << " : " << iter.second;
		}

		
		
		char error_buffer[512] = { 0 };
		rd_kafka_t* ptr = rd_kafka_new(_type, rd_kafka_conf_dup(get_conf_handle()), error_buffer, sizeof(error_buffer));
		if (!ptr)
		{
			LOG_ERROR(OUT_ALL) << "rd_kafka_new is fail err: " << error_buffer << "  type : " << _type;
			return false;
		}
		bool bRet = set_handle(ptr);
		if (!bRet)
		{
			LOG_ERROR(OUT_ALL) << "set_handle is fail err: " << error_buffer << "  type : " << _type;
			return false;
		}
		add_default_brokers();
		return true;
	}
	
	rd_kafka_t* get_handle() const	{ return handle_.get();}
	 
	std::pair<int64_t, int64_t> get_offsets(const Topic_Partition& topic_partition) const
	{
		int64_t low = 0;
		int64_t high = 0;
		const std::string& topic = topic_partition.get_topic();
		const int partition = topic_partition.get_partition();
		rd_kafka_get_watermark_offsets(get_handle(), topic.data(),	partition, &low, &high);		
		return std::make_pair(low, high);
	}

	void set_log_cb()
	{
		rd_kafka_conf_set_log_cb(get_conf_handle(), &KafkaHandle::rdkafka_logger);
	}
	void set_opaque_cb()
	{
		rd_kafka_conf_set_opaque(get_conf_handle(), this);		
	}
	void set_rebalance_cb()
	{
		rd_kafka_conf_set_rebalance_cb(get_conf_handle(), &KafkaHandle::rdkafka_rebalance_cb);
	}
	void set_offset_commit_cb()
	{
		set_callback = true;
		set_conf({ { "enable.auto.commit", false } }); //是否自动提交			
		rd_kafka_conf_set_offset_commit_cb(get_conf_handle(), &KafkaHandle::rdkafka_offset_commit_cb);
	}	
	void set_dr_msg_cb()
	{
		set_callback = true;
		rd_kafka_conf_set_dr_msg_cb(get_conf_handle(), &KafkaHandle::MsgDeliveredCallback);
	}
	void set_partitioner_cb()
	{
		rd_kafka_topic_conf_set_partitioner_cb(get_topic_conf_handle(), &KafkaHandle::partitioner_cb);
	}


	rd_kafka_resp_err_t commit(const rd_kafka_message_t* msg, bool async)
	{
		return rd_kafka_commit_message(get_handle(), msg, async ? 1 : 0);
	}

	rd_kafka_resp_err_t commit(const std::vector<MessagePtr>& topic_partitions, bool async)
	{
		rd_kafka_resp_err_t error = RD_KAFKA_RESP_ERR_UNKNOWN;
		if (topic_partitions.empty())
		{
			error = rd_kafka_commit(get_handle(), nullptr, async ? 1 : 0);
		}
		else
		{
			auto _hande = set_topic_partition_list_data(topic_partitions);
			if (_hande)
			{
				error = rd_kafka_commit(get_handle(), _hande.get(), async ? 1 : 0);
			}
		}
		return error;
	}
	void* GetInvoker() { return pInvoker_; } const
	DoneCallback GetCallBack(int _type)
	{
		std::lock_guard<std::mutex> lock(lock_callback);
		if (_callback.find(_type) != _callback.end())
		{
			return _callback[_type];
		}
		return nullptr;	
	}
	bool AddTopic(const Topic_Partition& _topic_info)
	{	
		std::lock_guard<std::mutex> lock(topics_mutex_);
		std::string _key = _topic_info.get_topic() + ":" + std::to_string(_topic_info.get_partition());
		if (m_topics_.find(_key) != m_topics_.end())
		{
			LOG_DEBUG(OUT_ALL) << "AddTopic rd_kafka_topic_new is repeat";
			return true;
		}

		rd_kafka_topic_t* pTopic = rd_kafka_topic_new(get_handle(), _topic_info.get_topic().c_str(), rd_kafka_topic_conf_dup(get_topic_conf_handle()));
		if (nullptr == pTopic)
		{
			LOG_ERROR(OUT_ALL) << "AddTopic rd_kafka_topic_new is fail";
			return false;
		}		

		auto topic = std::make_shared<Topic_Partition>(_topic_info);
		topic->set_topic_handle(pTopic);
		
		
		m_topics_[_key] = topic;
		
		return true;
	}
	std::shared_ptr<Topic_Partition> get_topic_info(const Topic_Partition& _topic_info)
	{
		std::lock_guard<std::mutex> lock(topics_mutex_);		
		std::string _key = _topic_info.get_topic() + ":" + std::to_string(_topic_info.get_partition());
		if (m_topics_.find(_key) == m_topics_.end())
		{
			LOG_DEBUG(OUT_ALL) << "GetTopic_Handle m_topics_ not find : " << _key;
			return nullptr;
		}

		return m_topics_[_key];
	}
	rd_kafka_topic_t* get_topic_handle(const Topic_Partition& _topic_info)
	{
		std::lock_guard<std::mutex> lock(topics_mutex_);
		std::string _key = _topic_info.get_topic() + ":" + std::to_string(_topic_info.get_partition());
		if (m_topics_.find(_key) == m_topics_.end())
		{
			LOG_DEBUG(OUT_ALL) << "GetTopic_Handle m_topics_ not find : " << _key;
			return nullptr;
		}
		auto handle = m_topics_[_key];
		if (nullptr == handle)
		{
			return nullptr;
		}

		return handle->get_topic_handle();
	}
	std::vector<Topic_Partition> get_topic_info()
	{
		std::lock_guard<std::mutex> lock(topics_mutex_);
		std::vector<Topic_Partition> _Topic_info;
		for (auto iter : m_topics_)
		{
			_Topic_info.push_back(*(iter.second));
		}
		return _Topic_info;
	}	
	int add_default_brokers()
	{
		std::string broker_ = get_conf_base("metadata.broker.list");
		if (broker_.size() > 0)
		{
			return AddBrokers(broker_);
		}
		return 0;
	}
	ConfigOption load(const std::string& key, const std::string& conf_path)
	{
		limonp::Config conf(conf_path);
		if (!bool(conf))
		{
			LOG_ERROR(OUT_ALL) << "load config is fail path: " << conf_path;
		}
		auto v_glabal = conf[key];

		ConfigOption co;
		for (auto iter : v_glabal)
		{
			co.push_back({ iter.first, iter.second });
		}
		return co;
	}
	bool is_callback()
	{
		return set_callback;
	}
private:
	
	HandlePtr make_handle(rd_kafka_t* ptr)
	{
		return HandlePtr(ptr,&rd_kafka_destroy);
	}
	bool set_handle(rd_kafka_t* handle)
	{
		handle_ = make_handle(handle);
		if (nullptr == handle_)
		{
			LOG_ERROR(OUT_ALL) << "make_handle err: ";
			return false;
		}
		return true;
	}
	static void rdkafka_rebalance_cb(rd_kafka_t* rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t* partitions, void* opaque)
	{
		switch (err)
		{
		case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
			rd_kafka_assign(rk, partitions);
			break;
		case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
		{
			rd_kafka_commit(rk, partitions, 0);
			rd_kafka_assign(rk, NULL);
		}
		break;
		default:
			rd_kafka_assign(rk, NULL);
			break;
		}
	}
	static void rdkafka_offset_commit_cb(rd_kafka_t* rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t* offsets, void* opaque)
	{
		KafkaHandle* pHandle = static_cast<KafkaHandle*>(opaque);
		if (NULL == pHandle)
		{
			LOG_ERROR(OUT_ALL) << "opaque to  pHandle is err ";
			return;
		}
		if (NULL == offsets)
		{
			LOG_ERROR(OUT_ALL) << "offsets is err ";
			return;
		}	

		for (int i = 0; i < offsets->cnt; ++i)
		{
			MessagePtr pMsg(static_cast<data_message_t*>(offsets->elems[i].opaque));
			if (nullptr != pMsg)
			{
				pMsg->committed_tm_ms = limonp::GetCurrentTimeMs();
				auto _callback_fun = pHandle->GetCallBack(OFFSETCOMMIT);
				if (_callback_fun)
				{
					_callback_fun(pHandle->GetInvoker(), pMsg);
				}
			}
		}
	}
	//消息投递回调函数
	static void MsgDeliveredCallback(rd_kafka_t* rk, const rd_kafka_message_t* rk_msg, void* opaque)
	{
		if (NULL == rk)
		{
			LOG_ERROR(OUT_ALL) << "rd_kafka_t is err ";			
			return;
		}

		if (NULL == rk_msg)
		{
			LOG_ERROR(OUT_ALL) << "rd_kafka_message_t is err ";
			return;
		}
		KafkaHandle* pHandle = static_cast<KafkaHandle*>(opaque);
		if (NULL == pHandle)
		{
			LOG_ERROR(OUT_ALL) << "opaque to  pHandle is err ";
			return;
		}

		std::shared_ptr<data_message_t> pMsg(static_cast<data_message_t*>(rk_msg->_private));
		if (nullptr == pMsg)
		{
			LOG_ERROR(OUT_ALL) << "std::shared_ptr<data_message_t> is err ";
			return;
		}		

		pMsg->status = rd_kafka_message_status(rk_msg);
		pMsg->errcode = rk_msg->err;
		pMsg->errmsg = rd_kafka_err2str(rk_msg->err);
		pMsg->partition = rk_msg->partition;
		pMsg->offset = rk_msg->offset;
		pMsg->producer_cb_tm_ms = limonp::GetCurrentTimeMs();

		auto _callback_fun = pHandle->GetCallBack(DELIVERED);
		if (_callback_fun)
		{
			_callback_fun(pHandle->GetInvoker(), pMsg);
		}
	}

	static int32_t partitioner_cb(const rd_kafka_topic_t* rkt, const void* keydata, size_t keylen, int32_t partition_cnt, void* rkt_opaque, void* msg_opaque)
	{
		int32_t hit_partition = 0;
		//key有值,取hash;key没值,随机
		if (keylen > 0 && NULL != keydata)
		{
			const char* key = static_cast<const char*>(keydata);
			hit_partition = limonp::djb_hash(key, keylen) % partition_cnt;
		}
		else
		{
			hit_partition = rd_kafka_msg_partitioner_random(rkt, keydata, keylen, partition_cnt, rkt_opaque, msg_opaque);
		}

		//检查分区是否可用,如果不可用,取出所有可用分区,然后再随机取一个
		if (1 != rd_kafka_topic_partition_available(rkt, hit_partition))
		{
			std::vector<int32_t> vPartition;
			for (int32_t i = 0; i < partition_cnt; ++i)
			{
				if (1 == rd_kafka_topic_partition_available(rkt, i))
				{
					vPartition.push_back(i);
				}
			}
			size_t nSize = vPartition.size();
			if (nSize > 0)
			{		
				size_t nRand = limonp::get_rand(nSize);
				hit_partition = vPartition[nRand];
			}
			else
			{
				hit_partition = 0;
			}
		}
		return hit_partition;
	}
	
	static void rdkafka_logger(const rd_kafka_t* rk, int level, const char* fac, const char* buf)
	{	
		LOG_DEBUG(OUT_ALL) << "RD_KAFKA_LOG " <<
			" | level: " << level <<
			" | fac: " << (NULL != fac ? fac : "") <<
			" | msg: " << (NULL != buf ? buf : "");		
	}
	
private:
	KafkaHandle(const KafkaHandle&) = delete;
	KafkaHandle(KafkaHandle&&) = delete;
	KafkaHandle& operator=(const KafkaHandle&) = delete;
	KafkaHandle& operator=(KafkaHandle&&) = delete;
private:
	HandlePtr handle_;

	void* pInvoker_;

	//消息回调
	std::map<int, DoneCallback>_callback;
	std::mutex lock_callback;


	std::mutex topics_mutex_;	
	std::map<std::string, std::shared_ptr<Topic_Partition>> m_topics_;

	std::string broker_list_;
	bool set_callback;
};





