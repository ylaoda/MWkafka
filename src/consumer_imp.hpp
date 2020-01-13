#pragma once

#include <string>
#include <set>
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include <string.h>
#include <atomic>

#include "handle_base.hpp"

class Consumer : public KafkaHandle
{
	using HandlePtr = std::shared_ptr<rd_kafka_message_t>;
public:
	~Consumer(){Stop();}
	Consumer(const ConfigOption& Config) : Consumer(Config, {})	{}
	Consumer(const std::string& conf_path) :KafkaHandle(load("global", conf_path), load("topic", conf_path)) {}

	Consumer(const ConfigOption& Config, const ConfigOption& Topic_config) : KafkaHandle(Config, Topic_config), isInit(false), commit_max_wait(1000), commit_ms_(3000){}	

public:
	bool cosumer_seek(const Topic_Partition& _topic_info)
	{		
		if (_topic_info.get_offset() >= 0 && _topic_info.get_partition() >= 0)
		{	
			rd_kafka_topic_t* ktf = get_topic_handle(_topic_info);
			if (nullptr == ktf)
			{
				LOG_ERROR(OUT_ALL) << "cosumer_seek ktf = null";
				return false;
			}			
		
			rd_kafka_resp_err_t err = rd_kafka_seek(ktf, _topic_info.get_partition(), _topic_info.get_offset(), 5000);
			if (RD_KAFKA_RESP_ERR_NO_ERROR != err)
			{
				LOG_ERROR(OUT_ALL) << "rd_kafka_seek is fail ret : " << err;
				return false;
			}			
		}
		return true;
	}
	void Stop()
	{
		CommitOffset(nullptr);

		rd_kafka_resp_err_t err = rd_kafka_commit(get_handle(), NULL, 0);
		if (RD_KAFKA_RESP_ERR_NO_ERROR != err)
		{
			LOG_ERROR(OUT_ALL) << " Failed to rd_kafka_commit | err msg : " << rd_kafka_err2str(err);
		}
			//关闭消费
		err = rd_kafka_consumer_close(get_handle());
		if (RD_KAFKA_RESP_ERR_NO_ERROR != err)
		{
			LOG_ERROR(OUT_ALL) << " Failed to close consumer | err msg : " << rd_kafka_err2str(err);				
		}		
	}
	bool Subscribe(const std::vector<Topic_Partition>& v_topic, bool assign)
	{		
		std::lock_guard<std::mutex> lock(lock_init_);
		bool bRet = init_handle();
		if (!bRet)
		{
			LOG_ERROR(OUT_ALL) << "Subscribe init is fail";
			return false;
		}	
		for (auto iter : v_topic)
		{
			AddTopic(iter);
		}		
		return InternalSubscribe(v_topic, assign);
	}
	
	bool AddTopics(const Topic_Partition& topic_info, bool assign)
	{
		if (nullptr != get_topic_info(topic_info))
		{
			LOG_DEBUG(OUT_ALL) << "has subscribed : " << topic_info.get_topic();
			return true;
		}
		
		bool bRet = unsubscribe();
		if(!bRet)
		{
			LOG_ERROR(OUT_ALL) << "unsubscribe is fail  ret: ";
		}

		AddTopic(topic_info);
		auto topic_info_list = get_topic_info();		

		return InternalSubscribe(topic_info_list, assign);
	}

	bool unsubscribe()
	{
		std::lock_guard<std::mutex> lock(lock_init_);
		rd_kafka_resp_err_t error = rd_kafka_unsubscribe(get_handle());
		if (RD_KAFKA_RESP_ERR_NO_ERROR != error)
		{		
			LOG_ERROR(OUT_ALL) << "unsubscribe is fail  ret: " << error;
			return false;
		}
		return true;
	}	
	
	MessagePtr Poll(int timeout_ms, int parttion = -1)
	{
		MessagePtr msg(new data_message_t());
		if (nullptr == msg)
		{
			LOG_ERROR(OUT_ALL) << "Poll new data_message_t() is fail  ret: ";
			return nullptr;
		}
		if (NULL == get_handle())
		{
			std::this_thread::sleep_for(std::chrono::seconds(timeout_ms));
			msg->errcode = -1;
			msg->errmsg  = "rdkafka handle ptr error";
			rdkafka_consume_error(msg,nullptr);
			return NULL;
		}		

		HandlePtr rk_msg_ptr = make_handle(rd_kafka_consumer_poll(get_handle(), timeout_ms ? timeout_ms : 500));
		if (NULL == rk_msg_ptr)
		{
			msg->errcode = rd_kafka_last_error();
			msg->errmsg = rd_kafka_err2str(rd_kafka_last_error());
			rdkafka_consume_error(msg, nullptr);
			return NULL;
		}
		bool msg_no_error = false;
		switch (rk_msg_ptr.get()->err)
		{
		case RD_KAFKA_RESP_ERR__TIMED_OUT:	
			break;			
		case RD_KAFKA_RESP_ERR__PARTITION_EOF:	
			break;
		case RD_KAFKA_RESP_ERR_NO_ERROR://成功
			msg_no_error = true;			
			break;
		default:			
			break;
		}
		if (!msg_no_error)
		{
			return rdkafka_consume_error(msg, rk_msg_ptr);
		}
		return rdkafka_consume(msg,rk_msg_ptr);
	}	
	bool Pause(const Topic_Partition& topic_info)
	{
		return PauseOrResumeConsume(topic_info,false);
	}
	bool Resume(const Topic_Partition& topic_info)
	{
		return PauseOrResumeConsume(topic_info, true);
	}
	void SetOffsetCommitPolicy(size_t max_wait_commit_, int commit_freq_)
	{
		commit_max_wait = max_wait_commit_;
		commit_ms_= commit_freq_;
	}
	void CommitOffset(const MessagePtr& msg_for_commit, int async = 0)
	{
		if (!is_callback())
		{
			LOG_DEBUG(OUT_ALL) << "CommitOffset is not set  CallBack:";
			return;
		}
		std::lock_guard<std::mutex> lock(lock_wait_commit_);
		if (nullptr != msg_for_commit)
		{
			wait_commit_offsets_.push_back(msg_for_commit);
		}	
		static int64_t last_commit_tm_ = 0;
		if (wait_commit_offsets_.size() >= commit_max_wait || limonp::GetDiffMs(last_commit_tm_) >= commit_ms_ )
		{
			last_commit_tm_ = limonp::GetCurrentTimeMs();

			size_t pos = findmeber(wait_commit_offsets_);
			if (pos)
			{
				if (wait_commit_offsets_.size() != pos)
				{
					LOG_WARNING(OUT_ALL) << "CommitOffset is begin :" << wait_commit_offsets_[0]->offset <<
						" | pos : " << wait_commit_offsets_[pos - 1]->offset <<
						" | end : " << wait_commit_offsets_[wait_commit_offsets_.size() - 1]->offset <<
						" | more : " << wait_commit_offsets_.size() - pos;
				}

				std::vector<MessagePtr> v_msg(wait_commit_offsets_.begin(), wait_commit_offsets_.begin() + pos);						

				bool bRet = handle_commit(v_msg, async);
				if (bRet)
				{
					wait_commit_offsets_.erase(wait_commit_offsets_.begin(), wait_commit_offsets_.begin() + pos);
					wait_commit_offsets_.shrink_to_fit();
				}
			}			
		}
	}
private:
	size_t findmeber(std::vector<MessagePtr>& v_commit_msg)
	{	
		std::sort(v_commit_msg.begin(), v_commit_msg.end(), [](const MessagePtr& x, const MessagePtr& y){return x->offset <  y->offset;	});
		size_t begin = 0;
		size_t end = v_commit_msg.size();
		while (begin < end--)
		{
			if (size_t(v_commit_msg[end]->offset - v_commit_msg[begin]->offset) == end)
			{
				break;
			}
		}
		return end + 1;
	}
	bool handle_commit(const std::vector<MessagePtr>& v_commit_msg, int async)
	{
		rd_kafka_resp_err_t err = commit(v_commit_msg, async);
		if (RD_KAFKA_RESP_ERR_NO_ERROR != err)
		{
			std::string errmsg = rd_kafka_err2str(err);
			LOG_ERROR(OUT_ALL) << "CommitOffset is fail  ret: " << err << " errmsg : " << errmsg;
			return false;
		}	
		return true;
	}
	bool PauseOrResumeConsume(const Topic_Partition& topic_info, bool resume/*0:pause,1:resume*/)
	{
		auto handle = get_assignment(get_handle());
		if (nullptr == handle)
		{
			LOG_ERROR(OUT_ALL) << "PauseOrResumeConsume get_assignment is fail";
			return false;
		}
		//从parts中找到该topic所有的分区信息
		std::vector<Topic_Partition> vTopicInfo = GetTopicInfoFromTopicPartitionList(handle.get(), topic_info);
		if (vTopicInfo.empty())
		{
			LOG_ERROR(OUT_ALL) << "PauseOrResumeConsume GetTopicInfoFromTopicPartitionList is fail";
			return false;
		}
		//申请一个list
		auto list_handle = get_topic_partition_list_handle(vTopicInfo);

		if (resume)
		{
			rd_kafka_resp_err_t err = rd_kafka_resume_partitions(get_handle(), list_handle.get());
			if (RD_KAFKA_RESP_ERR_NO_ERROR != err)
			{
				LOG_ERROR(OUT_ALL) << "PauseOrResumeConsume rd_kafka_resume_partitions is fail ret : "<< err;
				return false;
			}
		}
		else
		{
			rd_kafka_resp_err_t err = rd_kafka_pause_partitions(get_handle(), list_handle.get());
			if (RD_KAFKA_RESP_ERR_NO_ERROR != err)
			{
				LOG_ERROR(OUT_ALL) << "PauseOrResumeConsume rd_kafka_pause_partitions is fail ret : " << err;
				return false;
			}
		}
		return true;
	}
	MessagePtr rdkafka_consume(MessagePtr& pMsg,std::shared_ptr<rd_kafka_message_t>& rk_msg_ptr)
	{
		auto rk_msg = rk_msg_ptr.get();
		if (nullptr == rk_msg)
		{
			LOG_ERROR(OUT_ALL) << "rdkafka_consume std::shared_ptr<rd_kafka_message_t> is fail ret";
			return nullptr;
		}
		if (nullptr == pMsg)
		{
			LOG_ERROR(OUT_ALL) << "rdkafka_consume MessagePtr is fail ret";
			return nullptr;
		}	
	
		pMsg->topic = rd_kafka_topic_name(rk_msg->rkt);
		pMsg->data.reserve(rk_msg->len);
		pMsg->data.assign(static_cast<const char*>(rk_msg->payload), rk_msg->len);
		pMsg->key.assign(static_cast<const char*>(rk_msg->key), rk_msg->key_len);
		pMsg->partition = rk_msg->partition;
		pMsg->offset = rk_msg->offset;
		pMsg->errcode = rk_msg->err;
		pMsg->errmsg = rd_kafka_err2str(rk_msg->err);
		pMsg->consumer_tm_ms = limonp::GetCurrentTimeMs();;
		pMsg->commit_offset_tm_ms = 0;
		pMsg->committed_tm_ms = 0;			

		auto _callback_fun = GetCallBack(CONSUMED);
		if (_callback_fun)
		{
			_callback_fun(GetInvoker(), pMsg);
		}

		return pMsg;
	}
	MessagePtr rdkafka_consume_error(MessagePtr& pMsg, std::shared_ptr<rd_kafka_message_t> rk_msg_ptr)
	{
		CommitOffset(nullptr);	

		if (nullptr == pMsg)
		{
			LOG_ERROR(OUT_ALL) << "rdkafka_consume MessagePtr is fail ret";
			return nullptr;
		}

		auto rk_msg = rk_msg_ptr.get();
		if (nullptr != rk_msg)
		{
			pMsg->topic = rd_kafka_topic_name(rk_msg->rkt);
			pMsg->data.assign(static_cast<const char*>(rk_msg->payload), rk_msg->len);
			pMsg->key.assign(static_cast<const char*>(rk_msg->key), rk_msg->key_len);
			pMsg->partition = rk_msg->partition;
			pMsg->offset = rk_msg->offset;
			pMsg->errcode = rk_msg->err;
			pMsg->errmsg = rd_kafka_err2str(rk_msg->err);
			pMsg->consumer_tm_ms = limonp::GetCurrentTimeMs();;
			pMsg->commit_offset_tm_ms = 0;
			pMsg->committed_tm_ms = 0;			
		}
		auto _callback_fun = GetCallBack(ONERROR);
		if (_callback_fun)
		{
			_callback_fun(GetInvoker(), pMsg);
		}
		return pMsg;
	}
	
	bool init_handle()
	{
		if (!isInit)
		{	
			std::string  commit_max = get_conf_base("sdk.commit_max_wait");
			if (commit_max.size() > 0)
			{
				commit_max_wait = strtoll(commit_max.c_str(), nullptr, 10);
			}
			std::string  commit_ms = get_conf_base("sdk.commit_ms");
			if (commit_max.size() > 0)
			{
				commit_ms_ = strtoll(commit_ms.c_str(), nullptr, 10);
			}			
			rd_kafka_conf_set_default_topic_conf(get_conf_handle(), rd_kafka_topic_conf_dup(get_topic_conf_handle()));
			isInit = create_handle(RD_KAFKA_CONSUMER);
			if (!isInit)
			{
				LOG_ERROR(OUT_ALL) << "RD_KAFKA_CONSUMER create_handle is fail ret";
				return false;
			}			

			if (RD_KAFKA_RESP_ERR_NO_ERROR != rd_kafka_poll_set_consumer(get_handle()))
			{
				LOG_ERROR(OUT_ALL) << "rd_kafka_poll_set_consumer is fail ret";
				return false;
			}
		}
		return true;
	}	
	bool InternalSubscribe(const std::vector<Topic_Partition> _subscribe, bool assign)
	{
		auto list_handle = get_topic_partition_list_handle(_subscribe);
		if (nullptr == list_handle)
		{
			LOG_ERROR(OUT_ALL) << "get_topic_partition_list_handle is fail";
			return false;
		}		
		if (assign)
		{
			rd_kafka_resp_err_t err = rd_kafka_assign(get_handle(), list_handle.get());
			if (RD_KAFKA_RESP_ERR_NO_ERROR != err)
			{
				LOG_ERROR(OUT_ALL) << "rd_kafka_subscribe is fail  ret: " << err;
				return false;
			}
			for (auto topic_info : _subscribe)
			{
				cosumer_seek(topic_info);
			}			
		}
		else
		{
			rd_kafka_resp_err_t err = rd_kafka_subscribe(get_handle(), list_handle.get());
			if (RD_KAFKA_RESP_ERR_NO_ERROR != err)
			{
				LOG_ERROR(OUT_ALL) << "rd_kafka_subscribe is fail  ret: " << err;
				return false;
			}
		}
		
		
		return true;
	}

	HandlePtr make_handle(rd_kafka_message_t* ptr)
	{
		return HandlePtr(ptr, [](rd_kafka_message_t* handle) {if (handle) rd_kafka_message_destroy(handle); });
	}


private:
	bool isInit;	
		//待提交队列
	std::mutex lock_wait_commit_;
	std::vector<MessagePtr> wait_commit_offsets_;

	size_t	commit_max_wait;
	int64_t commit_ms_;

	std::mutex lock_init_;
	
};