#include "mwkafka_consumer.h"
#include "mwkafka_producer.h"
#include "consumer_imp.hpp"
#include "producer_imp.hpp"



mwKafka_Consumer::mwKafka_Consumer(const ConfigOption& Config) :mwKafka_Consumer(Config, {}){}
mwKafka_Consumer::mwKafka_Consumer(const ConfigOption& Config, const ConfigOption& Topic_config):_consumer(new Consumer(Config, Topic_config)){ SetInvoker(this); }
mwKafka_Consumer::mwKafka_Consumer(const std::string& conf_path) : _consumer(new Consumer(conf_path)) { SetInvoker(this); }

int mwKafka_Consumer::AddBrokers(const std::string& brokers)
{
	return _consumer->AddBrokers(brokers);
}
void mwKafka_Consumer::SetInvoker(void* pInvoker)
{
	return _consumer->SetInvoker(pInvoker);
}
void mwKafka_Consumer::AddCallBack(int _type, DoneCallback pCallBackCb)
{
	return _consumer->AddCallBack(_type, pCallBackCb);
}
void mwKafka_Consumer::setLogFile(const std::string& log_path, size_t level_)
{
	return _consumer->setLogFile(log_path, level_);
}
bool mwKafka_Consumer::cosumer_seek(const topic_info_t& _topic_info)
{
	return _consumer->cosumer_seek({ _topic_info.topic, _topic_info.partition, _topic_info.offset });
}
bool mwKafka_Consumer::Subscribe(const std::vector<topic_info_t>& topics, bool assign)
{
	std::vector<Topic_Partition>_topics;
	for (auto iter : topics)
	{
		_topics.push_back({ iter.topic, iter.partition, iter.offset });
	}
	return _consumer->Subscribe(_topics, assign);
}
bool mwKafka_Consumer::Subscribe(const topic_info_t& topics, bool assign)
{
	std::vector<topic_info_t>_topics;
	_topics.push_back(topics);
	return Subscribe(_topics, assign);
}

bool mwKafka_Consumer::unsubscribe()
{
	return _consumer->unsubscribe();
}
bool mwKafka_Consumer::AddTopics(const topic_info_t& topic_info, bool assign)
{
	return _consumer->AddTopics(Topic_Partition(topic_info.topic, topic_info.partition, topic_info.offset), assign);
}

void mwKafka_Consumer::CommitOffset(const MessagePtr& msg_for_commit, int async)
{
	return _consumer->CommitOffset(msg_for_commit, async);
}
MessagePtr mwKafka_Consumer::Poll(int timeout_ms)
{
	return _consumer->Poll(timeout_ms);
}

void mwKafka_Consumer::Stop()
{
	return _consumer->Stop();
}
bool mwKafka_Consumer::Pause(const topic_info_t& topic_info)
{
	return _consumer->Pause(Topic_Partition(topic_info.topic, topic_info.partition, topic_info.offset));
}
bool mwKafka_Consumer::Resume(const topic_info_t& topic_info)
{
	return _consumer->Resume(Topic_Partition(topic_info.topic, topic_info.partition, topic_info.offset));
}

void mwKafka_Consumer::SetOffsetCommitPolicy(size_t max_wait_commit, int commit_freq)
{
	return _consumer->SetOffsetCommitPolicy(max_wait_commit, commit_freq);
}

ConfigOption mwKafka_Consumer::get_delfault_config(const std::string& brokers, const std::string& group_id)
{
	ConfigOption config = {
			{ "metadata.broker.list", brokers },//消费IP
			{ "group.id", group_id },//消费GROUP
			{ "enable.auto.commit", true },//是否自动提交	
			{ "sdk.commit_max_wait",1000 },//设置批量提交最大容量
			{ "sdk.commit_ms_",3000 }//设置自动提交间隔时间  单位：毫秒
		};	
	
	return config;
}
ConfigOption mwKafka_Consumer::get_topic_delfault_config(Offset offset)
{
	ConfigOption topic_config;
	if (offset == Offset::OFFSET_BEGINNING)
	{
		topic_config = {
			{ "auto.offset.reset", "smallest" },//设置消费topic从头开始消费，当offset不存在时//earliest //
		};
	}
	else if (offset == Offset::OFFSET_END)
	{
		topic_config = {
			{ "auto.offset.reset", "earliest" },//设置消费topic从头开始消费，当offset不存在时//earliest //smallest
		};
	}	
	return topic_config;
}



mwKafka_Producer::mwKafka_Producer(const ConfigOption& Config) :mwKafka_Producer(Config, {}) {}
mwKafka_Producer::mwKafka_Producer(const ConfigOption& Config, const ConfigOption& Topic_config) : _producer(new Producer(Config, Topic_config)) { SetInvoker(this); }
mwKafka_Producer::mwKafka_Producer(const std::string& conf_path) : _producer(new Producer(conf_path)) { SetInvoker(this); }

int mwKafka_Producer::AddBrokers(const std::string& brokers)
{
	return _producer->AddBrokers(brokers);
}
void mwKafka_Producer::SetInvoker(void* pInvoker)
{
	return _producer->SetInvoker(pInvoker);
}
void mwKafka_Producer::AddCallBack(int _type, DoneCallback pCallBackCb)
{
	return _producer->AddCallBack(_type, pCallBackCb);
}
void mwKafka_Producer::setLogFile(const std::string& log_path, size_t level_)
{
	return _producer->setLogFile(log_path, level_);
}

bool mwKafka_Producer::Stop(int timeout_ms)
{
	return _producer->Stop(timeout_ms);
}
void mwKafka_Producer::Poll(int timeout_ms)
{
	return _producer->Poll(timeout_ms);
}

int mwKafka_Producer::Produce(const topic_info_t& _topic, const std::string& _buffer, const std::string& _key)
{
	return _producer->Produce(Topic_Partition(_topic.topic, _topic.partition, _topic.offset), _buffer, _key);
}
bool mwKafka_Producer::AddTopics(const std::string& topic_info)
{
	return _producer->AddTopics(topic_info);
}
void mwKafka_Producer::SetMaxOutQueSize(size_t size)
{
	return _producer->SetMaxOutQueSize(size);
}
ConfigOption mwKafka_Producer::get_delfault_config(const std::string& brokers, bool setcb)
{
	ConfigOption config;
	if (setcb)
	{
		config = {
			{ "metadata.broker.list", brokers },
			{ "sdk.set_partitioner_cb", true }
		};
	}
	else
	{
		config = {
			{ "metadata.broker.list", brokers }
		};
	}
	return config;
}