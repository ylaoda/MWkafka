#pragma once
#include "struct.h"

class Producer;
class KAFKA_LIB mwKafka_Producer
{
public:
	//setcb 设置为true 自定义分配分区，否则 系统自动分配
	mwKafka_Producer(const std::string& brokers, bool setcb) : mwKafka_Producer(get_delfault_config(brokers, setcb)) {}
	mwKafka_Producer(const ConfigOption& Config);
	mwKafka_Producer(const ConfigOption& Config, const ConfigOption& Topic_config);

	mwKafka_Producer(const std::string& conf_path);
	~mwKafka_Producer() = default;

public:
	//停止生产
	//上层在退出前必须调用,以便让底层快速强制将缓冲中的信息快速发到KAFKA,调用完了以后,等待所有回调返回
	//timeout_ms>0该函数会阻塞,超时后则返回,不保证在阻塞期间一定将信息全部送达KAFKA,
	//timeout_ms<=0表示一定要等到信息全部安全到达KAFKA后才返回,但如果KAFKA故障,可能会导致一直无法返回
	bool Stop(int timeout_ms = 60 * 1000);

	//定时调用,以便通知底层让消息抓紧投递到kafka
	void Poll(int timeout_ms);

	//往kafka生产消息,可以指定分区
	int Produce(const topic_info_t& _topic, const std::string& _buffer, const std::string& _key = "");

	//添加需要生产的topic
	//一个生产者可以负责多个topic的生产
	bool AddTopics(const std::string& topic_info);
	
	//添加brokers
	int AddBrokers(const std::string& brokers);

	//pInvoker:上层调用的类指针
	void SetInvoker(void* pInvoker);

	void SetMaxOutQueSize(size_t size);

	//设置消息投递结果回调函数
	//pOnDeliveredCb:消息投递回调函数
	void AddCallBack(int _type, DoneCallback pCallBackCb);

	//设置日志保存目录
	//日志保存目录
	//level_日志等级
	void setLogFile(const std::string& log_path, size_t level_);


private:
	ConfigOption get_delfault_config(const std::string& brokers, bool setcb);
private:
	std::shared_ptr<Producer>_producer;
};



