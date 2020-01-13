#pragma once
#include "struct.h"
class Consumer;
class KAFKA_LIB mwKafka_Consumer
{
public:
	mwKafka_Consumer(const std::string& brokers, const std::string& group_id) : mwKafka_Consumer(get_delfault_config(brokers, group_id), get_topic_delfault_config()) {}
	mwKafka_Consumer(const ConfigOption& Config);
	mwKafka_Consumer(const ConfigOption& Config, const ConfigOption& Topic_config);
	mwKafka_Consumer(const std::string& conf_path);
	~mwKafka_Consumer() = default;
	
public:
	//订阅某一个或多个topic的消息
	//可指定从某一个分区或某一个offset开始消费(建议使用默认值)
	//不可重复调用
	//当 assign=true时可以指定分区进行消费
	bool Subscribe(const std::vector<topic_info_t>& topics, bool assign = false);

	//消费单个topic 可以指定分区消费
	//当 assign=true时可以指定分区进行消费
	//不可重复调用
	bool Subscribe(const topic_info_t& topics, bool assign = false);

	//指定offset和分区消费,需在Subscribe之后使用
	bool cosumer_seek(const topic_info_t& _topic_info);

	//取消当前所有的订阅
	bool unsubscribe();

	//添加topic
	bool AddTopics(const topic_info_t& topic_info, bool assign);

	//提交偏移量,告诉kafka该条消息已被成功消费(注意:默认是必须手动提交!!!)
	//async,0:sync commit, 1:async commit
	//提交后的结果,将通过pfunc_on_offsetcommitted返回,如未设置该回调函数,可不用处理提交结果
	void CommitOffset(const MessagePtr& msg_for_commit, int async = 0);

	//定时调用,以便通知底层让快速从kafka取消息
	//消费所有的topic的所有分区消息
	//timeout_ms<=0仅代表通知,立刻返回,>0会阻塞,建议值:10
	MessagePtr Poll(int timeout_ms = 500);	

	//停止消费(停止消费前,要停止一切kafka接口调用,尤其是消费接口和commitoffset接口)
	//上层在退出前必须调用,以便让底层快速强制将缓冲中的信息快速发到KAFKA,调用完了以后,等待所有回调返回	
	void Stop();

	//暂停某一个topic的消费
	bool Pause(const topic_info_t& topic_info);

	//恢复某一个topic的消费
	bool Resume(const topic_info_t& topic_info);
	
	//当待提交数超过max_wait_commit或已经commit_freq长时间未提交了,就会触发批量提交
	void SetOffsetCommitPolicy(size_t max_wait_commit, int commit_freq);

	//添加brokers
	int AddBrokers(const std::string& brokers);

	//pInvoker:上层调用的类指针
	void SetInvoker(void* pInvoker);

	//设置消息消费结果回调函数	
	//_type 设置回调消息函数类型  CONSUMED ,OFFSETCOMMIT,
	//pConsumeCb:消息消费回调函数
	void AddCallBack(int _type, DoneCallback pCallBackCb);

	//设置日志保存目录
	//日志保存目录
	//level_日志等级
	void setLogFile(const std::string& log_path, size_t level_);

private:
	ConfigOption get_delfault_config(const std::string& brokers, const std::string& group_id);	
	ConfigOption get_topic_delfault_config(Offset offset = Offset::OFFSET_END);// 默认从末尾消费，如果要从头消费 设置offset = OFFSET_END

private:
	std::shared_ptr<Consumer>_consumer;
};





