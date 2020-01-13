#pragma once
#include <string>
#include <set>
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include "config_base.hpp"
class Topic_Partition 
{
public:
	
	Topic_Partition(const std::string topic): Topic_Partition(std::move(topic), RD_KAFKA_PARTITION_UA) {}
	Topic_Partition(std::string topic, int partition) :Topic_Partition(std::move(topic), partition, RD_KAFKA_OFFSET_INVALID) {}
	Topic_Partition(std::string topic, int partition, int64_t offset)	: topic_(std::move(topic)), partition_(partition), offset_(offset), topic_handle_(nullptr){}

	const std::string& get_topic() const
	{
		return topic_;
	}
	int get_partition() const
	{
		return partition_;
	}
	int64_t get_offset() const
	{
		return offset_;
	}
	rd_kafka_topic_t* get_topic_handle() const
	{
		return topic_handle_.get();
	}

	void set_topic(std::string& _topic)
	{
		topic_ = _topic;
	}
	void set_partition(int partition)
	{
		partition_ = partition;
	}
	void set_offset(int64_t offset)
	{
		offset_ = offset;
	}
	void set_topic_handle(rd_kafka_topic_t* _handle)
	{
		topic_handle_ = std::shared_ptr<rd_kafka_topic_t>(_handle, &rd_kafka_topic_destroy);
	}
private:
	std::string topic_;
	int partition_;
	int64_t offset_;
	std::shared_ptr<rd_kafka_topic_t> topic_handle_;
};


using TopicPartitionList = std::vector<Topic_Partition>;

using TopicPartitionsListPtr = std::unique_ptr<rd_kafka_topic_partition_list_t,decltype(&rd_kafka_topic_partition_list_destroy)>;

TopicPartitionsListPtr get_topic_partition_list_handle(const std::vector<Topic_Partition>& _topic_partitions)
{
	TopicPartitionsListPtr handle(rd_kafka_topic_partition_list_new((int)_topic_partitions.size()),&rd_kafka_topic_partition_list_destroy);
	for (const auto& item : _topic_partitions)
	{
		rd_kafka_topic_partition_t* new_item = rd_kafka_topic_partition_list_add(
			handle.get(),
			item.get_topic().c_str(),
			item.get_partition()
		);
		if (nullptr != new_item)
		{
			new_item->offset = item.get_offset();
		}		
	}
	return handle;
}
TopicPartitionsListPtr set_topic_partition_list_data(const std::vector<MessagePtr>& topic_partitions)
{
	TopicPartitionsListPtr handle(rd_kafka_topic_partition_list_new((int)topic_partitions.size()), &rd_kafka_topic_partition_list_destroy);
	for (const auto& it : topic_partitions)
	{
		rd_kafka_topic_partition_t* new_item = rd_kafka_topic_partition_list_add(handle.get(),it->topic.c_str(), it->partition);
		if (nullptr != new_item)
		{	
			new_item->offset = it->offset + 1;	

			data_message_t* pPrivate = new data_message_t();
			if (NULL != pPrivate)
			{	
				*pPrivate = *it;
				int64_t _time = limonp::GetCurrentTimeMs();
				pPrivate->commit_offset_tm_ms = _time;
				it->commit_offset_tm_ms = _time;
				new_item->opaque = static_cast<void*>(pPrivate);
			}
		}
	}
	return handle;
}

TopicPartitionsListPtr get_assignment(rd_kafka_t* _handle)
{
	rd_kafka_topic_partition_list_t* parts = NULL;
	rd_kafka_assignment(_handle, &parts);
	TopicPartitionsListPtr handle(parts, &rd_kafka_topic_partition_list_destroy);
	return handle;
}


TopicPartitionList get_topic_partition(rd_kafka_topic_partition_list_t* _topic_partitions)
{
	std::vector<Topic_Partition> output;
	for (int i = 0; i < _topic_partitions->cnt; ++i)
	{
		const auto& elem = _topic_partitions->elems[i];
		output.emplace_back(elem.topic, elem.partition, elem.offset);
	}
	return std::move(output);
}

TopicPartitionList GetTopicInfoFromTopicPartitionList(rd_kafka_topic_partition_list_t* parts, const Topic_Partition& topic)
{
	std::vector<Topic_Partition> output;
	for (int i = 0; i < parts->cnt; ++i)
	{
		if (topic.get_partition() >= 0)
		{
			if (0 == strcmp(topic.get_topic().c_str(), parts->elems[i].topic) && parts->elems[i].partition == topic.get_partition())
			{
				output.push_back(Topic_Partition(parts->elems[i].topic, parts->elems[i].partition, parts->elems[i].offset));
			}
		}
		else
		{
			if (0 == strcmp(topic.get_topic().c_str(), parts->elems[i].topic))
			{
				output.push_back(Topic_Partition(parts->elems[i].topic, parts->elems[i].partition, parts->elems[i].offset));
			}
		}
		
	}
	return std::move(output);
}
