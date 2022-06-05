#!/usr/bin/env python3

import sys
import os
from rosbag2_py import SequentialReader, StorageOptions, StorageFilter, ConverterOptions, SequentialWriter, TopicMetadata


def main():
    if (len(sys.argv) >= 4):  # sys.argv[0] is the script name
        bagfile_path_input = os.path.join(os.path.abspath(os.getcwd()), sys.argv[1])
        bagfile_path_output = os.path.join(os.path.abspath(os.getcwd()), sys.argv[2])
        topics = sys.argv[3:]
    else:
        print("bad argument(s): " + str(sys.argv))
        print("Should be: [bag_path_input] [bag_path_output] TOPICS...")
        exit(1)

    # init
    reader_storage_options = StorageOptions(uri=bagfile_path_input, storage_id='sqlite3')
    writer_storage_options = StorageOptions(uri=bagfile_path_output, storage_id='sqlite3')
    converter_options = ConverterOptions('', '')
    storage_filter = StorageFilter(topics=topics)

    reader = SequentialReader()
    reader.open(reader_storage_options, converter_options)
    reader.set_filter(storage_filter)

    writer = SequentialWriter()
    writer.open(writer_storage_options, converter_options)

    # write filtered bag
    topic_msg_count = {}
    topic_metadata = {
        elem.name: elem for elem in reader.get_all_topics_and_types()
    }

    while reader.has_next():
        topic_name, msg, timestamp = reader.read_next()

        if topic_name not in topic_msg_count.keys():  # first msg of topic
            writer.create_topic(
                TopicMetadata(name=topic_name,
                              type=topic_metadata[topic_name].type,
                              serialization_format=topic_metadata[topic_name].serialization_format)
            )
            topic_msg_count[topic_name] = 0

        topic_msg_count[topic_name] += 1
        writer.write(topic_name, msg, timestamp)

    # Print info
    print("Done writing {} topics into {}".format(len(topic_msg_count.keys()), bagfile_path_output))
    for topic_name in topic_msg_count.keys():
        print(f"\t{topic_name} [{topic_metadata[topic_name].type}]: {topic_msg_count[topic_name]} msgs")

    if list(topic_msg_count.keys()).sort() != list(topic_metadata.keys()).sort():
        print("[WARNING] Topics transferred to output file do not match all topics left after filtering!")
        print("[WARNING] Topics transferred:")
        for elem in topic_msg_count.keys():
            print("[WARNING] " + elem)
        print("[WARNING] Topics left after filtering:")
        for elem in topic_metadata.keys():
            print("[WARNING] " + elem)


if __name__ == "__main__":
    main()
