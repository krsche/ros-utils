#!/usr/bin/env python3

import sys
import os
from rosbag2_py import SequentialReader, StorageOptions, StorageFilter, ConverterOptions
from sensor_msgs.msg import NavSatFix
from rclpy.serialization import deserialize_message


def main():
    if (len(sys.argv) == 4):
        bagfile_path = os.path.join(os.path.abspath(os.getcwd()), sys.argv[1])
        csvfile_path = sys.argv[2]
        topic = sys.argv[3]
    else:
        print("bad argument(s): " + str(sys.argv))
        print("Should be: [bagfile path] [csvfile path] [topic name]")
        exit(1)

    storage_options = StorageOptions(uri=bagfile_path, storage_id='sqlite3')
    converter_options = ConverterOptions('', '')
    storage_filter = StorageFilter(topics=[topic])

    reader = SequentialReader()
    reader.open(storage_options, converter_options)
    reader.set_filter(storage_filter)

    data = []

    while reader.has_next():
        (topic, topic_data, t) = reader.read_next()
        msg: NavSatFix = deserialize_message(topic_data, NavSatFix)
        data.append({'lat': msg.latitude, 'lon': msg.longitude})

    with open(csvfile_path, 'w') as csv:
        csv.write("latitude,longitude\n")
        for entry in data:
            csv.write("{},{}\n".format(
                str(entry['lat']),
                str(entry['lon'])
            ))


if __name__ == "__main__":
    main()
