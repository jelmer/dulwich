#!/usr/bin/python3

from dulwich.client import get_transport_and_path_from_url
from dulwich.objects import ZERO_SHA
from dulwich.pack import pack_objects_to_data

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('url', type=str)
parser.add_argument('old_ref', type=str)
parser.add_argument('new_ref', type=str)
args = parser.parse_args()

client, path = get_transport_and_path_from_url(args.url)


def generate_pack_data(*args, **kwargs):
    return pack_objects_to_data([])


def update_refs(refs):
    sha = refs[args.old_ref.encode('utf-8')]
    return {
        args.old_ref.encode('utf-8'): ZERO_SHA,
        args.new_ref.encode('utf-8'): sha}


client.send_pack(path, update_refs, generate_pack_data)
print("Renamed {} to {}".format(args.old_ref, args.new_ref))
